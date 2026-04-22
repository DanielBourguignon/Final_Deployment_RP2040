#include <Arduino.h>
#include <SPI.h>
#include <Wire.h>
#include <SdFat.h>
#include <Fast4ier.h>
#include <complex.h>
#include <math.h>
#include <string.h>
#include <ctype.h>
#include <pico/multicore.h>

#include <MicroTFLite.h>
// #include "cryo_model_0.h"
#include "cryo_model_v4.h"
#include "cryo_pipeline_types.h"
#include <IridiumSBD.h>
#include <TinyGPS++.h>

static TinyGPSPlus GNSS;
static bool gGnssModuleDetected = false;
static bool gIridiumModuleDetected = false;
int messageBytes;
int bytesUsedThisMonth = 0;
int alreadyResetQuota = 0;
int iridiumDay = 100;
int iridiumDayCount = 0;

#define IridiumSerial Serial1  //Establish the serial for Iridium
#define IRIDIUM_PWR_PIN 7      //Iridium Power pin
#define TOGGLE_GNSS 6          //GNSS Power pin

IridiumSBD modem(IridiumSerial);

#define INITIAL_ADXL_THRESHOLD 0.020  //boot fallback value tuned to avoid missing events

#define SD_SPI_SPEED SD_SCK_MHZ(12)

// HARDWARE MAPPING AND SPI SETTINGS
// #define SD_CS            A1
#define LED_PIN 11  // TODO: Don't actually use the LED in production, except for maybe at initial setup. Waste of power
#define FIRST_INIT_PIN 10
#define KILL_PICO_PIN 9
#define KILL_SD_PIN 8

// Time will be taken at wakeup and compared to the GNSS PVT so that we can backtrack the start of a recording for timestamping
uint32_t startMillis = 0;

// -----------------------------------------------------------------------------
// Configuration
// -----------------------------------------------------------------------------

// Explicitly bind the SD card to the custom board's SPI wiring so board profile
// changes do not silently remap the SPI pins used by the card.
static const uint8_t SD_CS_PIN = 17;
static const uint8_t SD_MISO_PIN = 16;
static const uint8_t SD_SCK_PIN = 18;
static const uint8_t SD_MOSI_PIN = 19;

static const float kDcraBiasWindowSec = 0.1f;
static constexpr float kInputAmplitudeScale = 4.2926963207f;

static const char* kStageWavPath = "/STAGE.WAV";
static const char* kStageBackupWavPath = "/STAGE.BAK";
static const char* kThresholdLogPath = "/THRESHOLD.txt";
static const char* kIridiumStatePath = "/IRI_STATE.txt";
static const char* kIridiumStateTempPath = "/IRI_STATE.tmp";

static const char* kDtStatePathA = "/DTA.BIN";
static const char* kDtStatePathB = "/DTB.BIN";
static constexpr uint32_t kDtMagic = 0x52545343UL;  // 'CRTS'
static constexpr uint16_t kDtVersion = 2;
static uint32_t gDtSequence = 0;

static constexpr float kAdcVrefVolts = 4.096f;
static constexpr float kAdcFullRangeVolts = 3.0f * kAdcVrefVolts;
static constexpr float kAdxlSensitivityVoltsPerG = 10.12f;  // determined by calibration
static constexpr float kAdcVoltsPerCount = kAdcFullRangeVolts / 65535.0f;
static constexpr float kAdcCountsPerG = kAdxlSensitivityVoltsPerG / kAdcVoltsPerCount;

// Debug / diagnostics
static const bool kDebugPipeline = true;
static const bool kKeepBuiltinLedOnDuringProgram = true;
static const bool kBypassFinalShutdownForDebug = false;  // DO NOT enable this unless testing post-failure behavior
static const bool kTreatRunLogFailureAsNonfatalForDebug = false;
static const bool kDebugPrintTuples = false;
static const bool kDebugPrintDtState = true;
static const unsigned long kDebugSerialWaitMs = 5000;  // Only occurs when kDebugPipeline = true

// FFT parameters
static const size_t kFftBins = 4096;
static const size_t kFftOutBins = kFftBins / 2;

// Inference parameters
static const int kFrameSamples = 2048;
static const int kInputSize = 128;
static const int kNumClasses = 2;
static const int kTensorArenaSize = 32 * 1024;

static const float kThresholdAmbient = 0.75f;
static const float kThresholdStorm = 0.75f;

// These controller seeds are now defined in the same physical domain as the
// ADXL threshold: peak dynamic acceleration expressed in g, then converted into
// centered signed-PCM counts for the adaptive controller.
static constexpr float kInitialThresholdLowG = 0.020f;
static constexpr float kInitialThresholdHighG = 0.060f;
static constexpr float kInitialThresholdLowCounts = kInitialThresholdLowG * kAdcCountsPerG;
static constexpr float kInitialThresholdHighCounts = kInitialThresholdHighG * kAdcCountsPerG;
static constexpr float kDeploymentThresholdFloorG = 0.0005f;
static constexpr float kDeploymentThresholdFloorCounts = kDeploymentThresholdFloorG * kAdcCountsPerG;

// -----------------------------------------------------------------------------
// Global objects and buffers
// -----------------------------------------------------------------------------

SdFat SD;
FsFile gRoot;

alignas(16) uint8_t tensorArena[kTensorArenaSize];

static complex fftVals[kFftBins];
static float powerFrame[kFftOutBins];
static float fftInput[kInputSize];

// Fixed scratch buffers to avoid heap allocation during file transforms.

static constexpr size_t kDcraInMaxBytes = 24U * 1024U;
static constexpr size_t kDcraOutMaxBytes = 4U * 1024U;
static uint8_t dcraInScratch[kDcraInMaxBytes];
static uint8_t dcraOutScratch[kDcraOutMaxBytes];

// One-shot guard for the Arduino loop.
// Once the pipeline has run, this flag prevents the loop from re-entering the
// processing sequence again during the same boot.
static bool gPipelineDone = false;
static bool gSetupReady = false;

#define ADXL355_I2C_ADDRESS 0x1D
#define REG_STATUS 0x04
#define REG_XDATA3 0x08
#define REG_ACT_EN 0x24
#define REG_ACT_THRES_H 0x25
#define REG_ACT_THRES_L 0x26
#define REG_ACT_COUNT 0x27
#define REG_INT_MAP 0x2A
#define REG_RANGE 0x2C
#define REG_POWER_CTL 0x2D
#define REG_RESET 0x2F
#define REG_DEVID_AD 0x00
#define REG_PARTID 0x02
#define LED_RED 15
#define LED_YELLOW 14
#define INTERRUPT_PIN 2
#define ADXL355_EXPECTED_DEVID_AD 0xAD
#define ADXL355_EXPECTED_PARTID 0xED

SdFile tempFile;
static char gIridiumMessage[160] = { 0 };

static bool readTextFileInt(const char* path, int& out);
static bool readTextFileFloat(const char* path, float& out);
static bool loadIridiumState();
static bool saveIridiumState();
static bool hasGnssTimestamp();
static bool hasGnssDateTimeFix();
static bool makeIndexedPath(uint32_t index, const char* ext, char* out, size_t outSize);
static bool parseIndexedFileName(const char* name, uint32_t& indexOut);
static bool readSamdRecordingDurationMs(uint32_t index, uint32_t& outDurationMs);
static bool gnssToUnixUtc(uint32_t& outEpochSeconds);
static void unixUtcToCalendar(uint32_t epochSeconds, uint16_t& year, uint8_t& month, uint8_t& day,
                              uint8_t& hour, uint8_t& minute, uint8_t& second);
static bool applyGnssTimestampToFile(const String& fileName);
static void buildIridiumMessage(float thresholdG, bool hasThreshold);
static void finalizeDeploymentShutdown();
void setup();
void loop();

static bool gAdxlWakeThresholdReady = false;
static volatile uint32_t gCurrentRunFramesInferred = 0;
static volatile uint32_t gCurrentRunStormFrames = 0;
static constexpr float kStormLockdownFraction = 0.75f;

static float currentRunStormFrameFraction();
static bool currentRunShouldLockdown();

bool setupADXL() {
  // Return 0 if no errors and 1 if errors
  bool Errors = false;
  uint8_t devidAd = 0;
  uint8_t partId = 0;
  uint8_t powerCtl = 0;

  // Set I2C wire locations
  Wire.setSDA(12);
  Wire.setSCL(13);
  Wire.begin();

  if (!readReg(ADXL355_I2C_ADDRESS, REG_DEVID_AD, devidAd) || devidAd != ADXL355_EXPECTED_DEVID_AD) {
    Errors = true;
  }

  if (!readReg(ADXL355_I2C_ADDRESS, REG_PARTID, partId) || partId != ADXL355_EXPECTED_PARTID) {
    Errors = true;
  }

  if (!readReg(ADXL355_I2C_ADDRESS, REG_POWER_CTL, powerCtl)) {
    Errors = true;
  }

  return Errors;
}

// This function sets ADXL threshold given a decimal (in g’s) input
// Also, if the interrupt isn't used take out the ADXL_Isr() parameter
bool setADXLRegThreshold(float decThresh) {  // , void (*isr)()) {
  bool Errors = false;

  double scaled = decThresh * 256000.0;
  uint32_t temp = (uint32_t)(scaled + 0.5);
  uint8_t highByte = (temp >> 8) & 0xFF;
  uint8_t lowByte = temp & 0xFF;

  Errors |= !writeReg(REG_ACT_EN, 0x07);           // enables x, y, and z axis for detection (datasheet pg. 40)
  Errors |= !writeReg(REG_ACT_THRES_H, highByte);  // sets top byte of threshold register (datasheet pg. 40)
  Errors |= !writeReg(REG_ACT_THRES_L, lowByte);   // sets low byte of threshold register (datasheet pg. 40)
  Errors |= !writeReg(REG_ACT_COUNT, 0x01);        // sets number of consecutive events above threshold before detection (datasheet pg. 41)
  Errors |= !writeReg(REG_RANGE, 0xC0);            // sets I2C speed to highest speed and set both interrupts to active high (datasheet pg. 42)
  Errors |= !writeReg(REG_INT_MAP, 0x88);          // enable activity on all interrupts (datasheet pg. 42)

  // Put device into measurement mode
  Errors |= !writeReg(REG_POWER_CTL, 0x00);  // set to standby mode (0x01) for 21 [uA] draw or measurement mode (0x00) for 200 [uA] draw (datasheet pg. 43)
  delay(20);

  return Errors;
}

// Reads a single ADXL register. Returns true on success and writes the byte into outValue.
// Example:
//   uint8_t value = 0;
//   if (readReg(ADXL355_I2C_ADDRESS, REG_POWER_CTL, value)) { ... }
bool readReg(uint8_t devAddr, uint8_t regAddr, uint8_t& outValue) {
  Wire.beginTransmission(devAddr);
  Wire.write(regAddr);
  if (Wire.endTransmission(false) != 0) {
    return false;
  }

  if (Wire.requestFrom(devAddr, (uint8_t)1) != 1) {
    return false;
  }

  if (Wire.available()) {
    outValue = Wire.read();
    return true;
  }

  return false;
}

bool writeReg(uint8_t reg, uint8_t value) {
  Wire.beginTransmission(ADXL355_I2C_ADDRESS);
  Wire.write(reg);
  Wire.write(value);
  return Wire.endTransmission() == 0;
}

static bool setAdxlMeasurementMode() {
  const bool ok = writeReg(REG_POWER_CTL, 0x00);
  if (ok) {
    delay(20);
  }
  return ok;
}

static bool setAdxlStandbyMode() {
  const bool ok = writeReg(REG_POWER_CTL, 0x01);
  if (ok) {
    delay(20);
  }
  return ok;
}


// ==============================
// GNSS
// ==============================

//Function to setup GNSS
void setupGNSS() {
  //Turn on the GNSS module
  pinMode(TOGGLE_GNSS, OUTPUT);
  digitalWrite(TOGGLE_GNSS, HIGH);

  delay(1000);  // temporary delay

  //Set up the GNSS module
  Serial2.setTX(4);
  Serial2.setRX(5);
  Serial2.begin(460800);
}

//Function to pull data from GNSS
bool getGNSSData() {
  // NOTE: only works if the LG290P GNSS module is configured in default NMEA output
  unsigned long gnssStartTime = millis();
  GNSS = TinyGPSPlus();
  gGnssModuleDetected = false;

  // First, do a short presence probe to skip the long acquisition timeout
  // if the GNSS module is simply not connected.
  const unsigned long detectStartTime = millis();
  while (millis() - detectStartTime <= 12000) {
    while (Serial2.available()) {
      const int c = Serial2.read();
      if (c < 0) continue;
      GNSS.encode((char)c);

      // Accept either standard NMEA traffic ('$') or UBX sync bytes as evidence
      // that a real GNSS module is alive on the UART.
      if (c == '$' || c == 0xB5 || c == 0x62) {
        gGnssModuleDetected = true;
      }
    }

    if (gGnssModuleDetected) {
      break;
    }
  }

  if (!gGnssModuleDetected) {
    return false;
  }

  while (!hasGnssDateTimeFix()) {

    // Keep reading until the minimum downstream metadata is available.
    // Date + time are sufficient for timestamp backtracking, Iridium day-reset
    // logic, and the threshold-only Iridium message path. Location remains
    // optional and will be included later if it becomes valid.
    while (Serial2.available()) {
      GNSS.encode(Serial2.read());
    }

    // Cut reading if date/time are not found in 60 seconds.
    if (millis() - gnssStartTime > 60000) {
      return false;
    }
  }
  startMillis = millis() - startMillis;

  return true;
}

static bool probeIridiumModulePresent() {
  // Send a few simple AT probes and look for an OK response. This is a quick
  // "is anything alive on this UART?" check so the deployment flow can skip the
  // heavier Iridium init path when the modem is physically absent.
  while (IridiumSerial.available()) {
    IridiumSerial.read();
  }

  const unsigned long probeStart = millis();
  unsigned long lastProbe = 0;
  bool sawO = false;

  while (millis() - probeStart <= 2500) {
    const unsigned long now = millis();
    if (now - lastProbe >= 250) {
      IridiumSerial.print(F("AT\r"));
      lastProbe = now;
    }

    while (IridiumSerial.available()) {
      const char c = (char)IridiumSerial.read();
      if (c == 'O') {
        sawO = true;
      } else if (sawO && c == 'K') {
        return true;
      } else {
        sawO = false;
      }
    }
  }

  return false;
}

//Function to setup Iridium
bool setupIridium(int& beginErr, int& signalErr, int& signalQuality) {
  beginErr = -1;
  signalErr = -1;
  signalQuality = -1;

  // Power on the Iridium modem and start its UART before attempting a quick
  // presence probe.
  pinMode(IRIDIUM_PWR_PIN, OUTPUT);
  digitalWrite(IRIDIUM_PWR_PIN, HIGH);
  delay(5000);    // 5 second wait to allow modem to wake
  IridiumSerial.begin(19200);
  gIridiumModuleDetected = probeIridiumModulePresent();

  if (!gIridiumModuleDetected) {
    IridiumSerial.end();
    digitalWrite(IRIDIUM_PWR_PIN, LOW);
    return false;
  }

  // Begin satellite modem operation
  modem.setPowerProfile(IridiumSBD::USB_POWER_PROFILE);
  beginErr = modem.begin();

  if (beginErr != ISBD_SUCCESS) {
    return false;
  }

  // Optional: check signal quality (0..5)
  signalErr = modem.getSignalQuality(signalQuality);
  return true;
}

//Function to send the message through Iridium
bool sendIridiumMessage(const char* message, int& sendErr) {
  sendErr = modem.sendSBDBinary((const uint8_t*)message, strlen(message) + 1);
  if (sendErr == ISBD_SUCCESS) {
    bytesUsedThisMonth += (messageBytes + 1);
    iridiumDayCount += 1;
    return true;
  }
  return false;
}

static bool readTextFileInt(const char* path, int& out) {
  if (!tempFile.open(path, O_RDONLY)) {
    return false;
  }

  char buf[24];
  const int n = tempFile.read(buf, sizeof(buf) - 1);
  tempFile.close();
  if (n <= 0) {
    return false;
  }

  buf[n] = '\0';
  char* end = nullptr;
  const long value = strtol(buf, &end, 10);
  if (end == buf) {
    return false;
  }

  out = (int)value;
  return true;
}

static bool readTextFileFloat(const char* path, float& out) {
  if (!tempFile.open(path, O_RDONLY)) {
    return false;
  }

  char buf[32];
  const int n = tempFile.read(buf, sizeof(buf) - 1);
  tempFile.close();
  if (n <= 0) {
    return false;
  }

  buf[n] = '\0';
  char* end = nullptr;
  const float value = strtof(buf, &end);
  if (end == buf) {
    return false;
  }

  out = value;
  return true;
}

static bool parseIridiumStateValue(const char* text, const char* key, int& out) {
  if (!text || !key) return false;

  const char* found = strstr(text, key);
  if (!found) return false;
  found += strlen(key);

  char* end = nullptr;
  const long value = strtol(found, &end, 10);
  if (end == found) return false;

  out = (int)value;
  return true;
}

static bool loadIridiumState() {
  FsFile stateFile;
  if (!stateFile.open(kIridiumStatePath, O_RDONLY)) {
    return false;
  }

  char buf[128];
  const int n = stateFile.read(buf, sizeof(buf) - 1);
  stateFile.close();
  if (n <= 0) {
    return false;
  }
  buf[n] = '\0';

  int loadedBytes = 0;
  int loadedQuota = 0;
  int loadedDay = 100;
  int loadedCount = 0;

  const bool ok =
      parseIridiumStateValue(buf, "bytes=", loadedBytes) &&
      parseIridiumStateValue(buf, "quota=", loadedQuota) &&
      parseIridiumStateValue(buf, "day=", loadedDay) &&
      parseIridiumStateValue(buf, "count=", loadedCount);

  if (!ok) {
    return false;
  }

  bytesUsedThisMonth = loadedBytes;
  alreadyResetQuota = loadedQuota;
  iridiumDay = loadedDay;
  iridiumDayCount = loadedCount;
  return true;
}

static bool saveIridiumState() {
  FsFile stateFile;
  SD.remove(kIridiumStateTempPath);

  if (!stateFile.open(kIridiumStateTempPath, O_WRONLY | O_CREAT | O_TRUNC)) {
    return false;
  }

  char buf[128];
  const int len = snprintf(
      buf,
      sizeof(buf),
      "bytes=%d\r\nquota=%d\r\nday=%d\r\ncount=%d\r\n",
      bytesUsedThisMonth,
      alreadyResetQuota,
      iridiumDay,
      iridiumDayCount);
  if (len <= 0 || len >= (int)sizeof(buf)) {
    stateFile.close();
    SD.remove(kIridiumStateTempPath);
    return false;
  }

  if (stateFile.write(reinterpret_cast<const uint8_t*>(buf), (size_t)len) != len) {
    stateFile.close();
    SD.remove(kIridiumStateTempPath);
    return false;
  }

  if (!stateFile.sync()) {
    stateFile.close();
    SD.remove(kIridiumStateTempPath);
    return false;
  }
  stateFile.close();

  SD.remove(kIridiumStatePath);
  if (!SD.rename(kIridiumStateTempPath, kIridiumStatePath)) {
    SD.remove(kIridiumStateTempPath);
    return false;
  }

  return true;
}

static bool hasGnssTimestamp() {
  return hasGnssDateTimeFix();
}

static bool hasGnssDateTimeFix() {
  return GNSS.date.isValid() && GNSS.time.isValid();
}

static bool parseIndexedFileName(const char* name, uint32_t& indexOut) {
  if (!name) return false;

  const char* base = name;
  const char* slash = strrchr(name, '/');
  if (slash && slash[1] != '\0') {
    base = slash + 1;
  }

  const char* dot = strrchr(base, '.');
  if (!dot || dot == base) return false;

  uint32_t value = 0;
  for (const char* p = base; p < dot; ++p) {
    if (*p < '0' || *p > '9') return false;
    const uint32_t digit = (uint32_t)(*p - '0');
    if (value > 429496729U) return false;
    if (value == 429496729U && digit > 5U) return false;
    value = value * 10U + digit;
  }

  indexOut = value;
  return true;
}

static bool readSamdRecordingDurationMs(uint32_t index, uint32_t& outDurationMs) {
  char statsPath[32];
  if (!makeIndexedPath(index, "txt", statsPath, sizeof(statsPath))) {
    return false;
  }

  FsFile statsFile;
  if (!statsFile.open(statsPath, O_RDONLY)) {
    return false;
  }

  char buf[512];
  const int n = statsFile.read(buf, sizeof(buf) - 1);
  statsFile.close();
  if (n <= 0) {
    return false;
  }
  buf[n] = '\0';

  const char* found = strstr(buf, "Time During Recording (ms):");
  if (!found) {
    found = strstr(buf, "Time During Recording:");
  }
  if (!found) {
    return false;
  }

  const char* colon = strchr(found, ':');
  if (!colon) {
    return false;
  }
  found = colon + 1;
  while (*found == ' ' || *found == '\t') {   // loop only advances 'found' through a finite, null-terminated buffer, so it won't be infinite
    ++found;
  }

  char* end = nullptr;
  const unsigned long value = strtoul(found, &end, 10);
  if (end == found) {
    return false;
  }

  outDurationMs = (uint32_t)value;
  return true;
}

static int32_t daysFromCivil(int32_t year, uint32_t month, uint32_t day) {
  year -= month <= 2U;
  const int32_t era = (year >= 0 ? year : year - 399) / 400;
  const uint32_t yoe = (uint32_t)(year - era * 400);
  const uint32_t doy = (153U * (month + (month > 2U ? (uint32_t)-3 : 9U)) + 2U) / 5U + day - 1U;
  const uint32_t doe = yoe * 365U + yoe / 4U - yoe / 100U + doy;
  return era * 146097 + (int32_t)doe - 719468;
}

static void civilFromDays(int32_t z, uint16_t& year, uint8_t& month, uint8_t& day) {
  z += 719468;
  const int32_t era = (z >= 0 ? z : z - 146096) / 146097;
  const uint32_t doe = (uint32_t)(z - era * 146097);
  const uint32_t yoe = (doe - doe / 1460U + doe / 36524U - doe / 146096U) / 365U;
  const int32_t y = (int32_t)yoe + era * 400;
  const uint32_t doy = doe - (365U * yoe + yoe / 4U - yoe / 100U);
  const uint32_t mp = (5U * doy + 2U) / 153U;
  const uint32_t d = doy - (153U * mp + 2U) / 5U + 1U;
  const uint32_t m = mp + (mp < 10U ? 3U : (uint32_t)-9);

  year = (uint16_t)(y + (m <= 2U));
  month = (uint8_t)m;
  day = (uint8_t)d;
}

static bool gnssToUnixUtc(uint32_t& outEpochSeconds) {
  if (!hasGnssTimestamp()) {
    return false;
  }

  const int32_t days = daysFromCivil((int32_t)GNSS.date.year(), GNSS.date.month(), GNSS.date.day());
  const uint32_t secondsOfDay =
      (uint32_t)GNSS.time.hour() * 3600UL +
      (uint32_t)GNSS.time.minute() * 60UL +
      (uint32_t)GNSS.time.second();

  const int64_t epoch = (int64_t)days * 86400LL + (int64_t)secondsOfDay;
  if (epoch < 0) {
    return false;
  }

  outEpochSeconds = (uint32_t)epoch;
  return true;
}

static void unixUtcToCalendar(uint32_t epochSeconds, uint16_t& year, uint8_t& month, uint8_t& day,
                              uint8_t& hour, uint8_t& minute, uint8_t& second) {
  const uint32_t secondsOfDay = epochSeconds % 86400UL;
  const int32_t days = (int32_t)(epochSeconds / 86400UL);

  civilFromDays(days, year, month, day);

  hour = (uint8_t)(secondsOfDay / 3600UL);
  minute = (uint8_t)((secondsOfDay % 3600UL) / 60UL);
  second = (uint8_t)(secondsOfDay % 60UL);
}

static bool applyGnssTimestampToFile(const String& fileName) {

  if (!hasGnssTimestamp()) {
    return false;
  }

  uint32_t index = 0;
  if (!parseIndexedFileName(fileName.c_str(), index)) {
    return false;
  }

  uint32_t samdDurationMs = 0;
  if (!readSamdRecordingDurationMs(index, samdDurationMs)) {
    return false;
  }

  uint32_t gnssEpochSeconds = 0;
  if (!gnssToUnixUtc(gnssEpochSeconds)) {
    return false;
  }

  const uint64_t totalBacktrackMs = (uint64_t)startMillis + (uint64_t)samdDurationMs;
  const uint32_t backtrackSeconds = (uint32_t)(totalBacktrackMs / 1000ULL);
  if (gnssEpochSeconds < backtrackSeconds) {
    return false;
  }

  const uint32_t eventEpochSeconds = gnssEpochSeconds - backtrackSeconds;
  uint16_t year = 0;
  uint8_t month = 0;
  uint8_t day = 0;
  uint8_t hour = 0;
  uint8_t minute = 0;
  uint8_t second = 0;
  unixUtcToCalendar(eventEpochSeconds, year, month, day, hour, minute, second);

  if (!tempFile.open(fileName.c_str(), O_RDWR)) {
    return false;
  }

  tempFile.timestamp(T_CREATE, year, month, day, hour, minute, second);
  tempFile.timestamp(T_WRITE, year, month, day, hour, minute, second);
  tempFile.timestamp(T_ACCESS, year, month, day, hour, minute, second);
  tempFile.close();
  return true;
}

static void buildIridiumMessage(float thresholdG, bool hasThreshold) {
  if (hasThreshold && GNSS.location.isValid() && GNSS.date.isValid() && GNSS.time.isValid()) {
    snprintf(
      gIridiumMessage,
      sizeof(gIridiumMessage),
      "th=%.6f,la=%.6f,lo=%.6f,dt=%04d%02d%02d%02d%02d%02d",
      (double)thresholdG,
      GNSS.location.lat(),
      GNSS.location.lng(),
      GNSS.date.year(),
      GNSS.date.month(),
      GNSS.date.day(),
      GNSS.time.hour(),
      GNSS.time.minute(),
      GNSS.time.second());
  } else if (hasThreshold) {
    snprintf(
      gIridiumMessage,
      sizeof(gIridiumMessage),
      "th=%.6f",
      (double)thresholdG);
  } else {
    snprintf(
      gIridiumMessage,
      sizeof(gIridiumMessage),
      "th=na");
  }
  messageBytes = (int)strlen(gIridiumMessage);
}

// -----------------------------------------------------------------------------
// Benchmarking
// -----------------------------------------------------------------------------

static BenchTimes gBench;

static inline uint32_t elapsedMicros(uint32_t start) {
  return (uint32_t)(micros() - start);
}

static void printBenchResults(const BenchTimes& b) {
  Serial.println();
  Serial.println(F("=== benchmark ==="));
  Serial.print(F("discover_us: "));
  Serial.println(b.discover_us);
  Serial.print(F("dcra_us:     "));
  Serial.println(b.dcra_us);
  Serial.print(F("rename_us:   "));
  Serial.println(b.rename_us);
  Serial.print(F("stream_us:   "));
  Serial.println(b.stream_us);
  Serial.print(F("tail_us:     "));
  Serial.println(b.tail_us);
  Serial.print(F("worker_us:   "));
  Serial.println(b.worker_us);
  Serial.print(F("dt_save_us:  "));
  Serial.println(b.dt_save_us);
  Serial.print(F("sensor_us:   "));
  Serial.println(b.sensor_us);
  Serial.print(F("total_us:    "));
  Serial.println(b.total_us);
}

enum ErrorCode : uint8_t {
  ERR_SD_BEGIN = 2,
  ERR_NO_RECORDING = 3,
  ERR_MODEL_INIT = 4,
  ERR_BIN_PATH = 5,
  ERR_FFT_WRITE = 6,
  ERR_OPEN_INPUT = 7,
  ERR_PARSE_WAV = 8,
  ERR_OPEN_STAGE = 9,
  ERR_PIPELINE_RUN = 10,
  ERR_DCRA = 11,
  ERR_RENAME = 12,
  ERR_OPEN_BIN = 21,
  ERR_PREALLOC = 22,
  ERR_FFT_DRAIN = 23,
  ERR_CORE1 = 27,
  ERR_DT_SAVE = 29,
  ERR_SENSOR_WRITE = 31,
  ERR_RUN_LOG = 32,
  ERR_CORE1_TIMEOUT = 33,
};

struct DebugState {
  const char* stage = "boot";
  char wavPath[32] = { 0 };
  char binPath[32] = { 0 };
  bool hasIndex = false;
  uint32_t index = 0;
  uint32_t blockIndex = 0;
  uint32_t frameIndex = 0;
  uint32_t sampleRate = 0;
  uint32_t dataSize = 0;
};

static DebugState gDebug;
static volatile bool gFatalFailure = false;
static volatile uint8_t gFatalCode = 0;
static uint32_t gSetupStartMs = 0;
static bool gProgramTotalRuntimeLogged = false;

static bool appendCurrentRunErrorLog(uint8_t code);
static bool appendCurrentRunSuccessLog(const ThresholdSnapshot& snapshot, float thresholdG, const BenchTimes& bench);
static bool appendCurrentRunStatusLog(const char* key, const char* value);
static bool appendCurrentRunGnssLog();
static bool appendCurrentRunTotalRuntimeLog();

static void setProgramLedState(bool active) {
  const uint8_t state = (kKeepBuiltinLedOnDuringProgram && active) ? HIGH : LOW;
  digitalWrite(LED_PIN, state);
}

static void blinkProgramStageComplete(uint8_t count = 1U) {
  static constexpr uint16_t kStageBlinkOnMs = 120U;
  static constexpr uint16_t kStageBlinkOffMs = 120U;

  const bool keepLedOn = kKeepBuiltinLedOnDuringProgram;
  for (uint8_t i = 0; i < count; ++i) {
    if (keepLedOn) {
      digitalWrite(LED_PIN, LOW);
      delay(kStageBlinkOffMs);
      digitalWrite(LED_PIN, HIGH);
      delay(kStageBlinkOnMs);
    } else {
      digitalWrite(LED_PIN, HIGH);
      delay(kStageBlinkOnMs);
      digitalWrite(LED_PIN, LOW);
      delay(kStageBlinkOffMs);
    }
  }
}

static void waitForDebugSerial() {
  if (!kDebugPipeline) {
    return;
  }

  const unsigned long start = millis();
  while (!Serial && (millis() - start < kDebugSerialWaitMs)) {
    delay(10);
  }

  Serial.println(F("[setup] Serial diagnostics armed"));
}

static void debugPrintStartupStep(const __FlashStringHelper* msg) {
  if (kDebugPipeline) {
    Serial.println(msg);
  }
}

static void setDebugPaths(const char* wavPath, const char* binPath) {
  if (wavPath) {
    snprintf(gDebug.wavPath, sizeof(gDebug.wavPath), "%s", wavPath);
  } else {
    gDebug.wavPath[0] = '\0';
  }
  if (binPath) {
    snprintf(gDebug.binPath, sizeof(gDebug.binPath), "%s", binPath);
  } else {
    gDebug.binPath[0] = '\0';
  }
}

static const char* errorName(uint8_t code) {
  switch (code) {
    case ERR_SD_BEGIN: return "ERR_SD_BEGIN";
    case ERR_NO_RECORDING: return "ERR_NO_RECORDING";
    case ERR_MODEL_INIT: return "ERR_MODEL_INIT";
    case ERR_BIN_PATH: return "ERR_BIN_PATH";
    case ERR_FFT_WRITE: return "ERR_FFT_WRITE";
    case ERR_OPEN_INPUT: return "ERR_OPEN_INPUT";
    case ERR_PARSE_WAV: return "ERR_PARSE_WAV";
    case ERR_OPEN_STAGE: return "ERR_OPEN_STAGE";
    case ERR_PIPELINE_RUN: return "ERR_PIPELINE_RUN";
    case ERR_DCRA: return "ERR_DCRA";
    case ERR_RENAME: return "ERR_RENAME";
    case ERR_OPEN_BIN: return "ERR_OPEN_BIN";
    case ERR_PREALLOC: return "ERR_PREALLOC";
    case ERR_FFT_DRAIN: return "ERR_FFT_DRAIN";
    case ERR_CORE1: return "ERR_CORE1";
    case ERR_DT_SAVE: return "ERR_DT_SAVE";
    case ERR_SENSOR_WRITE: return "ERR_SENSOR_WRITE";
    case ERR_RUN_LOG: return "ERR_RUN_LOG";
    case ERR_CORE1_TIMEOUT: return "ERR_CORE1_TIMEOUT";
    default: return "ERR_UNKNOWN";
  }
}

static const char* labelName(uint8_t label) {
  switch (label) {
    case 0: return "ambient";
    case 1: return "event";
    case 2: return "storm";
    default: return "unknown";
  }
}

static void printThresholdSnapshot(const ThresholdSnapshot& s) {
  Serial.println(F("=== final threshold snapshot ==="));
  Serial.print(F("thresholdLow:  "));
  Serial.println(s.thresholdLow, 6);
  Serial.print(F("thresholdHigh: "));
  Serial.println(s.thresholdHigh, 6);
  Serial.print(F("lowTarget:     "));
  Serial.println(s.lowTarget, 6);
  Serial.print(F("highTarget:    "));
  Serial.println(s.highTarget, 6);

  Serial.print(F("ambient mu/std/w: "));
  Serial.print(s.muAmbient, 6);
  Serial.print(F(" / "));
  Serial.print(s.stddevAmbient, 6);
  Serial.print(F(" / "));
  Serial.println(s.weightAmbient, 6);
  Serial.print(F("event   mu/std/w: "));
  Serial.print(s.muEvent, 6);
  Serial.print(F(" / "));
  Serial.print(s.stddevEvent, 6);
  Serial.print(F(" / "));
  Serial.println(s.weightEvent, 6);
  Serial.print(F("storm   mu/std/w: "));
  Serial.print(s.muStorm, 6);
  Serial.print(F(" / "));
  Serial.print(s.stddevStorm, 6);
  Serial.print(F(" / "));
  Serial.println(s.weightStorm, 6);

  Serial.print(F("orderedCount: "));
  Serial.println(s.orderedCount);
  Serial.print(F("orderedLabels: "));
  for (uint8_t i = 0; i < 3; ++i) {
    if (i) Serial.print(F(", "));
    if (s.orderedLabels[i] == 255U) {
      Serial.print(F("-"));
    } else {
      Serial.print(s.orderedLabels[i]);
      Serial.print(F("("));
      Serial.print(labelName(s.orderedLabels[i]));
      Serial.print(F(")"));
    }
  }
  Serial.println();
}

static void printPersistentDtState(const PersistentDTState& s, uint32_t sequenceOverride) {
  Serial.println(F("=== dynamic threshold database ==="));
  Serial.print(F("magic: 0x"));
  Serial.println((uint32_t)s.magic, HEX);
  Serial.print(F("version: "));
  Serial.println(s.version);
  Serial.print(F("sequence: "));
  Serial.println(sequenceOverride);
  Serial.print(F("checksum: 0x"));
  Serial.println((uint32_t)s.checksum, HEX);
  Serial.print(F("thresholdLow:  "));
  Serial.println(s.thresholdLow, 6);
  Serial.print(F("thresholdHigh: "));
  Serial.println(s.thresholdHigh, 6);

  for (uint8_t i = 0; i < 3; ++i) {
    Serial.print(F("label "));
    Serial.print(i);
    Serial.print(F(" mean: "));
    Serial.print(s.mean[i], 6);
    Serial.print(F(", m2: "));
    Serial.print(s.m2[i], 6);
    Serial.print(F(", weightSum: "));
    Serial.println(s.weightSum[i], 6);
  }

  Serial.print(F("lastOrderValid: "));
  Serial.println(s.lastOrderValid);
  Serial.print(F("lastOrderedCount: "));
  Serial.println(s.lastOrderedCount);
  Serial.print(F("lastOrderedLabels: "));
  for (uint8_t i = 0; i < 3; ++i) {
    if (i) Serial.print(F(", "));
    if (s.lastOrderedLabels[i] == 255U) {
      Serial.print(F("-"));
    } else {
      Serial.print(s.lastOrderedLabels[i]);
      Serial.print(F("("));
      Serial.print(labelName(s.lastOrderedLabels[i]));
      Serial.print(F(")"));
    }
  }
  Serial.println();
  Serial.print(F("boostRemaining: "));
  Serial.println(s.boostRemaining);
}

static void printTupleDebug(uint8_t label, float amplitude, float confidence) {
  if (!kDebugPrintTuples) return;
  if (!Serial) return;
  Serial.print(F("[tuple] label="));
  Serial.print(label);
  Serial.print(F(" ("));
  Serial.print(labelName(label));
  Serial.print(F(") amplitude="));
  Serial.print(amplitude, 6);
  Serial.print(F(" confidence="));
  Serial.println(confidence, 6);
}

// -----------------------------------------------------------------------------
// Error handling
// -----------------------------------------------------------------------------

void errorHalt(uint8_t blinkNumber = 1) {
  (void)blinkNumber;
  pinMode(LED_PIN, OUTPUT);

  static constexpr uint8_t kErrorBlinkCount = 3U;
  static constexpr uint16_t kErrorBlinkOnMs = 120U;
  static constexpr uint16_t kErrorBlinkOffMs = 120U;

  for (uint8_t i = 0; i < kErrorBlinkCount; ++i) {
    digitalWrite(LED_PIN, HIGH);
    delay(kErrorBlinkOnMs);
    digitalWrite(LED_PIN, LOW);
    delay(kErrorBlinkOffMs);
  }

  setProgramLedState(false);
}

static void failImpl(uint8_t code, const __FlashStringHelper* msg, int line) {
  if (gFatalFailure) {
    return;
  }
  if (kDebugPipeline) {
    Serial.print(F("[fatal] code="));
    Serial.print((unsigned)code);
    Serial.print(F(" name="));
    Serial.print(errorName(code));
    Serial.print(F(" stage="));
    Serial.print(gDebug.stage ? gDebug.stage : "unknown");
    Serial.print(F(" line="));
    Serial.println(line);
    if (msg) {
      Serial.print(F("[fatal] msg="));
      Serial.println(msg);
    }
  }
  gFatalFailure = true;
  gFatalCode = code;
  appendCurrentRunErrorLog(code);
  errorHalt(code);
}

#define FAIL(code, msg) failImpl((uint8_t)(code), F(msg), __LINE__)

static void finalizeDeploymentShutdown() {
  if (gAdxlWakeThresholdReady && !currentRunShouldLockdown()) {
    const bool restoredMeasurement = setAdxlMeasurementMode();
    if (kDebugPipeline && Serial) {
      Serial.print(F("[shutdown] ADXL measurement restore="));
      Serial.println(restoredMeasurement ? F("ok") : F("failed"));
      Serial.flush();
    }
  } else if (gAdxlWakeThresholdReady && kDebugPipeline && Serial) {
    Serial.println(F("[shutdown] ADXL lockdown active; leaving sensor in standby"));
    Serial.flush();
  }

  if (!appendCurrentRunTotalRuntimeLog() && kDebugPipeline && Serial) {
    Serial.println(F("[shutdown] Warning: failed to append program total runtime"));
    Serial.flush();
  }

  if (kDebugPipeline) {
    Serial.println(F("[shutdown] Entering final shutdown sequence"));
    Serial.print(F("[shutdown] fatal="));
    Serial.println(gFatalFailure ? F("true") : F("false"));
    Serial.print(F("[shutdown] stage="));
    Serial.println(gDebug.stage ? gDebug.stage : "unknown");
    if (gFatalFailure) {
      Serial.print(F("[shutdown] code="));
      Serial.print((unsigned)gFatalCode);
      Serial.print(F(" name="));
      Serial.println(errorName(gFatalCode));
    }
    Serial.flush();
    delay(250);
  }

  if (kBypassFinalShutdownForDebug) {
    if (kDebugPipeline) {
      Serial.println(F("[shutdown] Debug bypass active; skipping kill-pin shutdown"));
      Serial.flush();
    }
    return;
  }

  pinMode(SD_SCK_PIN, INPUT);
  pinMode(SD_MOSI_PIN, INPUT);
  pinMode(SD_MISO_PIN, INPUT);
  pinMode(SD_CS_PIN, INPUT);

  digitalWrite(KILL_SD_PIN, LOW);
  delay(1000);
  digitalWrite(KILL_SD_PIN, HIGH);
  delay(1000);
  digitalWrite(KILL_PICO_PIN, LOW);
  delay(100);

  while (true) {}   // RP2040 should terminate itself by this point
}

// -----------------------------------------------------------------------------
// WAV helpers shared by DCRA and FFT
// -----------------------------------------------------------------------------

static uint16_t readU16LE(FsFile& f) {
  // Reads one unsigned 16-bit little-endian value from the current file position.
  // This is the standard WAV byte order, and the function avoids any alignment or
  // endianness assumptions by reading raw bytes and assembling the integer
  // explicitly.
  // Returns 0 if the read fails, which is acceptable here because the caller
  // immediately validates the WAV structure and format fields.
  uint8_t b[2];
  if (f.read(b, 2) != 2) return 0;
  return (uint16_t)b[0] | ((uint16_t)b[1] << 8);
}

static uint32_t readU32LE(FsFile& f) {
  // Reads one unsigned 32-bit little-endian value from the current file position.
  // Used for RIFF sizes, sample rate, byte rate, and chunk sizes in the WAV
  // header. The explicit byte assembly keeps the code portable and safe on RP2040.
  uint8_t b[4];
  if (f.read(b, 4) != 4) return 0;
  return (uint32_t)b[0]
         | ((uint32_t)b[1] << 8)
         | ((uint32_t)b[2] << 16)
         | ((uint32_t)b[3] << 24);
}

static void writeU32LE(uint8_t* p, uint32_t v) {
  // Writes a 32-bit value into a byte buffer in little-endian order.
  // Used when patching the WAV header sizes after processing so the RIFF and data
  // chunk lengths match the new output file contents.
  p[0] = (uint8_t)(v & 0xFF);
  p[1] = (uint8_t)((v >> 8) & 0xFF);
  p[2] = (uint8_t)((v >> 16) & 0xFF);
  p[3] = (uint8_t)((v >> 24) & 0xFF);
}

static int16_t clamp16(int32_t v) {
  // Saturates a signed 32-bit intermediate value into the valid 16-bit PCM range.
  // This is the correct way to keep DC-removed or otherwise processed samples from
  // wrapping on overflow. Saturation is preferable to a plain cast because it
  // preserves waveform integrity.
  if (v > 32767) return 32767;
  if (v < -32768) return -32768;
  return (int16_t)v;
}

static bool copyBytes(FsFile& inFile, FsFile& outFile, uint32_t count) {
  // Copies a raw byte span from one file to another in fixed-size blocks.
  // Used primarily for copying the WAV header before rewriting the processed audio
  // payload. The small local buffer keeps stack usage modest while remaining simple
  // and robust for short transfers.
  constexpr size_t kBufSize = 256;
  uint8_t buf[kBufSize];

  while (count > 0) {
    const uint32_t n = (count > kBufSize) ? kBufSize : count;
    const int r = inFile.read(buf, n);
    if (r != (int)n) return false;
    if (outFile.write(buf, n) != (int)n) return false;
    count -= n;
  }

  return true;
}

static bool preallocateOutputFiles(FsFile& stageFile, FsFile& binOut, const WavInfo& info) {
  // Preallocates the output files up front so long recordings do not fail later
  // due to allocation pressure, fragmentation, or gradual file growth on SD.
  const uint64_t stageBytes = (uint64_t)info.dataStart + (uint64_t)info.dataSize;
  const uint32_t totalFrames = info.dataSize / info.blockAlign;
  const uint32_t fftFrames = (totalFrames + (uint32_t)kFftBins - 1U) / (uint32_t)kFftBins;
  const uint64_t binBytes = (uint64_t)fftFrames * (uint64_t)sizeof(powerFrame);

  if (kDebugPipeline && Serial) {
    Serial.print(F("[prealloc] stageBytes="));
    Serial.print((uint32_t)(stageBytes >> 32));
    Serial.print(F(":"));
    Serial.print((uint32_t)(stageBytes & 0xFFFFFFFFUL));
    Serial.print(F(" binBytes="));
    Serial.print((uint32_t)(binBytes >> 32));
    Serial.print(F(":"));
    Serial.println((uint32_t)(binBytes & 0xFFFFFFFFUL));
  }

  if (!stageFile.preAllocate(stageBytes)) return false;
  if (!binOut.preAllocate(binBytes)) return false;
  return true;
}

static bool parseWavHeader(FsFile& f, WavInfo& info) {
  // Parses the WAV container structure and extracts the metadata needed by the
  // pipeline: channels, sample rate, bits per sample, block alignment, data size,
  // and data start offset.
  // The function walks the RIFF chunks until it finds both "fmt " and "data",
  // which is the standard way to handle WAV files without assuming fixed offsets.
  // It also enforces the deployment constraints for this pipeline:
  // - PCM format only
  // - mono only
  // - 16-bit samples only
  // - blockAlign must be 2 bytes
  //
  // This keeps the rest of the pipeline simple and prevents unsupported files from
  // reaching the hot paths.
  f.seekSet(0);

  uint8_t id[4];
  if (f.read(id, 4) != 4 || memcmp(id, "RIFF", 4) != 0) return false;
  (void)readU32LE(f);  // RIFF size
  if (f.read(id, 4) != 4 || memcmp(id, "WAVE", 4) != 0) return false;

  bool foundFmt = false;
  bool foundData = false;

  while (!foundData) {
    if (f.read(id, 4) != 4) return false;
    const uint32_t chunkSize = readU32LE(f);
    const uint32_t chunkDataStart = (uint32_t)f.curPosition();

    if (memcmp(id, "fmt ", 4) == 0) {
      const uint16_t audioFormat = readU16LE(f);
      info.channels = readU16LE(f);
      info.sampleRate = readU32LE(f);
      (void)readU32LE(f);  // byte rate
      info.blockAlign = readU16LE(f);
      info.bitsPerSample = readU16LE(f);

      if (audioFormat != 1) return false;
      if (info.channels != 1) return false;
      if (info.bitsPerSample != 16) return false;
      if (info.blockAlign != 2) return false;

      foundFmt = true;

      if (chunkSize > 16) {
        const uint32_t skip = chunkSize + (chunkSize & 1U);
        f.seekSet(chunkDataStart + skip);
      }
    } else if (memcmp(id, "data", 4) == 0) {
      info.dataSize = chunkSize;
      info.dataStart = chunkDataStart;
      foundData = true;
      break;
    } else {
      const uint32_t skip = chunkSize + (chunkSize & 1U);
      f.seekSet(chunkDataStart + skip);
    }
  }

  return foundFmt && foundData && info.dataSize > 0;
}

static bool patchOutputSizes(FsFile& outFile, uint32_t dataStart, uint32_t dataSize) {
  // Rewrites the RIFF size and data chunk size fields after processing.
  // This is required because the output file is written in stages: the header is
  // copied first, the processed PCM payload is written afterward, and the size
  // fields must be patched to match the final output length.
  if (dataStart < 8U) return false;

  uint8_t b[4];

  const uint32_t riffSize = dataStart + dataSize - 8U;
  writeU32LE(b, riffSize);
  if (!outFile.seekSet(4)) return false;
  if (outFile.write(b, 4) != 4) return false;

  writeU32LE(b, dataSize);
  if (!outFile.seekSet(dataStart - 4)) return false;
  if (outFile.write(b, 4) != 4) return false;

  return true;
}


static bool copyHeaderToOutput(FsFile& inFile, FsFile& outFile, uint32_t dataStart) {
  // Copies the WAV header bytes from the input file to the output file.
  // The payload is processed separately, so this helper preserves the original
  // header structure before the data chunk is rewritten and the sizes are patched.
  inFile.seekSet(0);
  return copyBytes(inFile, outFile, dataStart);
}

static bool readExactBytes(FsFile& f, uint8_t* dst, uint32_t count) {
  // Reads exactly the requested byte count unless the file ends or an error occurs.
  // File APIs may return partial reads, especially on SD-backed storage, so this
  // helper loops until all requested bytes are received or a failure is detected.
  uint32_t done = 0;
  while (done < count) {
    const int r = f.read(dst + done, count - done);
    if (r <= 0) return false;
    done += (uint32_t)r;
  }
  return true;
}

static bool writeExactBytes(FsFile& f, const uint8_t* src, uint32_t count) {
  // Writes exactly the requested byte count unless the file write fails.
  // This mirrors readExactBytes() and protects the pipeline from partial writes,
  // which is important when writing processed audio back to SD.
  uint32_t done = 0;
  while (done < count) {
    const int w = f.write(src + done, count - done);
    if (w <= 0) return false;
    done += (uint32_t)w;
  }
  return true;
}

static uint32_t reflectIndex(int64_t v, uint32_t n) {
  // Maps an out-of-range sample index back into the valid range using reflection.
  // This provides mirror-style boundary handling for DCRA, which avoids zero-pad
  // discontinuities at the edges of the file.
  // The fast path returns immediately for interior indices, so the expensive
  // modulo-based reflection is only used when the index actually falls outside the
  // valid interval.
  if (n == 0U) return 0U;
  if (n == 1U) return 0U;
  if (v >= 0 && v < (int64_t)n) return (uint32_t)v;

  const int64_t period = (int64_t)(2U * n - 2U);
  int64_t x = v % period;
  if (x < 0) x += period;
  if (x < (int64_t)n) return (uint32_t)x;
  return (uint32_t)(period - x);
}

// -----------------------------------------------------------------------------
// DCRA: centered moving-average DC bias removal
// -----------------------------------------------------------------------------

static inline int16_t readPcm16AtFrame(const uint8_t* raw, uint32_t frameOffset, uint16_t blockAlign) {
  // Reads one signed 16-bit PCM sample from a byte buffer at the requested frame offset.
  // The helper is byte-safe: it never casts the buffer to int16_t*, which avoids
  // alignment and aliasing problems on RP2040.
  // The blockAlign argument is retained so the helper can index in frame units,
  // even though the active pipeline is mono 16-bit and blockAlign is always 2.
  const uint32_t offset = frameOffset * blockAlign;
  const uint16_t lo = raw[offset + 0];
  const uint16_t hi = raw[offset + 1];
  return (int16_t)((uint16_t)lo | ((uint16_t)hi << 8));
}

static inline void writePcm16AtFrame(uint8_t* raw, uint32_t frameOffset, uint16_t blockAlign, int32_t value) {
  // Writes one signed 16-bit PCM sample back into a byte buffer at the requested frame offset.
  // The sample is first saturated through clamp16() so processed values cannot wrap on overflow.
  // Like the read helper, this stays byte-safe and avoids any int16_t pointer
  // casting in the hot path.
  const uint32_t offset = frameOffset * blockAlign;
  const int16_t s = clamp16(value);
  raw[offset + 0] = (uint8_t)(s & 0xFF);
  raw[offset + 1] = (uint8_t)((s >> 8) & 0xFF);
}

static inline int16_t decodePcm16Sample(const uint8_t* p) {
  // Minimal 2-byte little-endian decode helper for signed 16-bit PCM.
  // This is used when the code wants the sample value directly from a raw byte
  // pointer without going through a frame-based accessor.
  // It is intentionally simple and portable.
  return (int16_t)((uint16_t)p[0] | ((uint16_t)p[1] << 8));
}

// Number of frames processed per DCRA chunk.
// This is a tuning constant chosen to keep RAM usage bounded while giving the
// rolling-sum implementation enough work per pass to be efficient.
static const uint32_t kProcessBlockFrames = 2048U;

// -----------------------------------------------------------------------------
// Inference + Dynamic Thresholding
// -----------------------------------------------------------------------------
static inline float clampf(float x, float lo, float hi) {
  // Clamps a floating-point value into the inclusive range [lo, hi].
  // This is used to keep confidence, decay factors, and other tuning values
  // bounded so the controller cannot be driven by invalid or extreme inputs.
  return (x < lo) ? lo : ((x > hi) ? hi : x);
}

static inline float minf(float a, float b) {
  // Returns the smaller of two float values.
  // A small utility used by the threshold controller when computing boundaries
  // and enforcing ordering relationships.
  return (a < b) ? a : b;
}

static inline float maxf(float a, float b) {
  // Returns the larger of two float values.
  // Used throughout the controller to enforce floors, caps, and safe numerical
  // bounds.
  return (a > b) ? a : b;
}

struct WeightedGaussianStats {
  // Maintains a weighted running estimate of a class distribution using
  // exponentially decayed sufficient statistics.
  // Each class tracks:
  // - total effective weight
  // - running mean
  // - second moment accumulator (m2)
  //
  // This is a compact online Gaussian model that is suitable for embedded use.
  // It supports:
  // - decay(): gradually forgets older observations
  // - update(): incorporates a new weighted sample
  // - variance() / stddev(): estimates spread
  // - ready(): indicates whether there is enough data to trust the estimate
  //
  // The implementation is intentionally lightweight and numerically stable for
  // streaming updates on the RP2040.
  float weightSum;
  float mean;
  float m2;

  WeightedGaussianStats()
    : weightSum(0.0f), mean(0.0f), m2(0.0f) {}

  void decay(float decayFactor) {
    if (!isfinite(decayFactor)) return;
    decayFactor = clampf(decayFactor, 0.0f, 1.0f);
    weightSum *= decayFactor;
    m2 *= decayFactor;

    if (weightSum < 1e-12f) {
      weightSum = 0.0f;
      mean = 0.0f;
      m2 = 0.0f;
    }
  }

  void update(float x, float weight = 1.0f) {
    if (!(weight > 0.0f) || !isfinite(x)) return;
    const float newWeight = weightSum + weight;
    const float delta = x - mean;
    const float r = weight / newWeight;
    mean += r * delta;
    m2 += weight * delta * (x - mean);
    weightSum = newWeight;
  }

  float variance() const {
    if (weightSum <= 1e-12f) return 0.0f;
    const float v = m2 / weightSum;
    return (v > 0.0f) ? v : 0.0f;
  }

  float stddev() const {
    return sqrtf(variance());
  }

  bool ready() const {
    return weightSum > 1.0f;
  }
};

static float gaussianIntersection(float mu1, float sigma1, float mu2, float sigma2, float fallback) {
  // Computes the intersection point of two Gaussian density curves.
  // This is used as a principled boundary estimate between adjacent active
  // classes in the dynamic threshold controller.
  //
  // The function handles several cases:
  // - equal or nearly equal sigmas
  // - near-linear degeneracy
  // - no real intersection
  // - choosing a root that lies between the two means when possible
  //
  // The fallback value is used when the quadratic is ill-conditioned or the
  // intersection cannot be determined robustly.

  const float eps = 1e-12f;
  const float s1 = maxf(fabsf(sigma1), eps);
  const float s2 = maxf(fabsf(sigma2), eps);

  if (fabsf(s1 - s2) < 1e-10f) {
    return 0.5f * (mu1 + mu2);
  }

  const float a = 0.5f / (s2 * s2) - 0.5f / (s1 * s1);
  const float b = (mu1 / (s1 * s1)) - (mu2 / (s2 * s2));
  const float c = 0.5f * (mu2 * mu2 / (s2 * s2) - mu1 * mu1 / (s1 * s1)) + logf(s2 / s1);
  const float mid = 0.5f * (mu1 + mu2);

  if (fabsf(a) < eps) {
    if (fabsf(b) < eps) return fallback;
    return -c / b;
  }

  const float disc = b * b - 4.0f * a * c;
  if (disc < 0.0f) return fallback;

  const float rootDisc = sqrtf(disc);
  const float r1 = (-b + rootDisc) / (2.0f * a);
  const float r2 = (-b - rootDisc) / (2.0f * a);

  const float lo = minf(mu1, mu2);
  const float hi = maxf(mu1, mu2);

  bool haveBetween = false;
  float best = fallback;
  float bestDist = 1e30f;

  const float roots[2] = { r1, r2 };
  for (uint8_t i = 0; i < 2; ++i) {
    const float r = roots[i];
    if (r >= lo && r <= hi) {
      const float d = fabsf(r - mid);
      if (!haveBetween || d < bestDist) {
        haveBetween = true;
        best = r;
        bestDist = d;
      }
    }
  }

  if (haveBetween) return best;

  const float d1 = fabsf(r1 - mid);
  const float d2 = fabsf(r2 - mid);
  return (d1 <= d2) ? r1 : r2;
}

class DynamicThresholdController {
  // Online dynamic threshold controller for three classes:
  //   0 = ambient
  //   1 = event
  //   2 = storm
  //
  // The controller:
  // - maintains weighted Gaussian stats per class
  // - ranks active classes by current mean
  // - estimates boundaries between neighboring classes
  // - nudges thresholdLow / thresholdHigh toward those targets
  // - preserves ordering and prevents threshold collapse
  //
  // This is the stateful decision layer that turns frame-level model outputs into
  // adaptive threshold behavior suitable for the onboard pipeline.

public:
  DynamicThresholdController(
    // Constructs the controller with initial thresholds and tuning constants.
    // The constructor also initializes the class ordering state so the first few
    // updates have a sensible baseline.
    //
    // The default parameters control:
    // - confidence clipping
    // - low/high threshold learning rates
    // - minimum threshold gap
    // - sigma floor for Gaussian intersections
    // - exponential decay rate
    // - temporary boost when class order changes
    float initialThresholdLow,
    float initialThresholdHigh,
    float confidenceCap = 0.8f,
    float etaLow = 0.10f,
    float etaHigh = 0.03f,
    float minGap = 1e-6f,
    float sigmaFloor = 1e-6f,
    float decayFactor = 0.995f,
    uint8_t orderBoostSteps = 8,
    float orderBoostFactor = 2.5f)
    : thresholdLow(initialThresholdLow),
      thresholdHigh(initialThresholdHigh),
      confidenceCap(confidenceCap),
      etaLow(etaLow),
      etaHigh(etaHigh),
      minGap(minGap),
      sigmaFloor(sigmaFloor),
      decayFactor(decayFactor),
      orderBoostSteps(orderBoostSteps),
      orderBoostFactor(orderBoostFactor),
      lastOrderValid(false),
      lastOrderedCount(0),
      boostRemaining(0) {
    if (!(thresholdLow < thresholdHigh)) {
      thresholdLow = kDeploymentThresholdFloorCounts;
      thresholdHigh = thresholdLow + minGap;
    }

    for (uint8_t i = 0; i < 3; ++i) {
      lastOrderedLabels[i] = i;
    }
  }

  ThresholdSnapshot observe(uint8_t label, float amplitude, float confidence) {
    // Incorporates one classified frame into the controller state.
    // Input:
    // - label: model class prediction
    // - amplitude: frame-level amplitude statistic
    // - confidence: model confidence for the chosen label
    //
    // Processing:
    // - decays all class statistics
    // - updates the chosen class with the new sample
    // - recomputes active class order
    // - detects order changes and temporarily boosts adaptation
    // - updates thresholdLow / thresholdHigh toward their targets
    // - enforces valid threshold ordering
    //
    // Returns a full ThresholdSnapshot representing the controller state after the
    // update.
    if (label > 2U) {
      return snapshot();
    }

    const float w = clipConfidence(confidence);

    for (uint8_t i = 0; i < 3; ++i) {
      stats[i].decay(decayFactor);
    }

    stats[label].update(amplitude, w);

    uint8_t ordered[3];
    const uint8_t orderedCount = orderedActiveLabels(ordered);

    if (lastOrderValid && !sameOrder(ordered, orderedCount, lastOrderedLabels, lastOrderedCount)) {
      boostRemaining = orderBoostSteps;
    }

    for (uint8_t i = 0; i < orderedCount; ++i) {
      lastOrderedLabels[i] = ordered[i];
    }
    lastOrderedCount = orderedCount;
    lastOrderValid = true;

    const float boost = (boostRemaining > 0) ? orderBoostFactor : 1.0f;
    const float etaLowEff = etaLow * boost;
    const float etaHighEff = etaHigh * boost;

    const float lowTarget = boundaryLow(ordered, orderedCount);
    const float highTarget = boundaryHigh(ordered, orderedCount);

    int8_t rank = -1;
    for (uint8_t i = 0; i < orderedCount; ++i) {
      if (ordered[i] == label) {
        rank = (int8_t)i;
        break;
      }
    }

    if (orderedCount <= 1U || rank < 0) {
      const float currentMid = 0.5f * (thresholdLow + thresholdHigh);
      const float gap = maxf(thresholdHigh - thresholdLow, minGap);
      const float targetMid = stats[label].mean;
      const float newMid = moveToward(currentMid, targetMid, etaLowEff, w);
      thresholdLow = newMid - 0.5f * gap;
      thresholdHigh = newMid + 0.5f * gap;
    } else if (orderedCount == 2U) {
      if (rank == 0) {
        thresholdLow = moveToward(thresholdLow, lowTarget, etaLowEff, w);
      } else {
        thresholdHigh = moveToward(thresholdHigh, highTarget, etaHighEff, w);
      }
    } else {
      if (rank == 0) {
        thresholdLow = moveToward(thresholdLow, lowTarget, etaLowEff, w);
      } else if (rank == 1) {
        thresholdLow = moveToward(thresholdLow, lowTarget, etaLowEff, w);
        thresholdHigh = moveToward(thresholdHigh, highTarget, etaHighEff, w);
      } else {
        thresholdHigh = moveToward(thresholdHigh, highTarget, etaHighEff, w);
      }
    }

    if (boostRemaining > 0) {
      --boostRemaining;
    }

    enforceOrder();
    return snapshot(lowTarget, highTarget, ordered, orderedCount);
  }

  ThresholdSnapshot snapshot() const {
    // Produces a read-only snapshot of the current controller state without
    // incorporating a new observation.
    // This is used both for end-of-run reporting and for cases where no valid frames
    // were processed.
    uint8_t ordered[3];
    const uint8_t orderedCount = orderedActiveLabels(ordered);
    return snapshot(boundaryLow(ordered, orderedCount), boundaryHigh(ordered, orderedCount), ordered, orderedCount);
  }

  float getThresholdLow() const {
    return thresholdLow;
  }
  float getThresholdHigh() const {
    return thresholdHigh;
  }

  void exportState(PersistentDTState& out) const {
    // Serializes the controller state into the persistent DT structure.
    // This captures the threshold values, class statistics, and ordering state so
    // the controller can be restored after a reboot or power loss.
    memset(&out, 0, sizeof(out));
    out.thresholdLow = thresholdLow;
    out.thresholdHigh = thresholdHigh;

    for (uint8_t i = 0; i < 3; ++i) {
      out.mean[i] = stats[i].mean;
      out.m2[i] = stats[i].m2;
      out.weightSum[i] = stats[i].weightSum;
      out.lastOrderedLabels[i] = lastOrderedLabels[i];
    }

    out.lastOrderValid = lastOrderValid ? 1 : 0;
    out.lastOrderedCount = lastOrderedCount;
    out.boostRemaining = boostRemaining;
  }

  bool importState(const PersistentDTState& in) {
    // Restores the controller from a previously saved persistent state.
    // The function verifies the magic/version fields before accepting the state,
    // then reloads the thresholds, statistics, and ordering metadata.
    //
    // Returns true if the state was accepted and loaded successfully.
    if (in.magic != kDtMagic || in.version != kDtVersion) return false;

    thresholdLow = in.thresholdLow;
    thresholdHigh = in.thresholdHigh;

    for (uint8_t i = 0; i < 3; ++i) {
      stats[i].mean = in.mean[i];
      stats[i].m2 = in.m2[i];
      stats[i].weightSum = in.weightSum[i];
      lastOrderedLabels[i] = in.lastOrderedLabels[i];
    }

    lastOrderValid = (in.lastOrderValid != 0);
    lastOrderedCount = in.lastOrderedCount;
    boostRemaining = in.boostRemaining;

    enforceOrder();
    return true;
  }

private:
  // Internal tunable parameters and state:
  // - current thresholds
  // - confidence clamp
  // - learning rates
  // - sigma floor
  // - decay factor
  // - order-change boost parameters
  // - per-class weighted Gaussian statistics
  // - last known active ordering
  //
  // These fields are private because callers should interact through observe(),
  // snapshot(), exportState(), and importState() rather than mutating internals.
  float thresholdLow;
  float thresholdHigh;

  const float confidenceCap;
  const float etaLow;
  const float etaHigh;
  const float minGap;
  const float sigmaFloor;
  const float decayFactor;
  const uint8_t orderBoostSteps;
  const float orderBoostFactor;

  WeightedGaussianStats stats[3];

  bool lastOrderValid;
  uint8_t lastOrderedLabels[3];
  uint8_t lastOrderedCount;
  uint8_t boostRemaining;

  float clipConfidence(float confidence) const {
    // Clamps model confidence into the controller's usable range.
    // This prevents a single extreme value from having outsized influence on the
    // adaptive update.
    return clampf(confidence, 0.0f, confidenceCap);
  }

  float moveToward(float current, float target, float eta, float weight) const {
    // Moves a current value toward a target by a weighted fraction.
    // This is the basic threshold adaptation step used throughout the controller.
    return current + eta * weight * (target - current);
  }

  bool sameOrder(const uint8_t* a, uint8_t ac, const uint8_t* b, uint8_t bc) const {
    // Compares two label-order arrays for equality.
    // Used to detect when the active class ordering has changed, which can trigger
    // a temporary boost in threshold adaptation.
    if (ac != bc) return false;
    for (uint8_t i = 0; i < ac; ++i) {
      if (a[i] != b[i]) return false;
    }
    return true;
  }

  uint8_t orderedActiveLabels(uint8_t out[3]) const {
    // Collects the currently active labels and sorts them by their estimated mean.
    // If no classes are active yet, the controller falls back to the last known
    // ordering so threshold logic remains stable during startup.
    //
    // This function is the heart of the controller's ordering model.
    uint8_t active[3];
    uint8_t count = 0;
    for (uint8_t label = 0; label < 3; ++label) {
      if (stats[label].weightSum > 0.0f) {
        active[count++] = label;
      }
    }

    if (count == 0) {
      for (uint8_t i = 0; i < lastOrderedCount; ++i) {
        out[i] = lastOrderedLabels[i];
      }
      return lastOrderedCount;
    }

    uint8_t prevRank[3] = { 0, 1, 2 };
    for (uint8_t i = 0; i < lastOrderedCount; ++i) {
      prevRank[lastOrderedLabels[i]] = i;
    }

    for (uint8_t i = 0; i < count; ++i) {
      out[i] = active[i];
    }

    for (uint8_t i = 0; i + 1 < count; ++i) {
      for (uint8_t j = i + 1; j < count; ++j) {
        const uint8_t li = out[i];
        const uint8_t lj = out[j];
        const float mi = stats[li].mean;
        const float mj = stats[lj].mean;
        bool swap = false;
        if (mj < mi) {
          swap = true;
        } else if (fabsf(mj - mi) < 1e-9f && prevRank[lj] < prevRank[li]) {
          swap = true;
        }
        if (swap) {
          out[i] = lj;
          out[j] = li;
        }
      }
    }

    return count;
  }

  float pairBoundary(uint8_t a, uint8_t b, float fallback) const {
    // Computes a boundary between two class models.
    // If both classes have sufficient statistics, the function estimates the
    // boundary using the Gaussian intersection.
    // Otherwise it falls back to a midpoint or a class mean, depending on which
    // information is available.
    const WeightedGaussianStats& sa = stats[a];
    const WeightedGaussianStats& sb = stats[b];
    const float lo = minf(sa.mean, sb.mean);
    const float hi = maxf(sa.mean, sb.mean);

    if (sa.weightSum <= 0.0f && sb.weightSum <= 0.0f) return clampf(fallback, lo, hi);
    if (sa.weightSum <= 0.0f) return clampf(sb.mean, lo, hi);
    if (sb.weightSum <= 0.0f) return clampf(sa.mean, lo, hi);

    float boundary = 0.5f * (sa.mean + sb.mean);
    if (sa.ready() && sb.ready()) {
      boundary = gaussianIntersection(
        sa.mean,
        maxf(sa.stddev(), sigmaFloor),
        sb.mean,
        maxf(sb.stddev(), sigmaFloor),
        0.5f * (sa.mean + sb.mean));
    }

    return clampf(boundary, lo, hi);
  }

  float boundaryLow(const uint8_t* ordered, uint8_t orderedCount) const {
    // Returns the current target for the low threshold boundary based on the active
    // class ordering.
    if (orderedCount == 0) return thresholdLow;
    if (orderedCount == 1) return stats[ordered[0]].mean;
    return pairBoundary(ordered[0], ordered[1], thresholdLow);
  }

  float boundaryHigh(const uint8_t* ordered, uint8_t orderedCount) const {
    // Returns the current target for the high threshold boundary based on the
    // active class ordering.
    if (orderedCount == 0) return thresholdHigh;
    if (orderedCount == 1) return stats[ordered[0]].mean;
    if (orderedCount == 2) return pairBoundary(ordered[0], ordered[1], thresholdHigh);
    return pairBoundary(ordered[orderedCount - 2], ordered[orderedCount - 1], thresholdHigh);
  }

  void enforceOrder() {
    // Ensures the thresholds remain ordered and separated by at least minGap.
    // This prevents the adaptive controller from collapsing thresholdLow and
    // thresholdHigh into an invalid or unstable configuration.
    if (thresholdLow >= thresholdHigh) {
      const float mid = 0.5f * (thresholdLow + thresholdHigh);
      thresholdLow = mid - 0.5f * minGap;
      thresholdHigh = mid + 0.5f * minGap;
    }

    if (thresholdLow < kDeploymentThresholdFloorCounts) {
      thresholdLow = kDeploymentThresholdFloorCounts;
    }

    if (thresholdHigh <= thresholdLow) {
      thresholdHigh = thresholdLow + minGap;
    }
  }

  ThresholdSnapshot snapshot(float lowTarget, float highTarget, const uint8_t* ordered, uint8_t orderedCount) const {
    // Builds a complete ThresholdSnapshot containing the thresholds, target
    // boundaries, per-class moments, and current label ordering.
    // This is the structured output used for reporting and persistence.
    ThresholdSnapshot s;
    s.thresholdLow = thresholdLow;
    s.thresholdHigh = thresholdHigh;
    s.lowTarget = lowTarget;
    s.highTarget = highTarget;

    s.muAmbient = stats[0].mean;
    s.stddevAmbient = stats[0].stddev();
    s.weightAmbient = stats[0].weightSum;

    s.muEvent = stats[1].mean;
    s.stddevEvent = stats[1].stddev();
    s.weightEvent = stats[1].weightSum;

    s.muStorm = stats[2].mean;
    s.stddevStorm = stats[2].stddev();
    s.weightStorm = stats[2].weightSum;

    for (uint8_t i = 0; i < 3; ++i) {
      s.orderedLabels[i] = (i < orderedCount) ? ordered[i] : 255;
    }
    s.orderedCount = orderedCount;
    return s;
  }
};

// Global dynamic-threshold controller instance.
// This is the single live state machine used by the pipeline to adapt thresholds
// across frames and across runs when persistent state is loaded.
static DynamicThresholdController controller(kInitialThresholdLowCounts, kInitialThresholdHighCounts);

static uint32_t fnv1a32(const uint8_t* data, size_t len) {
  // Computes a 32-bit FNV-1a hash over a byte buffer.
  // Used for integrity checking of the persistent DT state on disk.
  uint32_t hash = 2166136261UL;
  for (size_t i = 0; i < len; ++i) {
    hash ^= data[i];
    hash *= 16777619UL;
  }
  return hash;
}

static uint32_t dtStateChecksum(PersistentDTState s) {
  // Computes the checksum for a PersistentDTState record.
  // The checksum field is cleared before hashing so the value is stable and can
  // be validated after load.
  s.checksum = 0;
  return fnv1a32(reinterpret_cast<const uint8_t*>(&s), sizeof(s));
}

static bool readDtStateFile(const char* path, PersistentDTState& out) {
  // Reads one persistent DT state file from SD and validates it.
  // Checks performed:
  // - file open succeeds
  // - full record can be read
  // - magic/version are correct
  // - checksum matches
  //
  // Returns true only if the file is a valid saved controller state.
  FsFile f;
  if (!f.open(&gRoot, path, O_RDONLY)) {
    return false;
  }

  const int n = f.read(reinterpret_cast<uint8_t*>(&out), sizeof(out));
  f.close();

  if (n != (int)sizeof(out)) {
    return false;
  }

  if (out.magic != kDtMagic || out.version != kDtVersion) {
    return false;
  }

  const uint32_t expected = out.checksum;
  if (expected != dtStateChecksum(out)) {
    return false;
  }

  return true;
}

static bool loadDtState() {
  // Loads the most recent valid DT state from the two alternating persistence files.
  // If both are valid, the newer sequence number wins.
  // If only one is valid, that one is used.
  // If neither is valid, the controller keeps its defaults.
  PersistentDTState a;
  PersistentDTState b;
  bool haveA = readDtStateFile(kDtStatePathA, a);
  bool haveB = readDtStateFile(kDtStatePathB, b);

  if (!haveA && !haveB) {
    return false;
  }

  const PersistentDTState* chosen = nullptr;
  if (haveA && haveB) {
    chosen = (b.sequence > a.sequence) ? &b : &a;
  } else if (haveA) {
    chosen = &a;
  } else {
    chosen = &b;
  }

  gDtSequence = chosen->sequence;
  return controller.importState(*chosen);
}

static bool saveDtState() {
  // Serializes and saves the current DT controller state to SD.
  // Uses alternating A/B files to reduce corruption risk if power is lost during a write.
  // A checksum is stored so the file can be validated on the next boot.
  PersistentDTState s;
  controller.exportState(s);

  s.magic = kDtMagic;
  s.version = kDtVersion;
  s.reserved = 0;
  s.sequence = gDtSequence + 1U;
  s.checksum = 0;
  s.checksum = dtStateChecksum(s);

  const bool writeToA = ((gDtSequence & 1U) != 0U);
  const char* targetPath = writeToA ? kDtStatePathA : kDtStatePathB;

  FsFile f;
  if (!f.open(&gRoot, targetPath, O_WRONLY | O_CREAT | O_TRUNC)) {
    return false;
  }

  const int written = f.write(reinterpret_cast<const uint8_t*>(&s), sizeof(s));
  if (written != (int)sizeof(s)) {
    f.close();
    return false;
  }

  if (!f.sync()) {
    f.close();
    return false;
  }

  f.close();
  gDtSequence = s.sequence;
  return true;
}

static uint16_t peakMagnitudeCounts(int16_t sample) {
  // Returns the absolute magnitude of a signed PCM sample in centered ADC counts.
  // The INT16_MIN case is handled explicitly so the absolute value cannot overflow.
  if (sample == INT16_MIN) {
    return 32768U;
  }
  return (uint16_t)((sample < 0) ? -sample : sample);
}

static void compressFFT(const float* in, float* out) {
  // Compresses the full FFT power frame into a smaller feature vector using
  // one accumulation pass over the input frame, then a short normalization/log
  // pass over the compact output bins.
  const int ratio = kFrameSamples / kInputSize;
  float total = 0.0f;

  for (int i = 0; i < kInputSize; ++i) {
    out[i] = 0.0f;
  }

  for (int i = 0; i < kFrameSamples; ++i) {
    const float v = in[i];
    total += v;
    out[i / ratio] += v;
  }

  const float denom = total + 1e-6f;
  for (int i = 0; i < kInputSize; ++i) {
    out[i] = logf((out[i] / denom) + 1e-6f);
  }
}

static bool runInferenceOnFrame(const float* input, float frameAmplitude, ThresholdSnapshot& outSnapshot) {
  // Runs the model on one compressed FFT frame and feeds the resulting class
  // decision into the dynamic threshold controller.
  //
  // Workflow:
  // - load model input tensor
  // - run inference
  // - read ambient/storm output probabilities
  // - choose a label using threshold logic
  // - emit the debug tuple
  // - update the controller with label, frame peak amplitude in centered PCM
  //   counts, and confidence
  //
  // Returns true on success.
  for (int i = 0; i < kInputSize; ++i) {
    if (!ModelSetInput(input[i], i)) {
      return false;
    }
  }

  if (!ModelRunInference()) {
    return false;
  }

  const float pAmbient = ModelGetOutput(0);
  const float pStorm = ModelGetOutput(1);

  uint8_t bestLabel = 1;
  float bestConf = (pAmbient > pStorm) ? pAmbient : pStorm;

  if (pAmbient >= kThresholdAmbient) {
    bestLabel = 0;
    bestConf = pAmbient;
  } else if (pStorm >= kThresholdStorm) {
    bestLabel = 2;
    bestConf = pStorm;
  }

  ++gCurrentRunFramesInferred;
  if (bestLabel == 2U) {
    ++gCurrentRunStormFrames;
  }

  printTupleDebug(bestLabel, frameAmplitude, bestConf);
  outSnapshot = controller.observe(bestLabel, frameAmplitude, bestConf);
  return true;
}

static float currentRunStormFrameFraction() {
  const uint32_t totalFrames = gCurrentRunFramesInferred;
  if (totalFrames == 0U) {
    return 0.0f;
  }
  return (float)gCurrentRunStormFrames / (float)totalFrames;
}

static bool currentRunShouldLockdown() {
  if (gCurrentRunFramesInferred == 0U) {
    return false;
  }
  return currentRunStormFrameFraction() >= kStormLockdownFraction;
}

static float chooseSensorThreshold(const ThresholdSnapshot& s) {
  // Always apply the low adaptive threshold to the sensor hook.
  // Storm handling is now represented by lockdown mode at shutdown rather than
  // by selecting the controller's high threshold.
  // NOTE: if upgrading hardware which only wakes the system when amplitude is within
  // a certain range, this function should be modified to return both threholdLow and
  // thresholdHigh. This function is inefficient but left here to support that if chosen later.
  (void)s;
  return s.thresholdLow;
}

static float convertThresholdToG(float thresholdCounts) {
  if (!(thresholdCounts > 0.0f) || !isfinite(thresholdCounts)) {
    return 0.0f;
  }
  // The controller threshold is currently a zero-based peak-amplitude count in the
  // centered PCM domain, so convert it linearly using ADC volts-per-count rather
  // than reinterpreting it as a signed midscale-referenced level.
  const float thresholdVolts = thresholdCounts * kAdcVoltsPerCount;
  return thresholdVolts / kAdxlSensitivityVoltsPerG;
}

static bool applyADXLThreshold(float thresholdCounts) {
  // Convert the controller's zero-based peak-amplitude count threshold into g
  // before programming the ADXL355 and persisting the value.
  const float thresholdG = convertThresholdToG(thresholdCounts);

  if (setADXLRegThreshold(thresholdG)) {
    return false;
  }
  gAdxlWakeThresholdReady = true;

  FsFile logFile;
  if (!logFile.open(&gRoot, kThresholdLogPath, O_WRONLY | O_CREAT | O_TRUNC)) {
    return false;
  }

  char line[24];
  const int len = snprintf(line, sizeof(line), "%.6f\r\n", (double)thresholdG);
  if (len <= 0 || len >= (int)sizeof(line)) {
    logFile.close();
    return false;
  }

  if (logFile.write(reinterpret_cast<const uint8_t*>(line), (size_t)len) != len) {
    logFile.close();
    return false;
  }

  if (!logFile.sync()) {
    logFile.close();
    return false;
  }

  logFile.close();
  return true;
}

// -----------------------------------------------------------------------------
// File discovery and naming
// -----------------------------------------------------------------------------

static bool parseNumericWavStem(const char* name, uint32_t& indexOut) {
  // Validates that a filename has the form "<digits>.wav" and extracts the
  // numeric stem as an unsigned integer.
  // This is used to identify recording files that are numbered sequentially.
  //
  // Rules enforced:
  // - the name must exist
  // - the extension must be .wav (case-insensitive)
  // - every character before the extension must be a decimal digit
  //
  // The function returns false if the filename is malformed or if the numeric
  // stem would overflow the supported 32-bit range.

  if (!name) return false;

  const size_t len = strlen(name);
  if (len < 5U) return false;

  const char* ext = name + (len - 4U);
  const bool extOk =
    (ext[0] == '.') && (tolower((unsigned char)ext[1]) == 'w') && (tolower((unsigned char)ext[2]) == 'a') && (tolower((unsigned char)ext[3]) == 'v');
  if (!extOk) return false;

  uint32_t value = 0;
  for (size_t i = 0; i + 4U < len; ++i) {
    const char c = name[i];
    if (c < '0' || c > '9') return false;
    const uint32_t digit = (uint32_t)(c - '0');
    if (value > 429496729U) return false;
    if (value == 429496729U && digit > 5U) return false;
    value = value * 10U + digit;
  }

  indexOut = value;
  return true;
}

static bool makeIndexedPath(uint32_t index, const char* ext, char* out, size_t outSize) {
  // Builds a root-relative path of the form "/<index>.<ext>".
  // This is used to reconstruct the canonical WAV or DT file path from a numeric
  // recording index.
  // Returns true if the formatted path fit in the destination buffer.

  if (!out || !ext) return false;
  const int n = snprintf(out, outSize, "/%lu.%s", (unsigned long)index, ext);
  return (n > 0 && (size_t)n < outSize);
}

static bool openIndexedRunLogForAppend(FsFile& logFile, uint32_t index) {
  char logPath[32];
  if (!makeIndexedPath(index, "txt", logPath, sizeof(logPath))) {
    return false;
  }

  if (!logFile.open(logPath, O_WRONLY | O_CREAT)) {
    return false;
  }

  const uint32_t size = (uint32_t)logFile.fileSize();
  if (!logFile.seekSet(size)) {
    logFile.close();
    return false;
  }

  if (size > 0U) {
    static const char kSeparator[] = "\r\n";
    if (logFile.write(reinterpret_cast<const uint8_t*>(kSeparator), sizeof(kSeparator) - 1U) != (int)(sizeof(kSeparator) - 1U)) {
      logFile.close();
      return false;
    }
  }

  static const char kOpenOkLine[] = "run_log_open_status=open_ok\r\n";
  if (logFile.write(reinterpret_cast<const uint8_t*>(kOpenOkLine), sizeof(kOpenOkLine) - 1U) != (int)(sizeof(kOpenOkLine) - 1U)) {
    logFile.close();
    return false;
  }

  return true;
}

static bool openCurrentRunLogForAppend(FsFile& logFile) {
  if (gDebug.hasIndex) {
    return openIndexedRunLogForAppend(logFile, gDebug.index);
  }

  uint32_t fallbackIndex = 0;
  char wavPath[32];
  if (!findLatestRecording(fallbackIndex, wavPath, sizeof(wavPath))) {
    return false;
  }

  gDebug.index = fallbackIndex;
  gDebug.hasIndex = true;
  return openIndexedRunLogForAppend(logFile, fallbackIndex);
}

static bool appendLogLine(FsFile& logFile, const char* key, float value) {
  char line[64];
  const int len = snprintf(line, sizeof(line), "%s=%.6f\r\n", key, (double)value);
  if (len <= 0 || len >= (int)sizeof(line)) return false;
  return logFile.write(reinterpret_cast<const uint8_t*>(line), (size_t)len) == len;
}

static bool appendLogLine(FsFile& logFile, const char* key, int value) {
  char line[64];
  const int len = snprintf(line, sizeof(line), "%s=%d\r\n", key, value);
  if (len <= 0 || len >= (int)sizeof(line)) return false;
  return logFile.write(reinterpret_cast<const uint8_t*>(line), (size_t)len) == len;
}

static bool appendLogLine(FsFile& logFile, const char* key, const char* value) {
  if (!value) value = "";
  char line[96];
  const int len = snprintf(line, sizeof(line), "%s=%s\r\n", key, value);
  if (len <= 0 || len >= (int)sizeof(line)) return false;
  return logFile.write(reinterpret_cast<const uint8_t*>(line), (size_t)len) == len;
}

static bool appendCurrentRunErrorLog(uint8_t code) {
  FsFile logFile;
  if (!openCurrentRunLogForAppend(logFile)) return false;

  const bool ok =
    appendLogLine(logFile, "error_code", (int)code) &&
    appendLogLine(logFile, "error_stage", gDebug.stage ? gDebug.stage : "unknown") &&
    appendLogLine(logFile, "storm_frame_fraction", currentRunStormFrameFraction()) &&
    appendLogLine(logFile, "lockdown_mode", currentRunShouldLockdown() ? 1 : 0) &&
    logFile.sync();
  logFile.close();
  return ok;
}

static bool appendCurrentRunSuccessLog(const ThresholdSnapshot& snapshot, float thresholdG, const BenchTimes& bench) {
  FsFile logFile;
  if (!openCurrentRunLogForAppend(logFile)) return false;

  const bool ok =
    appendLogLine(logFile, "threshold_g", thresholdG) &&
    appendLogLine(logFile, "ambient_mean", snapshot.muAmbient) &&
    appendLogLine(logFile, "ambient_stddev", snapshot.stddevAmbient) &&
    appendLogLine(logFile, "event_mean", snapshot.muEvent) &&
    appendLogLine(logFile, "event_stddev", snapshot.stddevEvent) &&
    appendLogLine(logFile, "storm_mean", snapshot.muStorm) &&
    appendLogLine(logFile, "storm_stddev", snapshot.stddevStorm) &&
    appendLogLine(logFile, "storm_frame_fraction", currentRunStormFrameFraction()) &&
    appendLogLine(logFile, "lockdown_mode", currentRunShouldLockdown() ? 1 : 0) &&
    appendLogLine(logFile, "discover_us", (float)bench.discover_us) &&
    appendLogLine(logFile, "dcra_us", (float)bench.dcra_us) &&
    appendLogLine(logFile, "rename_us", (float)bench.rename_us) &&
    appendLogLine(logFile, "stream_us", (float)bench.stream_us) &&
    appendLogLine(logFile, "tail_us", (float)bench.tail_us) &&
    appendLogLine(logFile, "worker_us", (float)bench.worker_us) &&
    appendLogLine(logFile, "dt_save_us", (float)bench.dt_save_us) &&
    appendLogLine(logFile, "sensor_us", (float)bench.sensor_us) &&
    appendLogLine(logFile, "total_us", (float)bench.total_us) &&
    logFile.sync();
  logFile.close();
  return ok;
}

static bool appendCurrentRunIridiumLog(const char* status, int initErr, int signalErr, int signalQuality, int sendErr) {
  FsFile logFile;
  if (!openCurrentRunLogForAppend(logFile)) return false;

  const bool ok =
    appendLogLine(logFile, "iridium_status", status) && appendLogLine(logFile, "iridium_init_err", initErr) && appendLogLine(logFile, "iridium_signal_err", signalErr) && appendLogLine(logFile, "iridium_signal_quality", signalQuality) && appendLogLine(logFile, "iridium_send_err", sendErr) && logFile.sync();
  logFile.close();
  return ok;
}

static bool appendCurrentRunStatusLog(const char* key, const char* value) {
  FsFile logFile;
  if (!openCurrentRunLogForAppend(logFile)) return false;

  const bool ok = appendLogLine(logFile, key, value) && logFile.sync();
  logFile.close();
  return ok;
}

static bool appendCurrentRunGnssLog() {
  FsFile logFile;
  if (!openCurrentRunLogForAppend(logFile)) return false;

  bool ok = true;

  ok = ok && appendLogLine(logFile, "gnss_module_detected", gGnssModuleDetected ? 1 : 0);
  ok = ok && appendLogLine(logFile, "gnss_datetime_ready", hasGnssDateTimeFix() ? 1 : 0);
  ok = ok && appendLogLine(logFile, "gnss_location_valid", GNSS.location.isValid() ? 1 : 0);
  ok = ok && appendLogLine(logFile, "gnss_altitude_valid", GNSS.altitude.isValid() ? 1 : 0);
  ok = ok && appendLogLine(logFile, "gnss_speed_valid", GNSS.speed.isValid() ? 1 : 0);

  if (GNSS.date.isValid()) {
    char dateBuf[16];
    const int len = snprintf(
      dateBuf,
      sizeof(dateBuf),
      "%04d-%02d-%02d",
      GNSS.date.year(),
      GNSS.date.month(),
      GNSS.date.day());
    if (len > 0 && len < (int)sizeof(dateBuf)) {
      ok = ok && appendLogLine(logFile, "gnss_date_utc", dateBuf);
    } else {
      ok = false;
    }
  }

  if (GNSS.time.isValid()) {
    char timeBuf[16];
    const int len = snprintf(
      timeBuf,
      sizeof(timeBuf),
      "%02d:%02d:%02d",
      GNSS.time.hour(),
      GNSS.time.minute(),
      GNSS.time.second());
    if (len > 0 && len < (int)sizeof(timeBuf)) {
      ok = ok && appendLogLine(logFile, "gnss_time_utc", timeBuf);
    } else {
      ok = false;
    }
  }

  if (GNSS.location.isValid()) {
    ok = ok && appendLogLine(logFile, "gnss_lat", (float)GNSS.location.lat());
    ok = ok && appendLogLine(logFile, "gnss_lon", (float)GNSS.location.lng());
  }

  if (GNSS.altitude.isValid()) {
    ok = ok && appendLogLine(logFile, "gnss_altitude_m", (float)GNSS.altitude.meters());
  }

  if (GNSS.speed.isValid()) {
    ok = ok && appendLogLine(logFile, "gnss_speed_mps", (float)GNSS.speed.mps());
  }

  if (GNSS.satellites.isValid()) {
    ok = ok && appendLogLine(logFile, "gnss_satellites", (int)GNSS.satellites.value());
  }

  ok = ok && logFile.sync();
  logFile.close();
  return ok;
}

static bool appendCurrentRunTotalRuntimeLog() {
  if (gProgramTotalRuntimeLogged) return true;

  FsFile logFile;
  if (!openCurrentRunLogForAppend(logFile)) return false;

  char line[64];
  const unsigned long totalRuntimeMs = (unsigned long)(millis() - gSetupStartMs);
  const int len = snprintf(line, sizeof(line), "program_total_ms=%lu\r\n", totalRuntimeMs);
  if (len <= 0 || len >= (int)sizeof(line)) {
    logFile.close();
    return false;
  }

  const bool ok =
    (logFile.write(reinterpret_cast<const uint8_t*>(line), (size_t)len) == len) &&
    logFile.sync();
  logFile.close();
  if (ok) {
    gProgramTotalRuntimeLogged = true;
  }
  return ok;
}

static bool findLatestRecording(uint32_t& indexOut, char* wavPath, size_t wavPathSize) {
  // Scans the root directory and finds the numerically largest recording file
  // matching the "<digits>.wav" naming scheme.
  // This is the input discovery step for the pipeline: it selects the recording
  // with index (maxIndex - 1) rather than the largest WAV present in the SD root.
  //
  // The function:
  // - opens the root directory
  // - iterates over entries
  // - ignores subdirectories
  // - tests each filename with parseNumericWavStem()
  // - keeps the largest numeric stem found
  // - subtracts one from that largest index
  // - reconstructs the final path using makeIndexedPath()
  //
  // Returns true only if at least one valid recording file is found and the
  // derived (maxIndex - 1) target is still a positive root-level WAV index.
  // Note: the function accepts any digits before ".wav", so "0001.wav" and "1.wav"
  // map to the same numeric index.

  FsFile dir;
  FsFile entry;
  char nameBuf[64];

  if (!dir.open("/", O_RDONLY)) {
    return false;
  }

  bool found = false;
  uint32_t bestIndex = 0;

  while (entry.openNext(&dir, O_RDONLY)) {
    if (!entry.isDir()) {
      entry.getName(nameBuf, sizeof(nameBuf));
      uint32_t idx = 0;
      if (parseNumericWavStem(nameBuf, idx)) {
        if (!found || idx > bestIndex) {
          bestIndex = idx;
          found = true;
        }
      }
    }
    entry.close();
  }

  dir.close();

  if (!found || bestIndex == 0U) return false;

  const uint32_t selectedIndex = bestIndex - 1U;
  if (selectedIndex == 0U) return false;

  if (!makeIndexedPath(selectedIndex, "wav", wavPath, wavPathSize)) return false;
  indexOut = selectedIndex;
  return true;
}


// -----------------------------------------------------------------------------
// Pipeline orchestration
// -----------------------------------------------------------------------------

static constexpr uint8_t kDcraQueueSlots = 6U;
static constexpr uint8_t kFftQueueSlots = 6U;
static constexpr uint32_t kCore1QueueTimeoutUs = 15000000U;

struct DcraStreamSlot {
  volatile uint32_t ready;
  uint32_t bytes;
  alignas(16) uint8_t data[kDcraOutMaxBytes];
};

struct FftStreamSlot {
  volatile uint32_t ready;
  uint32_t bytes;
  alignas(16) uint8_t data[sizeof(powerFrame)];
};

static DcraStreamSlot gDcraQueue[kDcraQueueSlots];
static FftStreamSlot gFftQueue[kFftQueueSlots];
static volatile uint8_t gDcraWriteIndex = 0;
static volatile uint8_t gDcraReadIndex = 0;
static volatile uint8_t gFftWriteIndex = 0;
static volatile uint8_t gFftReadIndex = 0;

static volatile bool gDualCoreActive = false;
static volatile bool gDcraFinished = false;
static volatile bool gCore1Finished = false;
static volatile bool gCore1Ok = false;
static volatile uint32_t gCore1WorkerUs = 0;
static volatile uint32_t gCore1HeartbeatUs = 0;
static uint32_t gStreamDrainUs = 0;
static FsFile* gBinOutFile = nullptr;
static ThresholdSnapshot gCore1FinalSnapshot;

struct StreamingFftState {
  size_t sampleIndex;
  bool haveCarry;
  uint8_t carryByte;
  bool anyFrame;
  uint16_t framePeakCounts;
};

static inline void memoryBarrier() {
  __sync_synchronize();
}

static inline void noteCore1Heartbeat() {
  gCore1HeartbeatUs = micros();
}

static bool waitForCore1Progress(uint32_t& lastProgressUs, uint32_t& lastHeartbeatUs, uint32_t timeoutUs) {
  const uint32_t now = micros();
  const uint32_t heartbeat = gCore1HeartbeatUs;
  if (heartbeat != lastHeartbeatUs) {
    lastHeartbeatUs = heartbeat;
    lastProgressUs = now;
    return true;
  }
  return (uint32_t)(now - lastProgressUs) <= timeoutUs;
}

static bool drainQueuedFftFrames(FsFile& fftOut, bool waitForMore);

static void resetStreamQueues() {
  for (uint8_t i = 0; i < kDcraQueueSlots; ++i) {
    gDcraQueue[i].ready = 0;
    gDcraQueue[i].bytes = 0;
  }
  for (uint8_t i = 0; i < kFftQueueSlots; ++i) {
    gFftQueue[i].ready = 0;
    gFftQueue[i].bytes = 0;
  }
  gDcraWriteIndex = 0;
  gDcraReadIndex = 0;
  gFftWriteIndex = 0;
  gFftReadIndex = 0;
}

static bool enqueueDcraBlock(const uint8_t* data, uint32_t bytes) {
  const uint8_t slotIndex = gDcraWriteIndex;
  DcraStreamSlot& slot = gDcraQueue[slotIndex];
  uint32_t lastProgressUs = micros();
  uint32_t lastHeartbeatUs = gCore1HeartbeatUs;

  while (slot.ready != 0U) {
    if (gBinOutFile != nullptr) {
      const uint32_t tDrain0 = micros();
      if (!drainQueuedFftFrames(*gBinOutFile, false)) {
        return false;
      }
      gStreamDrainUs += elapsedMicros(tDrain0);
    }
    if (!gDualCoreActive) return false;
    if (!waitForCore1Progress(lastProgressUs, lastHeartbeatUs, kCore1QueueTimeoutUs)) {
      gDebug.stage = "wait_dcra_slot_timeout";
      FAIL(ERR_CORE1_TIMEOUT, "Timed out waiting for DCRA queue slot");
      return false;
    }
    yield();
  }

  if (bytes > kDcraOutMaxBytes) {
    return false;
  }

  memcpy(slot.data, data, bytes);
  slot.bytes = bytes;
  memoryBarrier();
  slot.ready = 1U;
  gDcraWriteIndex = (uint8_t)((slotIndex + 1U) % kDcraQueueSlots);
  return true;
}

static bool dequeueDcraBlock(const uint8_t*& data, uint32_t& bytes, uint8_t& slotIndexOut) {
  const uint8_t slotIndex = gDcraReadIndex;
  DcraStreamSlot& slot = gDcraQueue[slotIndex];

  if (slot.ready == 0U) {
    return false;
  }

  memoryBarrier();
  data = slot.data;
  bytes = slot.bytes;
  slotIndexOut = slotIndex;
  return true;
}

static void releaseDcraBlock(uint8_t slotIndex) {
  DcraStreamSlot& slot = gDcraQueue[slotIndex];
  slot.ready = 0U;
  gDcraReadIndex = (uint8_t)((slotIndex + 1U) % kDcraQueueSlots);
}

static bool enqueueFftFrame(const uint8_t* data, uint32_t bytes) {
  const uint8_t slotIndex = gFftWriteIndex;
  FftStreamSlot& slot = gFftQueue[slotIndex];
  uint32_t lastProgressUs = micros();
  uint32_t lastHeartbeatUs = gCore1HeartbeatUs;

  while (slot.ready != 0U) {
    if (!gDualCoreActive) return false;
    if (!waitForCore1Progress(lastProgressUs, lastHeartbeatUs, kCore1QueueTimeoutUs)) {
      gDebug.stage = "wait_fft_slot_timeout";
      FAIL(ERR_CORE1_TIMEOUT, "Timed out waiting for FFT queue slot");
      return false;
    }
    yield();
  }

  if (bytes > sizeof(slot.data)) {
    return false;
  }

  memcpy(slot.data, data, bytes);
  slot.bytes = bytes;
  memoryBarrier();
  slot.ready = 1U;
  gFftWriteIndex = (uint8_t)((slotIndex + 1U) % kFftQueueSlots);
  return true;
}

static bool drainQueuedFftFrames(FsFile& fftOut, bool waitForMore) {
  uint32_t lastProgressUs = micros();
  uint32_t lastHeartbeatUs = gCore1HeartbeatUs;

  for (;;) {
    const uint8_t slotIndex = gFftReadIndex;
    FftStreamSlot& slot = gFftQueue[slotIndex];

    if (slot.ready == 0U) {
      if (!waitForMore || gCore1Finished) {
        return true;
      }
      if (!waitForCore1Progress(lastProgressUs, lastHeartbeatUs, kCore1QueueTimeoutUs)) {
        gDebug.stage = "wait_fft_drain_timeout";
        FAIL(ERR_CORE1_TIMEOUT, "Timed out waiting for final FFT drain");
        return false;
      }
      yield();
      continue;
    }

    memoryBarrier();
    const uint32_t bytes = slot.bytes;
    if (fftOut.write(slot.data, bytes) != (int)bytes) {
      return false;
    }
    slot.ready = 0U;
    gFftReadIndex = (uint8_t)((slotIndex + 1U) % kFftQueueSlots);
    lastProgressUs = micros();
    lastHeartbeatUs = gCore1HeartbeatUs;
  }
}

static bool finalizeQueuedFftDrain(FsFile& fftOut) {
  const uint32_t t0 = micros();

  if (kDebugPipeline && Serial) {
    Serial.println(F("[run] finalizing FFT tail"));
  }

  if (!drainQueuedFftFrames(fftOut, true)) {
    return false;
  }

  if (!fftOut.sync()) {
    return false;
  }

  gBench.tail_us = elapsedMicros(t0);
  return true;
}

static void computeFFTFrameToBuffer() {
  Fast4::FFT(fftVals, kFftBins);

  size_t i = 0;
  for (; i + 3U < kFftOutBins; i += 4U) {
    const float r0 = fftVals[i + 0].re();
    const float i0 = fftVals[i + 0].im();
    const float r1 = fftVals[i + 1].re();
    const float i1 = fftVals[i + 1].im();
    const float r2 = fftVals[i + 2].re();
    const float i2 = fftVals[i + 2].im();
    const float r3 = fftVals[i + 3].re();
    const float i3 = fftVals[i + 3].im();

    powerFrame[i + 0] = r0 * r0 + i0 * i0;
    powerFrame[i + 1] = r1 * r1 + i1 * i1;
    powerFrame[i + 2] = r2 * r2 + i2 * i2;
    powerFrame[i + 3] = r3 * r3 + i3 * i3;
  }
  for (; i < kFftOutBins; ++i) {
    const float r = fftVals[i].re();
    const float im = fftVals[i].im();
    powerFrame[i] = r * r + im * im;
  }
}

static bool finalizeStreamingFftFrame(ThresholdSnapshot& snapshotOut, float framePeakCounts) {
  ++gDebug.frameIndex;
  computeFFTFrameToBuffer();

  compressFFT(powerFrame, fftInput);
  if (!runInferenceOnFrame(fftInput, framePeakCounts, snapshotOut)) {
    return false;
  }

  if (!enqueueFftFrame(reinterpret_cast<const uint8_t*>(powerFrame), (uint32_t)sizeof(powerFrame))) {
    return false;
  }
  return true;
}

static bool feedStreamingBytes(const uint8_t* src, uint32_t bytes, void* statePtr, ThresholdSnapshot& snapshotOut) {
  StreamingFftState& state = *reinterpret_cast<StreamingFftState*>(statePtr);
  size_t pos = 0;

  if (state.haveCarry) {
    if (bytes == 0U) {
      return true;
    }

    const int16_t rv = (int16_t)((uint16_t)state.carryByte | ((uint16_t)src[0] << 8));
    const uint16_t samplePeak = peakMagnitudeCounts(rv);
    if (samplePeak > state.framePeakCounts) {
      state.framePeakCounts = samplePeak;
    }
    fftVals[state.sampleIndex++] = (complex)((float)rv * (1.0f / 32767.0f));
    pos = 1;
    state.haveCarry = false;

    if (state.sampleIndex >= kFftBins) {
      if (!finalizeStreamingFftFrame(snapshotOut, (float)state.framePeakCounts)) {
        return false;
      }
      state.sampleIndex = 0;
      state.framePeakCounts = 0U;
      state.anyFrame = true;
    }
  }

  const size_t usableEnd = (size_t)bytes & ~(size_t)1;
  for (; pos < usableEnd; pos += 2U) {
    const int16_t rv = (int16_t)((uint16_t)src[pos + 0] | ((uint16_t)src[pos + 1] << 8));
    const uint16_t samplePeak = peakMagnitudeCounts(rv);
    if (samplePeak > state.framePeakCounts) {
      state.framePeakCounts = samplePeak;
    }
    fftVals[state.sampleIndex++] = (complex)((float)rv * (1.0f / 32767.0f));

    if (state.sampleIndex >= kFftBins) {
      if (!finalizeStreamingFftFrame(snapshotOut, (float)state.framePeakCounts)) {
        return false;
      }
      state.sampleIndex = 0;
      state.framePeakCounts = 0U;
      state.anyFrame = true;
    }
  }

  if (((size_t)bytes & 1U) != 0U) {
    state.carryByte = src[bytes - 1];
    state.haveCarry = true;
  }

  return true;
}

static void core1Worker() {
  while (!gDualCoreActive) {
    yield();
  }
  noteCore1Heartbeat();

  StreamingFftState state;
  state.sampleIndex = 0;
  state.haveCarry = false;
  state.carryByte = 0;
  state.anyFrame = false;
  state.framePeakCounts = 0U;

  ThresholdSnapshot snapshot = controller.snapshot();
  const uint32_t t0 = micros();

  for (;;) {
    const uint8_t* data = nullptr;
    uint32_t bytes = 0;
    uint8_t slotIndex = 0;

    if (dequeueDcraBlock(data, bytes, slotIndex)) {
      noteCore1Heartbeat();
      const bool ok = feedStreamingBytes(data, bytes, &state, snapshot);
      releaseDcraBlock(slotIndex);
      if (!ok) {
        gCore1Ok = false;
        gCore1Finished = true;
        return;
      }
      continue;
    }

    if (gDcraFinished) {
      break;
    }

    noteCore1Heartbeat();
    yield();
  }

  if (state.haveCarry) {
    gCore1Ok = false;
    gCore1Finished = true;
    return;
  }

  if (state.sampleIndex > 0U) {
    for (size_t i = state.sampleIndex; i < kFftBins; ++i) {
      fftVals[i] = (complex)0.0f;
    }
    if (!finalizeStreamingFftFrame(snapshot, (float)state.framePeakCounts)) {
      gCore1Ok = false;
      gCore1Finished = true;
      return;
    }
    state.anyFrame = true;
  }

  noteCore1Heartbeat();
  gCore1FinalSnapshot = snapshot;
  gCore1WorkerUs = elapsedMicros(t0);
  gCore1Ok = state.anyFrame;
  gCore1Finished = true;
}

static bool applyDcraToWavStreamedAndQueue(const WavInfo& info, FsFile& inFile, FsFile& outFile, float biasWindowSec) {
  gDebug.stage = "dcra_stream";
  gDebug.sampleRate = info.sampleRate;
  gDebug.dataSize = info.dataSize;
  if (info.channels != 1U || info.bitsPerSample != 16U || info.blockAlign != 2U) {
    return false;
  }
  if (info.dataSize == 0U || info.dataStart == 0U) {
    return false;
  }

  const uint32_t totalFrames = info.dataSize / info.blockAlign;
  if (totalFrames == 0U) {
    return false;
  }

  uint32_t windowFrames = (uint32_t)lroundf(biasWindowSec * (float)info.sampleRate);
  if (windowFrames < 1U) windowFrames = 1U;
  if ((windowFrames & 1U) == 0U) ++windowFrames;

  if (windowFrames > totalFrames) {
    windowFrames = (totalFrames & 1U) ? totalFrames : (totalFrames > 1U ? totalFrames - 1U : 1U);
  }
  if ((windowFrames & 1U) == 0U) {
    windowFrames = (windowFrames > 1U) ? (windowFrames - 1U) : 1U;
  }

  const uint32_t half = windowFrames / 2U;
  const float invWindow = 1.0f / (float)windowFrames;

  if (!copyHeaderToOutput(inFile, outFile, info.dataStart)) return false;
  if (!outFile.seekSet(info.dataStart)) return false;

  uint32_t blockFrames = kProcessBlockFrames;
  if (blockFrames > totalFrames) blockFrames = totalFrames;

  const uint32_t maxReadFrames = (blockFrames + 2U * half > totalFrames)
                                   ? totalFrames
                                   : (blockFrames + 2U * half);

  const uint32_t inBytes = (uint32_t)((uint64_t)maxReadFrames * info.blockAlign);
  const uint32_t outBytes = (uint32_t)((uint64_t)blockFrames * info.blockAlign);
  if (inBytes > sizeof(dcraInScratch) || outBytes > sizeof(dcraOutScratch)) {
    return false;
  }

  uint8_t* inBuf = dcraInScratch;
  uint8_t* outBuf = dcraOutScratch;
  uint32_t clippedSampleCount = 0U;

  for (uint32_t outStart = 0; outStart < totalFrames; outStart += blockFrames) {
    gDebug.blockIndex = outStart / blockFrames;
    uint32_t thisBlockFrames = blockFrames;
    if (thisBlockFrames > totalFrames - outStart) {
      thisBlockFrames = totalFrames - outStart;
    }

    const uint32_t readStart = (outStart > half) ? (outStart - half) : 0U;
    const uint32_t readCount = (outStart + thisBlockFrames + half > totalFrames)
                                 ? (totalFrames - readStart)
                                 : (outStart + thisBlockFrames + half - readStart);

    if (readCount == 0U || readCount > maxReadFrames) {
      return false;
    }

    if (!inFile.seekSet(info.dataStart + (uint32_t)((uint64_t)readStart * info.blockAlign))) {
      return false;
    }

    if (!readExactBytes(inFile, inBuf, (uint32_t)((uint64_t)readCount * info.blockAlign))) {
      return false;
    }

    memset(outBuf, 0, (size_t)thisBlockFrames * info.blockAlign);

    const int64_t vStart = (int64_t)outStart - (int64_t)half;
    int64_t sum = 0;

    for (uint32_t w = 0; w < windowFrames; ++w) {
      const int64_t v = vStart + (int64_t)w;
      const uint32_t actual = reflectIndex(v, totalFrames);
      sum += (int64_t)readPcm16AtFrame(inBuf, actual - readStart, info.blockAlign);
    }

    for (uint32_t i = 0; i < thisBlockFrames; ++i) {
      const int64_t centerV = vStart + (int64_t)i + (int64_t)half;
      const uint32_t centerActual = reflectIndex(centerV, totalFrames);
      const int16_t center = readPcm16AtFrame(inBuf, centerActual - readStart, info.blockAlign);

      const float dcraCorrected = ((float)center - ((float)sum * invWindow)) * kInputAmplitudeScale;
      const int32_t corrected = (int32_t)lroundf(dcraCorrected);
      if (corrected > 32767 || corrected < -32768) {
        ++clippedSampleCount;
      }
      writePcm16AtFrame(outBuf, i, info.blockAlign, corrected);

      if (i + 1U < thisBlockFrames) {
        const int64_t leavingV = vStart + (int64_t)i;
        const int64_t enteringV = vStart + (int64_t)i + (int64_t)windowFrames;

        const uint32_t leavingActual = reflectIndex(leavingV, totalFrames);
        const uint32_t enteringActual = reflectIndex(enteringV, totalFrames);

        const int16_t leaving = readPcm16AtFrame(inBuf, leavingActual - readStart, info.blockAlign);
        const int16_t entering = readPcm16AtFrame(inBuf, enteringActual - readStart, info.blockAlign);

        sum += (int64_t)entering - (int64_t)leaving;
      }
    }

    if (!writeExactBytes(outFile, outBuf, (uint32_t)((uint64_t)thisBlockFrames * info.blockAlign))) {
      return false;
    }

    if (!enqueueDcraBlock(outBuf, (uint32_t)((uint64_t)thisBlockFrames * info.blockAlign))) {
      return false;
    }

    if (kDebugPipeline && Serial && ((outStart / blockFrames) % 256U == 0U)) {
      Serial.print(F("[dcra] block "));
      Serial.print((unsigned long)(outStart / blockFrames));
      Serial.print(F(" / "));
      Serial.println((unsigned long)((totalFrames + blockFrames - 1U) / blockFrames));
    }

    if (gBinOutFile != nullptr) {
      const uint32_t tDrain0 = micros();
      if (!drainQueuedFftFrames(*gBinOutFile, false)) {
        return false;
      }
      gStreamDrainUs += elapsedMicros(tDrain0);
    }
  }

  if (kDebugPipeline && Serial) {
    Serial.print(F("[dcra] clipped_samples="));
    Serial.println((unsigned long)clippedSampleCount);
  }

  return patchOutputSizes(outFile, info.dataStart, info.dataSize);
}

static bool runPipelineOnce(float* appliedThresholdGOut = nullptr) {
  const uint32_t tTotal0 = micros();
  gBench = BenchTimes();
  gDebug = DebugState();
  gFatalFailure = false;
  gFatalCode = 0;
  gDebug.stage = "pipeline_start";
  gCurrentRunFramesInferred = 0;
  gCurrentRunStormFrames = 0;

  resetStreamQueues();
  gDualCoreActive = false;
  gDcraFinished = false;
  gCore1Finished = false;
  gCore1Ok = false;
  gCore1WorkerUs = 0;
  gCore1HeartbeatUs = micros();
  gStreamDrainUs = 0;
  gBinOutFile = nullptr;
  gCore1FinalSnapshot = controller.snapshot();

  uint32_t index = 0;
  char wavPath[32];
  char binPath[32];

  uint32_t t0 = micros();
  gDebug.stage = "discover_recording";
  if (!findLatestRecording(index, wavPath, sizeof(wavPath))) {
    FAIL(ERR_NO_RECORDING, "No numbered WAV recording found");
    return false;
  }
  gBench.discover_us = elapsedMicros(t0);
  gDebug.index = index;
  gDebug.hasIndex = true;

  gDebug.stage = "make_bin_path";
  if (!makeIndexedPath(index, "bin", binPath, sizeof(binPath))) {
    FAIL(ERR_BIN_PATH, "Failed to build BIN output path");
    return false;
  }

  setDebugPaths(wavPath, binPath);

  FsFile inFile;
  FsFile stageFile;
  FsFile binOut;
  WavInfo wavInfo;

  gDebug.stage = "open_input";
  if (!inFile.open(&gRoot, wavPath, O_RDONLY)) {
    FAIL(ERR_OPEN_INPUT, "Failed to open input WAV");
    return false;
  }
  gDebug.stage = "parse_wav";
  if (!parseWavHeader(inFile, wavInfo)) {
    inFile.close();
    FAIL(ERR_PARSE_WAV, "Failed to parse WAV header");
    return false;
  }
  gDebug.sampleRate = wavInfo.sampleRate;
  gDebug.dataSize = wavInfo.dataSize;
  gDebug.stage = "open_stage";
  if (!stageFile.open(&gRoot, kStageWavPath, O_WRONLY | O_CREAT | O_TRUNC)) {
    inFile.close();
    FAIL(ERR_OPEN_STAGE, "Failed to open staged WAV output");
    return false;
  }
  gDebug.stage = "open_bin";
  if (!binOut.open(&gRoot, binPath, O_WRONLY | O_CREAT | O_TRUNC)) {
    stageFile.close();
    inFile.close();
    FAIL(ERR_OPEN_BIN, "Failed to open BIN output");
    return false;
  }

  if (kDebugPipeline && Serial) {
    Serial.println(F("[run] opened input + outputs"));
    Serial.print(F("[run] wavPath="));
    Serial.println(wavPath);
    Serial.print(F("[run] binPath="));
    Serial.println(binPath);
    Serial.print(F("[run] dataSize="));
    Serial.println(wavInfo.dataSize);
    Serial.print(F("[run] sampleRate="));
    Serial.println(wavInfo.sampleRate);
  }

  gDebug.stage = "preallocate";
  if (!preallocateOutputFiles(stageFile, binOut, wavInfo)) {
    stageFile.close();
    binOut.close();
    inFile.close();
    FAIL(ERR_PREALLOC, "Failed to preallocate output files");
    return false;
  }

  gBinOutFile = &binOut;
  gDualCoreActive = true;

  t0 = micros();
  gDebug.stage = "dcra";
  if (!applyDcraToWavStreamedAndQueue(wavInfo, inFile, stageFile, kDcraBiasWindowSec)) {
    gDualCoreActive = false;
    gDcraFinished = true;
    gBinOutFile = nullptr;
    stageFile.close();
    binOut.close();
    inFile.close();
    FAIL(ERR_DCRA, "DCRA streaming stage failed");
    return false;
  }
  gBench.dcra_us = elapsedMicros(t0);

  gDcraFinished = true;

  t0 = micros();
  inFile.close();
  stageFile.close();
  gDebug.stage = "rename_original_to_backup";
  SD.remove(kStageBackupWavPath);
  if (!SD.rename(wavPath, kStageBackupWavPath)) {
    gDualCoreActive = false;
    gBinOutFile = nullptr;
    binOut.close();
    FAIL(ERR_RENAME, "Failed to rename original WAV to backup");
    return false;
  }
  gDebug.stage = "rename_stage_to_live";
  if (!SD.rename(kStageWavPath, wavPath)) {
    SD.rename(kStageBackupWavPath, wavPath);
    gDualCoreActive = false;
    gBinOutFile = nullptr;
    binOut.close();
    FAIL(ERR_RENAME, "Failed to replace original WAV with staged WAV");
    return false;
  }
  SD.remove(kStageBackupWavPath);
  gBench.rename_us = elapsedMicros(t0);

  gBench.stream_us = gStreamDrainUs;
  gDebug.stage = "finalize_fft_drain";
  if (!finalizeQueuedFftDrain(binOut)) {
    gDualCoreActive = false;
    gBinOutFile = nullptr;
    binOut.close();
    FAIL(ERR_FFT_DRAIN, "Failed to finalize queued FFT output");
    return false;
  }

  binOut.close();
  gBinOutFile = nullptr;
  gDualCoreActive = false;

  gDebug.stage = "verify_core1";
  if (!gCore1Finished || !gCore1Ok) {
    // The worker must have completed the frame stream successfully.
    // If it did not, the pipeline cannot safely continue.
    FAIL(ERR_CORE1, "Core1 worker did not finish successfully");
    return false;
  }
  if (kDebugPipeline && Serial) {
    Serial.println(F("[run] core1 verified"));
  }

  const ThresholdSnapshot finalSnapshot = gCore1FinalSnapshot;

  t0 = micros();
  gDebug.stage = "save_dt_state";
  if (!saveDtState()) {
    FAIL(ERR_DT_SAVE, "Failed to save dynamic threshold state");
    return false;
  }
  gBench.dt_save_us = elapsedMicros(t0);
  if (kDebugPipeline && Serial) {
    Serial.println(F("[run] DT state saved"));
  }

  t0 = micros();
  gDebug.stage = "apply_sensor_threshold";
  const float sensorThreshold = chooseSensorThreshold(finalSnapshot);
  const float thresholdG = convertThresholdToG(sensorThreshold);
  if (!applyADXLThreshold(sensorThreshold)) {
    FAIL(ERR_SENSOR_WRITE, "Failed to apply sensor threshold");
    return false;
  }
  gBench.sensor_us = elapsedMicros(t0);
  if (kDebugPipeline && Serial) {
    Serial.println(F("[run] ADXL threshold applied"));
  }
  if (appliedThresholdGOut) {
    *appliedThresholdGOut = thresholdG;
  }

  gBench.worker_us = gCore1WorkerUs;
  gBench.total_us = elapsedMicros(tTotal0);

  gDebug.stage = "append_run_log";
  if (!appendCurrentRunSuccessLog(finalSnapshot, thresholdG, gBench)) {
    if (kTreatRunLogFailureAsNonfatalForDebug) {
      if (kDebugPipeline && Serial) {
        Serial.println(F("[run] run log append failed; continuing due to debug override"));
      }
    } else {
      FAIL(ERR_RUN_LOG, "Failed to append run success log");
      return false;
    }
  }
  if (kDebugPipeline && Serial) {
    Serial.println(F("[run] run log appended"));
  }

  if (kDebugPrintDtState && Serial) {
    Serial.println();
    Serial.println(F("=== end-of-run debug dump ==="));
    printThresholdSnapshot(finalSnapshot);

    PersistentDTState debugState;
    controller.exportState(debugState);
    debugState.magic = kDtMagic;
    debugState.version = kDtVersion;
    debugState.reserved = 0;
    debugState.sequence = gDtSequence + 1U;
    debugState.checksum = 0;
    debugState.checksum = dtStateChecksum(debugState);
    printPersistentDtState(debugState, debugState.sequence);

    Serial.print(F("sensorThreshold_counts_applied: "));
    Serial.println(sensorThreshold, 6);
  }

  return true;
}

void setup() {
  gSetupStartMs = millis();
  gProgramTotalRuntimeLogged = false;

  // Immediately hold the power on to the Pico and the SD card.
  pinMode(KILL_PICO_PIN, OUTPUT);
  digitalWrite(KILL_PICO_PIN, HIGH);
  pinMode(KILL_SD_PIN, OUTPUT);
  digitalWrite(KILL_SD_PIN, HIGH);

  // After the most critical first step, start timekeeping to use for GNSS back-timestamping.
  startMillis = millis();
  // This pin is no longer named well because I repurposed it. This tells the
  // SAMD that the Pico is still working if the SAMD were to be woken from
  // sleep, aborting the recording that it would otherwise immediately start.
  // At the moment, these cannot both run simultaneously because both require
  // the SD card.
  pinMode(FIRST_INIT_PIN, OUTPUT);
  digitalWrite(FIRST_INIT_PIN, HIGH);
  // delay(1000);
  bool gnssReady = false;
  bool sdReady = false;
  bool pipelinePhaseReady = false;
  float oldThresh = INITIAL_ADXL_THRESHOLD;
  float iridiumThresholdG = INITIAL_ADXL_THRESHOLD;
  bool haveIridiumThreshold = false;
  int iridiumPayloadBytes = 0;
  int iridiumInitErr = -1;
  int iridiumSignalErr = -1;
  int iridiumSignalQuality = -1;
  int iridiumSendErr = -1;
  const char* iridiumStatus = "not_evaluated";

  Serial.begin(115200);
  waitForDebugSerial();

  pinMode(LED_PIN, OUTPUT);
  setProgramLedState(true);
  gDebug.stage = "setup";

  gDebug.stage = "adxl_setup";
  debugPrintStartupStep(F("[setup] Starting ADXL setup"));
  if (setupADXL()) {  // setupADXL retuns true if errors
    FAIL(ERR_SENSOR_WRITE, "ADXL initialization failed");
    goto shutdown_sequence;
  }
  debugPrintStartupStep(F("[setup] ADXL setup complete"));

  SPI.setRX(SD_MISO_PIN);
  SPI.setTX(SD_MOSI_PIN);
  SPI.setSCK(SD_SCK_PIN);
  SPI.begin();
  pinMode(SD_CS_PIN, OUTPUT);
  digitalWrite(SD_CS_PIN, HIGH);

  gDebug.stage = "sd_begin";
  debugPrintStartupStep(F("[setup] Starting SD init"));
  if (!SD.begin(SdSpiConfig(SD_CS_PIN, SHARED_SPI, SD_SCK_MHZ(12), &SPI))) {
    FAIL(ERR_SD_BEGIN, "SD initialization failed");
    goto shutdown_sequence;
  }
  sdReady = true;
  debugPrintStartupStep(F("[setup] SD init complete"));

  debugPrintStartupStep(F("[setup] Restoring ADXL threshold"));
  readTextFileFloat("THRESHOLD.txt", oldThresh);
  if (setADXLRegThreshold(oldThresh)) {  // setADXLRegThreshold retuns true if errors
    FAIL(ERR_SENSOR_WRITE, "Failed to restore ADXL threshold");
    goto shutdown_sequence;
  }
  gAdxlWakeThresholdReady = true;
  if (!setAdxlStandbyMode()) {
    FAIL(ERR_SENSOR_WRITE, "Failed to place ADXL into standby mode");
    goto shutdown_sequence;
  }
  debugPrintStartupStep(F("[setup] ADXL threshold restore complete"));

  gDebug.stage = "open_root";
  debugPrintStartupStep(F("[setup] Opening SD root"));
  if (!gRoot.open("/", O_RDONLY)) {
    FAIL(ERR_PARSE_WAV, "Failed to open SD root");
    goto shutdown_sequence;
  }
  pipelinePhaseReady = true;
  debugPrintStartupStep(F("[setup] SD root open complete"));

  if (!loadDtState()) {
    // Start fresh; this is the first boot OR the state file was corrupted/deleted
  }

  gDebug.stage = "model_init";
  debugPrintStartupStep(F("[setup] Starting model init"));
  if (!ModelInit(model_int8_tflite, tensorArena, kTensorArenaSize)) {
    FAIL(ERR_MODEL_INIT, "Model initialization failed");
    goto shutdown_sequence;
  }
  debugPrintStartupStep(F("[setup] Model init complete"));

  gDebug.stage = "ready";
  multicore_launch_core1(core1Worker);
  gSetupReady = true;

  if (gSetupReady && !gFatalFailure) {
    if (kDebugPipeline && Serial) {
      Serial.println(F("Beginning Final Deployment Data Processing Pipeline"));
    }
    if (!runPipelineOnce(&iridiumThresholdG)) {
      if (gDebug.hasIndex) {
        appendCurrentRunStatusLog("adxl_status", currentRunShouldLockdown() ? "standby_lockdown" : "measurement_restore_pending");
      }
      if (gFatalFailure) {
        goto shutdown_sequence;
      }
      FAIL(ERR_PIPELINE_RUN, "Pipeline run returned failure");
      goto shutdown_sequence;
    }
    haveIridiumThreshold = true;
    if (!setAdxlStandbyMode()) {
      FAIL(ERR_SENSOR_WRITE, "Failed to return ADXL to standby mode");
      goto shutdown_sequence;
    }
    blinkProgramStageComplete();
  }

  if (gDebug.hasIndex && !gFatalFailure) {
    appendCurrentRunStatusLog("adxl_status", currentRunShouldLockdown() ? "standby_lockdown" : "ready");
  }

  gDebug.stage = "gnss";
  debugPrintStartupStep(F("[setup] Starting GNSS acquisition"));
  setupGNSS();
  gnssReady = getGNSSData();
  if (kDebugPipeline && Serial) {
    if (!gGnssModuleDetected) {
      Serial.println(F("[setup] GNSS module not detected"));
    } else {
      Serial.print(F("[setup] GNSS acquisition "));
      Serial.println(gnssReady ? F("complete") : F("timed out"));
    }
  }
  if (gDebug.hasIndex) {
    appendCurrentRunStatusLog(
      "gnss_status",
      !gGnssModuleDetected ? "module_not_detected" : (gnssReady ? "ready" : "timed_out"));
    appendCurrentRunGnssLog();
  }

  digitalWrite(TOGGLE_GNSS, LOW);

  if (pipelinePhaseReady && gnssReady && gDebug.hasIndex) {
    applyGnssTimestampToFile(String(gDebug.index) + ".txt");
    applyGnssTimestampToFile(String(gDebug.index) + ".wav");
  }

  blinkProgramStageComplete();

  if (sdReady) {
    loadIridiumState();
  }

  if (sdReady && gnssReady && GNSS.date.isValid()) {
    if (iridiumDay != GNSS.date.day()) {
      iridiumDay = GNSS.date.day();
      iridiumDayCount = 0;
    }

    if (GNSS.date.day() == 15 && !alreadyResetQuota) {
      bytesUsedThisMonth = 0;
      alreadyResetQuota = 1;
    } else if (GNSS.date.day() == 17) {
      alreadyResetQuota = 0;
    }
  }

  buildIridiumMessage(iridiumThresholdG, haveIridiumThreshold);

  // + 1 for the end character
  iridiumPayloadBytes = messageBytes + 1;

  if (!sdReady) {
    iridiumStatus = "skipped_no_sd";
    if (kDebugPipeline && Serial) {
      Serial.println(F("[setup] Iridium skipped: no SD"));
    }
  } else if (iridiumPayloadBytes >= 340) {
    iridiumStatus = "skipped_size";
    if (kDebugPipeline && Serial) {
      Serial.print(F("[setup] Iridium skipped: payload too large (bytes="));
      Serial.print(iridiumPayloadBytes);
      Serial.println(F(")"));
    }
  } else if ((iridiumPayloadBytes + bytesUsedThisMonth) >= 30000) {
    iridiumStatus = "skipped_monthly_quota";
    if (kDebugPipeline && Serial) {
      Serial.print(F("[setup] Iridium skipped: monthly quota (payload="));
      Serial.print(iridiumPayloadBytes);
      Serial.print(F(", used="));
      Serial.print(bytesUsedThisMonth);
      Serial.println(F(")"));
    }
  } else if (iridiumDayCount >= 3) {
    iridiumStatus = "skipped_daily_limit";
    if (kDebugPipeline && Serial) {
      Serial.print(F("[setup] Iridium skipped: daily limit (count="));
      Serial.print(iridiumDayCount);
      Serial.println(F(")"));
    }
  } else {
    gDebug.stage = "iridium_setup";
    if (kDebugPipeline && Serial) {
      Serial.println(F("[setup] Starting Iridium setup"));
    }
    if (!setupIridium(iridiumInitErr, iridiumSignalErr, iridiumSignalQuality)) {
      iridiumStatus = gIridiumModuleDetected ? "init_failed" : "module_not_detected";
      if (kDebugPipeline && Serial) {
        if (!gIridiumModuleDetected) {
          Serial.println(F("[setup] Iridium module not detected"));
        } else {
          Serial.print(F("[setup] Iridium init failed (beginErr="));
          Serial.print(iridiumInitErr);
          Serial.print(F(", signalErr="));
          Serial.print(iridiumSignalErr);
          Serial.print(F(", signalQuality="));
          Serial.print(iridiumSignalQuality);
          Serial.println(F(")"));
        }
      }
    } else {
      gDebug.stage = "iridium_send";
      if (kDebugPipeline && Serial) {
        Serial.print(F("[setup] Iridium setup complete (signalErr="));
        Serial.print(iridiumSignalErr);
        Serial.print(F(", signalQuality="));
        Serial.print(iridiumSignalQuality);
        Serial.println(F(")"));
        Serial.print(F("[setup] Sending Iridium message (bytes="));
        Serial.print(iridiumPayloadBytes);
        Serial.println(F(")"));
      }
      if (sendIridiumMessage(gIridiumMessage, iridiumSendErr)) {
        iridiumStatus = "sent";
        if (kDebugPipeline && Serial) {
          Serial.println(F("[setup] Iridium send complete"));
        }
      } else {
        iridiumStatus = "send_failed";
        if (kDebugPipeline && Serial) {
          Serial.print(F("[setup] Iridium send failed (sendErr="));
          Serial.print(iridiumSendErr);
          Serial.println(F(")"));
        }
      }
    }
  }

  gDebug.stage = "iridium_log";
  if (kDebugPipeline && Serial) {
    Serial.print(F("[setup] Iridium final status: "));
    Serial.println(iridiumStatus);
  }
  if (!appendCurrentRunIridiumLog(
        iridiumStatus,
        iridiumInitErr,
        iridiumSignalErr,
        iridiumSignalQuality,
        iridiumSendErr)) {
    appendCurrentRunStatusLog("iridium_log_status", "append_failed");
    if (kDebugPipeline && Serial) {
      Serial.println(F("[setup] Warning: failed to append Iridium log"));
    }
  }

  blinkProgramStageComplete();

  if (!gFatalFailure) {
    gDebug.stage = "finalize";
  }

  if (sdReady) {
    saveIridiumState();
  }

shutdown_sequence:  // All paths lead here
  setProgramLedState(false);
  finalizeDeploymentShutdown();
}

void loop() {
  if (!(kDebugPipeline && kBypassFinalShutdownForDebug)) {
    return;
  }

  static unsigned long lastPrintMs = 0;
  const unsigned long now = millis();
  if (now - lastPrintMs < 1000) {
    return;
  }
  lastPrintMs = now;

  Serial.print(F("[loop] fatal="));
  Serial.print(gFatalFailure ? F("true") : F("false"));
  Serial.print(F(" stage="));
  Serial.println(gDebug.stage ? gDebug.stage : "unknown");
}
