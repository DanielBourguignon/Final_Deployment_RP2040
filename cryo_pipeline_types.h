#pragma once
#include <stdint.h>

struct WavInfo {
  uint16_t channels = 0;
  uint32_t sampleRate = 0;
  uint16_t bitsPerSample = 0;
  uint16_t blockAlign = 0;
  uint32_t dataSize = 0;
  uint32_t dataStart = 0;
};

struct ThresholdSnapshot {
  float thresholdLow;
  float thresholdHigh;
  float lowTarget;
  float highTarget;

  float muAmbient;
  float stddevAmbient;
  float weightAmbient;

  float muEvent;
  float stddevEvent;
  float weightEvent;

  float muStorm;
  float stddevStorm;
  float weightStorm;

  uint8_t orderedLabels[3];
  uint8_t orderedCount;
};

struct PersistentDTState {
  uint32_t magic;      // 'CRTS'
  uint16_t version;    // schema version
  uint16_t reserved;

  uint32_t sequence;   // monotonically increasing save counter
  uint32_t checksum;   // FNV-1a over the whole struct with checksum = 0

  float thresholdLow;
  float thresholdHigh;

  float mean[3];
  float m2[3];
  float weightSum[3];

  uint8_t lastOrderValid;
  uint8_t lastOrderedCount;
  uint8_t lastOrderedLabels[3];
  uint8_t boostRemaining;

  uint8_t reserved2[2];
};

struct BenchTimes {
  uint32_t discover_us = 0;
  uint32_t dcra_us = 0;
  uint32_t rename_us = 0;
  uint32_t stream_us = 0;
  uint32_t tail_us = 0;
  uint32_t worker_us = 0;
  uint32_t dt_save_us = 0;
  uint32_t sensor_us = 0;
  uint32_t total_us = 0;
};
