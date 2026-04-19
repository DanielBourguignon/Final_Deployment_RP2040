# Final_Deployment_RP2040

Deployment-focused RP2040 Arduino sketch that ties together the Cryophone processing pipeline, ADXL355 wake-threshold programming, SD-card file handling, GNSS timestamping, and Iridium status messaging.

This README is intended to track the script's real behavior as the deployment firmware evolves. If major behavior, data flow, thresholds, peripheral sequencing, or debug strategy changes, this file should be updated accordingly.

## High-Level Purpose

The sketch is a one-shot deployment program for the RP2040 side of the system.

On boot, it:

1. Holds system power on.
2. Verifies and programs the ADXL355.
3. Mounts the SD card.
4. Loads prior dynamic-threshold state.
5. Runs the audio-processing pipeline on the newest numbered WAV file on the card.
6. Computes and applies an updated ADXL wake threshold.
7. Optionally gathers GNSS metadata.
8. Optionally sends a compact Iridium message.
9. Writes bookkeeping/log files back to SD.
10. Executes the final shutdown sequence so the RP2040 can power itself down.

The intended deployment model is:

- The SAMD recorder creates numbered WAV recordings such as `1.wav`, `2.wav`, etc.
- The RP2040 processes the highest-numbered WAV on the SD card.
- The processed WAV replaces the original WAV in place.
- The threshold learned from that run is written into the ADXL355 for the next wake cycle and also persisted to SD.

## Major Functional Blocks

### 1. ADXL355 Bring-Up and Threshold Programming

The ADXL355 is connected over I2C.

Boot-time behavior:

- `setupADXL()` binds `Wire` to the board's ADXL pins, starts I2C, and verifies that the device responds with the expected `DEVID_AD` and `PARTID` values.
- The sketch does not fully reset the ADXL on boot.
- The last persisted threshold is restored from `THRESHOLD.txt` and immediately programmed into the ADXL355.

End-of-run behavior:

- After the pipeline finishes, the chosen threshold is converted from controller counts into `g`.
- `setADXLRegThreshold()` writes the threshold and related activity-detection configuration into the ADXL355.
- The same threshold value is saved to `THRESHOLD.txt` so it can be restored on the next boot.
- The freshly computed threshold is also kept in memory and handed directly to the later Iridium payload builder, so the modem message does not depend on re-reading `THRESHOLD.txt` from SD after the pipeline run.

Current ADXL threshold behavior:

- The adaptive controller now operates in a peak-amplitude domain based on centered PCM sample counts.
- The ADXL threshold is therefore derived from the processed run's peak-count threshold rather than the old FFT-power-based metric.
- The current fallback threshold if `THRESHOLD.txt` is missing is `0.020 g`.

### 2. SD Card and File Discovery

The SD card is explicitly bound to the board's custom SPI pin mapping before `SD.begin(...)`.

The pipeline expects numbered WAV files in the SD root:

- valid examples: `1.wav`, `2.wav`, `15.wav`
- invalid examples: `test.wav`, `foo1.wav`, nested files in subdirectories

`findLatestRecording()` scans the SD root and selects the highest-numbered `N.wav` file as the pipeline input.

If no numbered WAV file exists:

- the pipeline fails with `ERR_NO_RECORDING`
- no audio processing occurs
- the normal shutdown path still runs

### 3. Audio Processing Pipeline

The RP2040 pipeline works on the selected WAV file in several stages:

1. Parse and validate the WAV header.
2. Create a staged WAV output file and a matching BIN output file.
3. Apply DCRA (moving-average DC/bias removal) to the WAV in a streamed, blockwise fashion.
4. Feed the DCRA-corrected samples into the FFT / inference pipeline on core 1.
5. Rename the staged WAV back onto the original numbered WAV path.
6. Finalize any queued FFT tail output.
7. Save dynamic-threshold state.
8. Apply the new ADXL threshold.
9. Append success metrics to the current run's text log.

The processing path is intentionally streamed and staged rather than fully in-memory. This keeps RAM use bounded while still allowing the audio file to be rewritten in place.

Current amplitude compensation:

- Because the board's front-end circuitry only delivers a reduced fraction of the physical input signal to the RP2040, the sketch now applies a fixed post-DCRA gain of `4.2926963207` to each corrected PCM sample before writing the processed WAV and feeding the FFT/inference path.
- This gain is intentionally applied after DC/bias removal so the signal amplitude is compensated without also magnifying the large DC offset present in the raw waveform.
- When `kDebugPipeline` is enabled, the streamed DCRA path now prints a final `clipped_samples` count so large-event saturation after the compensation gain can be diagnosed during testing.
- The older non-streamed DCRA helper has been removed so there is now a single live DCRA implementation path to maintain.

Fatal behavior:

- Fatal failures raised from `runPipelineOnce()` now jump directly to the shared shutdown path instead of continuing into later GNSS and Iridium handling.
- The one optional exception is run-log append failure: `kTreatRunLogFailureAsNonfatalForDebug` can be enabled to keep post-run behavior going for diagnostics even if `ERR_RUN_LOG` occurs.

### 4. Inference and Dynamic Thresholding

The model outputs two probabilities per frame:

- ambient
- storm

Label assignment is:

- `ambient` if ambient probability is at least the ambient decision threshold
- otherwise `storm` if storm probability is at least the storm decision threshold
- otherwise `event`

The dynamic-threshold controller then:

- keeps exponentially decayed weighted Gaussian statistics for `ambient`, `event`, and `storm`
- updates those statistics using the chosen label, frame peak amplitude, and clipped confidence
- maintains `thresholdLow` and `thresholdHigh`
- chooses the final sensor threshold based on whether storm is currently the dominant class by weight

Persistent dynamic-threshold state is stored in alternating files:

- `DTA.BIN`
- `DTB.BIN`

These files allow the threshold controller to carry learned state across boots and power loss.

### 5. GNSS

GNSS is intentionally placed near the end of the run so bulk processing happens first.

Current behavior:

- `setupGNSS()` powers the GNSS module and binds `Serial2` to the GNSS UART pins.
- `getGNSSData()` now does a short presence probe first.
- If no GNSS traffic is detected, the sketch exits GNSS quickly instead of spending the full timeout waiting for a fix.
- If the module appears present, the sketch still waits for a full fix using the current validity requirements.

GNSS is used for:

- applying file timestamps to the current run's `.wav` and `.txt`
- backtracking those timestamps from GNSS fix time using the RP2040 elapsed-to-fix time plus the SAMD's logged `Time During Recording` value from the paired `N.txt`
- enriching the Iridium payload when valid location/date/time are available
- daily quota/reset logic for Iridium bookkeeping

Current timestamp approximation:

- The file timestamp is intended to represent when the event/recording started, not when GNSS finally got a fix.
- The sketch therefore subtracts two elapsed durations from the GNSS fix time:
  - RP2040 boot-to-fix elapsed time
  - SAMD `Time During Recording`
- This is an approximation because the SAMD value includes recording-side tail/flush time, but it is currently the best available deployment-side estimate of event start time.

### 6. Iridium

The Iridium modem is optional and is handled after the main pipeline run.

Current behavior:

- The script builds a compact message centered around threshold information, with GNSS fields included when valid.
- The threshold included in that message now comes directly from the threshold that was just computed and applied during `runPipelineOnce()`, rather than from a second post-run read of `THRESHOLD.txt`.
- The script now performs a quick UART `AT` probe before full Iridium initialization so a physically absent modem can be skipped quickly.
- The modem is initialized only if message-size and quota checks allow it.
- Outcomes such as `sent`, `init_failed`, `send_failed`, or quota-based skips are logged per run.

Persistent Iridium bookkeeping is stored in small SD text files:

- `iribytes.txt` - monthly bytes used
- `iricount.txt` - per-day send count
- `iriday.txt` - last GNSS day used for daily bookkeeping
- `iriquota.txt` - monthly reset state

These files are written to SD so bookkeeping survives reboots and power loss.

## Key SD-Card Artifacts

### Input / output audio files

- `N.wav` - the newest numbered WAV is the pipeline input; after processing, the staged output replaces it
- `N.bin` - FFT/pipeline side output corresponding to the same run

### Staging and threshold files

- `STAGE.WAV` - temporary staged WAV output
- `STAGE.BAK` - temporary backup of the original WAV during replacement
- `THRESHOLD.txt` - persisted ADXL threshold in `g`

### Dynamic-threshold persistence

- `DTA.BIN`
- `DTB.BIN`

### Per-run logs

The paired `N.txt` file for the current run can accumulate:

- `error_code=...` on failure
- `error_stage=...` on failure
- `adxl_status=...`
- `gnss_status=...`
- `threshold_g=...`
- class statistics such as ambient/event/storm mean and stddev
- benchmark timings such as discover, DCRA, rename, stream, tail, worker, threshold-save/apply, and total time
- Iridium status fields

## Power and Shutdown Behavior

At the top of `setup()`, the sketch asserts the pins that keep:

- the Pico alive
- the SD card alive
- the SAMD informed that the Pico is still working

At the end of execution, all paths lead to the final shutdown function.

That shutdown function is responsible for:

- optional debug prints
- asserting the SD and Pico kill behavior
- ending the RP2040 run

Important note:

- Some debug modes intentionally bypass final self-shutdown for diagnosis.
- Those modes are useful during bring-up but should not be left enabled in deployment firmware.

## Debug / Diagnostic Controls

The sketch keeps several compile-time debug booleans near the top of the file.

These currently control behavior such as:

- serial startup diagnostics
- whether the onboard/debug LED stays on during execution
- whether a run-log append failure is allowed to continue for debugging
- tuple printing for per-frame classifications
- dynamic-threshold state dump printing
- optional shutdown bypass / loop heartbeat behavior during debug sessions

Because these are compile-time flags, the exact deployment behavior depends partly on the current values in the sketch at build time.

## Current Operational Assumptions

- The SD card contains numbered WAV recordings in the root directory.
- The WAV files are mono, 16-bit PCM files in the format expected by the pipeline.
- The ADXL355 remains available on I2C and is programmed both at boot restore and end-of-run.
- The RP2040 is intended to behave as a single-run appliance, not a continuously looping application.
- GNSS may be absent; the sketch now detects that case quickly.
- Iridium may or may not be enabled by runtime conditions such as quota, payload size, and available metadata.

## Files in This Project

- `Final_Deployment_RP2040.ino` - main deployment firmware
- `cryo_model_0.h` - embedded model weights/data
- `cryo_pipeline_types.h` - shared pipeline structures and types
- `OPEN_ISSUES.md` - current known issues and follow-up items

## Maintenance Note

This README should be treated as part of the deployment documentation, not as a one-time placeholder.

When major changes are made, update this file for items such as:

- threshold-domain changes
- ADXL programming behavior
- GNSS sequencing or validity requirements
- Iridium payload semantics or quota logic
- shutdown behavior
- SD file layout or naming conventions
- pipeline stage order
