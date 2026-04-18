# Open Issues

This file tracks known problems and incomplete integration points in the deployment sketch. Update it as fixes land so it remains the current checklist.

## Compile Status

- The sketch now compiles in this environment with `Iridium SBD` and `TinyGPSPlus` installed.
- Current compile status still reports very high RAM usage (`92%`) and should be treated as a stability risk.

## Incomplete Integration

- GNSS is only partially integrated. `getGNSSData()` now short-circuits quickly when the module appears absent, but when a live module is present it still waits for location, date, time, altitude, and speed even though downstream logic mostly needs date/time and optional location.
- The ADXL threshold value is now written at end-of-run and restored on boot, and the adaptive controller now learns on time-domain peak PCM counts instead of FFT-power-derived amplitude. The remaining work here is empirical tuning and validation of the new peak-domain threshold seeds/behavior in the field.
- ADXL bring-up now fails at the correct stage, but `setupADXL()` still reports only a generic pass/fail result. More granular debug output per register read would make hardware bring-up much faster.

## Fragile Or Likely Incorrect Logic

- Temporary diagnostics are still enabled. `kBypassFinalShutdownForDebug = true` and the debug `loop()` heartbeat intentionally prevent the RP2040 from killing itself, so both must be disabled before deployment.
- `kDebugPipeline = true` currently enables `waitForDebugSerial()`, which can pause boot for up to 5 seconds waiting for USB serial. That delay is useful for debugging but should not ship in deployment firmware.
- `SetToStandbyMode()` is still semantically wrong: it writes `0x00` (measurement mode) even though the function name says standby mode.
- If `THRESHOLD.txt` is missing or cannot be parsed on boot, the sketch falls back to `INITIAL_ADXL_THRESHOLD = 0.020 g`. That is much safer than `0 g`, but it still deserves validation as a deployment-default wake threshold.
- `kTreatRunLogFailureAsNonfatalForDebug` can intentionally override fatal shutdown for `ERR_RUN_LOG`. That is useful during debugging, but it should remain `false` in deployment firmware unless continued post-run behavior is specifically desired.
- `appendCurrentRunIridiumLog(...)` returns a success flag, but `setup()` currently ignores it, so Iridium-log append failures are silent.

## Architecture Follow-Up

- The merged sketch now combines ADXL, GNSS, Iridium, and the RP2040 ML pipeline in one file, but the orchestration is not yet cleanly separated by stage.
- Deployment-time error handling and the inherited pipeline error/logging systems should be reconciled into one consistent approach.
- Full shutdown safety still depends on external libraries and peripheral calls returning control; a watchdog or equivalent recovery path is still needed if any lower-level SD/I2C/Iridium call wedges internally.
