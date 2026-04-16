# Open Issues

This file tracks known problems and incomplete integration points in the deployment sketch. Update it as fixes land so it remains the current checklist.

## Compile Status

- The sketch now compiles in this environment with `Iridium SBD` and `TinyGPSPlus` installed.
- Current compile status still reports very high RAM usage (`92%`) and should be treated as a stability risk.

## Incomplete Integration

- GNSS is only partially integrated. Timeout now fails cleanly for compilation, but downstream behavior still assumes GNSS data may be unavailable and needs a deliberate product decision.
- The ADXL threshold value is now written at end-of-run and restored on boot, and the adaptive controller now learns on time-domain peak PCM counts instead of FFT-power-derived amplitude. The remaining work here is empirical tuning and validation of the new peak-domain threshold seeds/behavior in the field.

## Fragile Or Likely Incorrect Logic

- If `THRESHOLD.txt` is missing or cannot be parsed on boot, the sketch falls back to `INITIAL_ADXL_THRESHOLD = 0.020 g`. That is much safer than `0 g`, but it still deserves validation as a deployment-default wake threshold.

## Architecture Follow-Up

- The merged sketch now combines ADXL, GNSS, Iridium, and the RP2040 ML pipeline in one file, but the orchestration is not yet cleanly separated by stage.
- Deployment-time error handling and the inherited pipeline error/logging systems should be reconciled into one consistent approach.
- Full shutdown safety still depends on external libraries and peripheral calls returning control; a watchdog or equivalent recovery path is still needed if any lower-level SD/I2C/Iridium call wedges internally.
