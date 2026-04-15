# Open Issues

This file tracks known problems and incomplete integration points in the deployment sketch. Update it as fixes land so it remains the current checklist.

## Compile Status

- The sketch now compiles in this environment with `Iridium SBD` and `TinyGPSPlus` installed.
- Current compile status still reports very high RAM usage (`92%`) and should be treated as a stability risk.

## Incomplete Integration

- Iridium message construction is still a placeholder. The code now builds a minimal compile-safe status message, but the final deployment payload format still needs to be designed.
- GNSS is only partially integrated. Timeout now fails cleanly for compilation, but downstream behavior still assumes GNSS data may be unavailable and needs a deliberate product decision.
- The ADXL threshold conversion still uses the older direct scaling path and has not incorporated the newer Parseval/RMS/peak reasoning.

## Fragile Or Likely Incorrect Logic

- The deployment sketch performs most work inside `setup()` and leaves `loop()` empty, which makes retry/recovery behavior rigid.

## Architecture Follow-Up

- The merged sketch now combines ADXL, GNSS, Iridium, and the RP2040 ML pipeline in one file, but the orchestration is not yet cleanly separated by stage.
- Deployment-time error handling and the inherited pipeline error/logging systems should be reconciled into one consistent approach.
- After the compile blockers are fixed, run a fresh compile to surface the next layer of integration issues before changing runtime behavior.
