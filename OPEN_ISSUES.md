# Open Issues

This file tracks known problems and incomplete integration points in the deployment sketch. Update it as fixes land so it remains the current checklist.

## Compile Status

- The sketch now compiles in this environment with `Iridium SBD` and `TinyGPSPlus` installed.
- Current compile status still reports very high RAM usage (`92%`) and should be treated as a stability risk.

## Incomplete Integration

- Iridium message construction is still a placeholder. The code now builds a minimal compile-safe status message, but the final deployment payload format still needs to be designed.
- GNSS is only partially integrated. Timeout now fails cleanly for compilation, but downstream behavior still assumes GNSS data may be unavailable and needs a deliberate product decision.
- The ADXL threshold conversion still uses the older direct scaling path and has not incorporated the newer Parseval/RMS/peak reasoning.
- The dynamic-threshold controller is still learning on FFT-power-derived amplitude rather than a raw-domain amplitude that can be converted directly into volts or `g`.

## Fragile Or Likely Incorrect Logic

- `setupADXL()` still only checks for I2C ACKs during its probe/reset path; it does not read back `DEVID_AD` / `PARTID` to verify that the expected ADXL355 is actually present.
- `setupADXL()` still ignores the return value from its final standby-mode `writeReg(REG_POWER_CTL, 0x01)`, so it can report success even if that write failed.
- The Iridium message does not currently include an explicit success/failure status when GNSS data is valid.
## Architecture Follow-Up

- The merged sketch now combines ADXL, GNSS, Iridium, and the RP2040 ML pipeline in one file, but the orchestration is not yet cleanly separated by stage.
- Deployment-time error handling and the inherited pipeline error/logging systems should be reconciled into one consistent approach.
- After the compile blockers are fixed, run a fresh compile to surface the next layer of integration issues before changing runtime behavior.
