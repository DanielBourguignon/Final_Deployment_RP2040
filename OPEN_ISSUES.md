# Open Issues

This file tracks known problems and incomplete integration points in the deployment sketch. Update it as fixes land so it remains the current checklist.

## Compile Blockers

- `IridiumSBD.h` is included, but the `IridiumSBD` library is not currently installed in this environment.
- `sendIridiumMessage(message);` is called in `setup()`, but there is no visible in-scope `message` defined there.
- `String fileName` is redeclared multiple times in `setup()`, which is likely to fail compilation once earlier blockers are cleared.
- `getGNSSData()` returns `TinyGPSPlus&`, but on timeout it attempts to `return NULL;`, which is invalid for a reference return type.

## Incomplete Integration

- Iridium message construction is still a placeholder. The code checks quotas and then calls `sendIridiumMessage(...)`, but no final payload is assembled.
- GNSS is only partially integrated. Fix acquisition, timeout behavior, and timestamp usage before relying on the returned data.
- The ADXL threshold conversion still uses the older direct scaling path and has not incorporated the newer Parseval/RMS/peak reasoning.

## Fragile Or Likely Incorrect Logic

- `pinMode(KILL_SD_PIN, HIGH)` is suspicious and should likely be `pinMode(KILL_SD_PIN, OUTPUT)` plus a separate `digitalWrite(...)`.
- The SD init failure path still uses an old `while(1)` blink loop before `FAIL(...)`, which bypasses the newer unified failure handling.
- The file timestamp code subtracts 4 from `GNSS.time.minute()`, which looks like timezone correction on the wrong field and can underflow.
- The deployment sketch performs most work inside `setup()` and leaves `loop()` empty, which makes retry/recovery behavior rigid.

## Architecture Follow-Up

- The merged sketch now combines ADXL, GNSS, Iridium, and the RP2040 ML pipeline in one file, but the orchestration is not yet cleanly separated by stage.
- Deployment-time error handling and the inherited pipeline error/logging systems should be reconciled into one consistent approach.
- After the compile blockers are fixed, run a fresh compile to surface the next layer of integration issues before changing runtime behavior.
