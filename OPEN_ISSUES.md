# Open Issues

This file tracks known problems and incomplete integration points in the deployment sketch. Update it as fixes land so it remains the current checklist.

## Compile Status

- The sketch compiles in this environment with `Iridium SBD` and `TinyGPSPlus` installed.
- Compile status reports very high RAM usage (`92%`) and should be treated as a stability risk.

## Incomplete Integration

- GNSS is integrated for deployment needs. `getGNSSData()` short-circuits quickly when the module appears absent and requires only date/time for success, which matches downstream timestamping and Iridium bookkeeping needs. The remaining limitations are that location is opportunistic rather than actively managed, and timestamp backtracking still depends on the SAMD-side recording-duration approximation.
- GNSS file timestamping backtracks from fix time using `startMillis` plus the SAMD's logged `Time During Recording`, which is the intended approximation for event-start time. This still inherits the SAMD-side approximation error from recording flush/tail time.
- The ADXL threshold value is written at end-of-run and restored on boot, and the adaptive controller learns on time-domain peak PCM counts instead of FFT-power-derived amplitude. The remaining work here is empirical tuning and validation of the peak-domain threshold seeds and behavior in the field.
- `convertThresholdToG()` matches that same zero-based peak-count domain by using a direct linear volts-per-count conversion. If the controller is moved into a signed midscale-referenced domain later, this conversion must be revisited together with the controller seeds and persisted DT-state interpretation.
- The controller now clamps each neighboring-class boundary target between the two learned class means and floors `thresholdLow` to `0.0005 g` in the same count domain. That closes the recent zero-threshold failure mode, but the remaining question is still whether the learned software domain matches the ADXL355 activity-detection path closely enough on real hardware.
- The ADXL is placed into standby during RP2040 processing, and the shutdown path restores measurement mode only when the current run was not mostly storm. Runs with at least 75% storm-labeled frames deliberately stay in standby as a lockdown mode to avoid wasting power on repeated storm-triggered wakes. This behavior should still be validated on hardware against real storm recordings and power measurements.
- The ADXL programming step always uses `thresholdLow`; storm response is represented only through lockdown mode. If future testing shows a need for an intermediate non-lockdown storm response, the role of `thresholdHigh` will need to be revisited.
- Iridium bookkeeping is consolidated into `IRI_STATE.txt` as the only supported format. This is cleaner and more cohesive, but it would still benefit from a checksum/version field if long-term field robustness becomes a concern.
- The pipeline applies a fixed post-DCRA input-amplitude compensation factor of `4.2926963207`. That factor still needs empirical validation across real signals, and large events should be checked for clipping after the compensation is applied.
- ADXL bring-up fails at the correct stage, but `setupADXL()` still reports only a generic pass/fail result. More granular debug output per register read would make hardware bring-up much faster.

## Fragile Or Likely Incorrect Logic

- `kDebugPipeline = true` enables `waitForDebugSerial()`, which can pause boot for up to 5 seconds waiting for USB serial. That delay is useful for debugging but should not ship in deployment firmware.
- If `THRESHOLD.txt` is missing or cannot be parsed on boot, the sketch falls back to `INITIAL_ADXL_THRESHOLD = 0.020 g`. That is much safer than `0 g`, but it still deserves validation as a deployment-default wake threshold.
- `kTreatRunLogFailureAsNonfatalForDebug` can intentionally override fatal shutdown for `ERR_RUN_LOG`. That is useful during debugging, but it should remain `false` in deployment firmware unless continued post-run behavior is specifically desired.
- Iridium-log append failures trigger a best-effort `iridium_log_status=append_failed` write to `N.txt` and a serial warning when debug output is enabled. If SD/log access itself is what failed, that warning line may still be impossible to persist.
- Each successful run-log append-open also writes `run_log_open_status=open_ok`, which makes missing breadcrumbs a little easier to spot when diagnosing whether `N.txt` was writable at each logging step. It does add some repetition to the per-run text log.
- The run log includes both pipeline-only timing (`total_us`) and overall program timing (`program_total_ms`). Any downstream analysis needs to treat those as different scopes rather than interchangeable totals.
- The active embedded model header is `cryo_model_v4.h`. Future model swaps should continue to provide the expected `model_int8_tflite` symbol or the sketch will stop compiling.
- The Iridium threshold payload uses the in-memory threshold produced by the just-finished pipeline run, which removes the stale-fallback risk from the old second `THRESHOLD.txt` read. Boot-time restore still depends on `THRESHOLD.txt`, so corruption or loss of that file before startup remains a separate fallback case.

## Architecture Follow-Up

- The merged sketch combines ADXL, GNSS, Iridium, and the RP2040 ML pipeline in one file, but the orchestration is not yet cleanly separated by stage.
- Deployment-time error handling and the inherited pipeline error/logging systems should be reconciled into one consistent approach.
- Full shutdown safety still depends on external libraries and peripheral calls returning control; a watchdog or equivalent recovery path is still needed if any lower-level SD/I2C/Iridium call wedges internally.
