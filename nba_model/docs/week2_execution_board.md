# Week 2 Execution Board (Pipeline Hardening)

## Goal
Harden ingestion and sync behavior for reliability:
- idempotent imports
- lower write churn
- contract drift visibility

## Completed
- Added ingestion manifest tracking in `data/pipeline/ingestion_manifest.json`.
- Added deterministic frame fingerprints and idempotency registration.
- Wired duplicate-payload skip logic into:
  - historical imports
  - season priors imports
  - PrizePicks lines imports
  - historical bootstrap fetch
- Added `duplicate_skipped` ingestion outcome events with details.
- Added API drift audit endpoint:
  - `GET /api/data/drift-audit`
- Added optional drift expansion on pipeline status:
  - `GET /api/data/pipeline?include_drift=true`
- Reduced CSV write churn in live sync by skipping unchanged writes in `_write_csv_frame`.
- Added developer-mode UI controls for:
  - running drift audit on demand
  - viewing drift report output

## Pending Week 2 Hardening
- Add retry backoff/jitter policy centralization for all provider fetches.
- Add dataset-specific idempotency source keys for all async provider ingestions.
- Add unit tests for duplicate-skip behavior and drift-audit summaries.
- Add alert thresholds for drift audit (warnings in status when required columns disappear).

## Success Criteria
- Re-uploading identical payloads does not rewrite canonical datasets.
- Ingestion events clearly indicate `duplicate_skipped` outcomes.
- Drift audit is available from API and visible in developer UI.
- Repeated sync loops do not rewrite unchanged CSV outputs.
