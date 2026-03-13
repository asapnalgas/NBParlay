# Week 1 Execution Board (Data Foundation)

## Goal
Build a strict, observable data foundation before further model tuning:
- canonical schemas
- bronze/silver/gold traceability
- import validation + rejection logging

## Completed
- Added canonical data contracts and zone layout in `src/data_pipeline.py`.
- Added pipeline directories:
  - `data/pipeline/bronze`
  - `data/pipeline/silver`
  - `data/pipeline/gold`
  - `data/pipeline/rejections`
  - `data/pipeline/ingestion_events.jsonl`
- Added strict import flow instrumentation:
  - season priors import (`import_season_priors_*`)
  - PrizePicks lines import (`import_prizepicks_lines_*`)
  - historical import (`import_historical_*`)
- Added rejection reasons and persisted rejection logs.
- Added ingestion event logging (rows in/out/rejected + source + details).
- Added API visibility:
  - `GET /api/data/contracts`
  - `GET /api/data/pipeline`
  - `GET /api/schema` now includes `data_contracts`
  - `GET /api/status` now includes `data_pipeline`
- Added a developer-only UI panel for pipeline diagnostics.

## Pending Week 1 Hardening
- Add stricter uniqueness constraints per dataset as configurable policy.
- Add optional "quarantine" files for malformed rows beyond rejections.
- Add automated daily audit job for contract drift detection.
- Add unit tests for each importer rejection path and event logging.

## Success Criteria
- Every import writes:
  - bronze snapshot
  - silver output
  - rejection log (if any)
  - ingestion event
- API endpoints return contract + pipeline diagnostics without errors.
