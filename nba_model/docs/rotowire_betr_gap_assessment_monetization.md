# RotoWire / Betr Gap Assessment (Monetization-Focused)

Date: 2026-03-11

## Goal
Close the practical gap between this engine and premium fantasy-props products by improving:

1. reliability
2. projection calibration
3. pregame certainty
4. coverage depth
5. UX speed-to-decision
6. product/commercial readiness

## Current Position
The app already has strong foundations:

- starter-first board
- live sync pipeline
- per-player confidence/error features
- guardrails and no-bet flags
- market-line joins and edge generation
- blended board modes (`Best of Both`, `Roto-Style Dense`, `Betr-Style Cards`)

The remaining gap is not “missing basic features.” It is mostly about **execution quality and consistency under live conditions**.

## Parity Matrix
| Capability | Current State | Gap vs Premium Apps | Owner | Next Action |
|---|---|---|---|---|
| Pregame starter/minutes certainty | Partial | Still misses late lock-window changes | App + User Data | Stronger T-90/T-30/T-5 lock weighting + higher-quality lineup confirmations |
| Per-market calibration (PTS/REB/AST/PRA) | Partial | Calibration drift by market/time window | App | Per-market specialist calibration refresh against benchmark |
| Live ingestion reliability | Partial | Intermittent sync interruptions degrade trust | App + Infra | Harden retry/state/heartbeat and keep running continuously |
| Coverage breadth (slate + bench + call-ups) | Partial | User sees thin coverage when upstream rows are missing | App + User Data | Expand ingestion/backfill and verify roster completeness daily |
| Actionability quality (no-bet vs bet) | Partial | Overly conservative states can suppress value rows | App | Improve uncertainty bands and no-bet threshold calibration |
| UX speed-to-decision | Good | Still too dense for non-technical users in default flow | App | Keep default card flow concise; move deep diagnostics into expandable details |
| Benchmark governance (head-to-head tracking) | Partial | Not enough rolling comparable samples to claim superiority | App | Run ongoing benchmark capture + weekly calibration audits |
| Commercial layer (accounts, billing, funnel metrics) | Missing | Cannot monetize without usage/billing instrumentation | Product | Add auth, plans, metering, conversion analytics, paywall gates |

## What You Can Do Immediately (High Impact)
- Keep provider keys active and quota-safe to avoid silent feed gaps.
- Keep daily historical backfill running so confidence/error estimates stay current.
- Import same-day line snapshots close to lock; stale lines weaken market anchoring.
- Review lock-window injury/news updates before slate lock.

## What the App Should Keep Doing Automatically
- Live sync loop with hard retries and health checks.
- Frequent lineup/injury/odds refresh.
- Daily retrain + recheck + benchmark capture.
- Per-market calibration updates based on benchmark drift.

## Launch Readiness Gates
Use these gates before paid rollout:

1. Live sync reliability gate:
   - live sync continuously running
   - no recurring runtime errors
2. Accuracy gate:
   - stable recheck error band within target
   - no major market-specific drift
3. Coverage gate:
   - broad slate player inclusion (starters + relevant rotation)
4. Benchmark gate:
   - enough rows for fair, same-time comparisons
5. Product gate:
   - account + billing + conversion analytics enabled

## Legal/Branding Note
The app should be **inspired by** workflow patterns, not a direct clone of proprietary branding/assets/code from third-party products.
