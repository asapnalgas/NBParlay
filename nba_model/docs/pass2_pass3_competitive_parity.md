# Pass 2 + Pass 3 Competitive Parity Matrix

## Objective
Blend the strongest publicly-visible usability patterns from RotoWire-style workflow and BETR-style workflow into one board without copying proprietary code or protected assets.

## Public Feature Inputs Used
- RotoWire lineup context and starter certainty emphasis:
  [RotoWire NBA Lineups](https://www.rotowire.com/basketball/nba-lineups.php)
- RotoWire PrizePicks context and line-reference workflow:
  [RotoWire PrizePicks Picks](https://www.rotowire.com/picks/prizepicks/)
- RotoWire optimizer/tooling pattern for dense, sortable decision workflows:
  [RotoWire NBA Optimizer](https://www.rotowire.com/daily/nba/optimizer.php)
- BETR product UX cues (simple card-first experience and quick pick framing):
  [Betr](https://www.betr.app/)
  and support portal:
  [Betr Help Center](https://help.betr.app/)
- BETR onboarding / discoverability patterns:
  [Betr on App Store](https://apps.apple.com/us/app/betr-fantasy-sports/id1575638442)
- Live lineup and game-status cross reference surfaced in the board:
  [NBA Daily Lineups](https://www.rotowire.com/basketball/nba-lineups.php)
  and
  [ESPN NBA Scoreboard](https://www.espn.com/nba/scoreboard)

## Cross-Reference Results (What Was Missing vs Existing)
- Already present before this pass:
  - Starter-first cards
  - Confidence/error bands
  - Line movement fields
  - No-bet guardrails
  - Live source diagnostics
- Missing and now added:
  - Board mode switcher (`Best of Both`, `Roto-Style Dense`, `Betr-Style Cards`)
  - Top-edges rail for fast market scanning
  - Game hub cards summarizing starter/actionable/no-bet distribution by matchup
  - Signal tiers (`Anchor`, `Edge`, `Boost`) at market-row level
  - Snapshot web-mode fallback for hosted read-only operation when local API is unavailable
  - Root redirect + static snapshot packaging support for web deployment

## Implementation Notes
- UI and interaction patterns were blended; no third-party visual assets were copied.
- Existing engine outputs were reused so new UI sections stay data-consistent with model outputs.
- External links are exposed as references only (scoreboard + lineups), not as hidden scraping surfaces.
- Hosted static deploy mode intentionally runs read-only (snapshot-backed) unless a live API backend is also deployed.
