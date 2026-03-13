# NBA Prediction Engine

Local NBA player projection engine with a browser UI.

## What it predicts

- Points
- Rebounds
- Assists
- PRA
- DraftKings fantasy points
- FanDuel fantasy points

## Minimum historical columns

- `player_name`
- `game_date`
- `home`
- `opponent`
- `team`
- `points`
- `rebounds`
- `assists`

## Extra columns the engine can use when present

- `team`
- `position`
- `starter`
- `age`
- `height_inches`
- `weight_lbs`
- `injury_status`
- `health_status`
- `suspension_status`
- `family_context`
- `expected_minutes`
- `salary_dk`
- `salary_fd`
- `implied_team_total`
- `game_total`
- `spread`
- `rest_days`
- `travel_miles`

Historical stat columns such as `steals`, `blocks`, `turnovers`, `three_points_made`, `minutes`, and shooting splits are used as rolling-history inputs. Upcoming rows should leave same-game outcome columns blank or omit them entirely.

## Live automation

The app includes a background live-sync service that:

- polls the NBA public live scoreboard feed
- backfills recent completed games from the official NBA schedule + boxscore feeds (last 6 weeks by default)
- fetches player box scores for completed games
- appends completed player rows into the training dataset
- auto-builds `upcoming_slate.csv` from scheduled games using the latest known player-team assignments
- auto-builds pregame slates from future The Odds API NBA events up to 36 hours ahead, so projections are ready before the official live scoreboard is useful
- auto-builds `provider_context_updates.csv` from connected odds and injury feeds
- auto-retrains the model when new final games are added
- trains on a configurable rolling window (default 28 days)
- applies a modeling-quality filter before train/recheck (NBA team/opponent rows only; removes low-minute zero-stat noise rows)
- auto-generates fresh predictions after sync

Files used by the live engine:

- [live_sync.json](/Users/josephdelallera/Documents/Playground%202/nba_model/config/live_sync.json)
- [training_data.csv](/Users/josephdelallera/Documents/Playground%202/nba_model/data/training_data.csv)
- [upcoming_slate.csv](/Users/josephdelallera/Documents/Playground%202/nba_model/data/upcoming_slate.csv)
- [context_updates.csv](/Users/josephdelallera/Documents/Playground%202/nba_model/data/context_updates.csv)
- [provider_context_updates.csv](/Users/josephdelallera/Documents/Playground%202/nba_model/data/provider_context_updates.csv)
- [live_sync_state.json](/Users/josephdelallera/Documents/Playground%202/nba_model/data/live_sync_state.json)

`context_updates.csv` is the place for manual overrides and hand-maintained context rows. `provider_context_updates.csv` is generated automatically by the live engine from connected providers. The final upcoming slate uses both, with manual `context_updates.csv` values taking precedence.

These files can carry structured pre-game variables that do not come from the NBA public live box score feed, such as:

- injury or health status
- suspensions
- expected minutes
- DraftKings and FanDuel salaries
- betting context
- travel or rest adjustments
- structured off-court context categories

### Provider connectors

The app now ships with a concrete provider choice:

- Odds: The Odds API writes `spread`, `game_total`, and `implied_team_total` into `provider_context_updates.csv`
- Player props: The Odds API event-level player markets write `line_points`, `line_rebounds`, `line_assists`, `line_pra`, and `line_three_points_made` when API credits are available
- Lineups: NBA daily lineups feed writes starter/position context from expected and confirmed lineup statuses
- Injuries: BALDONTLIE `player_injuries` writes structured player injury status into `provider_context_updates.csv`

For PrizePicks-style player prop workflows, the key prediction columns are `predicted_points`, `predicted_rebounds`, `predicted_assists`, and `predicted_pra`. The live engine now tries to keep those projections ready for the next slate before tipoff, not just after the official day-of scoreboard populates.

The official NBA injury report page is still monitored so the latest report URL is tracked in `live_sync_state.json`, but the app does not pretend that the public report page alone is enough to build a complete row-level injury feed.

Set your provider keys before launching the app if you want those feeds to populate automatically:

```bash
export ODDS_API_KEY="your_odds_api_key"
export BALLDONTLIE_API_KEY="your_balldontlie_api_key"
export NEON_DATABASE_URL="postgresql://user:password@ep-...neon.tech/neondb?sslmode=require"
```

When running locally through this project, `config/providers.env` is automatically read by the live-sync engine, so you do not need to manually export keys in every shell.

A blank template is included at [providers.env.example](/Users/josephdelallera/Documents/Playground%202/nba_model/config/providers.env.example).

If you prefer a different injury feed later, the fallback CSV/JSON settings are still available in [live_sync.json](/Users/josephdelallera/Documents/Playground%202/nba_model/config/live_sync.json).

### Neon + Git tie-in

The app supports optional Neon archival sync in the live loop:

1. Set `NEON_DATABASE_URL` in [providers.env](/Users/josephdelallera/Documents/Playground%202/nba_model/config/providers.env).
2. Enable `providers.neon_sync.enabled` in [live_sync.json](/Users/josephdelallera/Documents/Playground%202/nba_model/config/live_sync.json).
3. Run one sync cycle:

```bash
cd "/Users/josephdelallera/Documents/Playground 2"
./venv/bin/python "./nba_model/src/live_sync.py" --once
```

Git helper to bind remote, commit this app, and optionally push:

```bash
cd "/Users/josephdelallera/Documents/Playground 2/nba_model"
./scripts/git_sync_app.sh "https://github.com/<you>/<repo>.git"
# push immediately:
PUSH=1 ./scripts/git_sync_app.sh "https://github.com/<you>/<repo>.git"
```

## Friday live LLM + Agent Mode

The UI includes **Friday AI Assistant** with two modes:

- `local_fallback`: no OpenAI key configured
- `live`: real LLM responses from OpenAI

To enable live mode directly in the app:

1. Open the Friday card in the UI.
2. Paste your OpenAI API key into `OpenAI API Key`.
3. Click `Connect Live LLM`.

Friday stores runtime key/model settings at:

- [assistant_runtime.json](/Users/josephdelallera/Documents/Playground%202/nba_model/config/assistant_runtime.json)

Friday also supports **Agent Mode** from chat (toggle in UI) and can execute app actions on your direction, including live sync start/stop, sync now, in-game refresh, daily refresh pipeline, train, predict, recheck, benchmark runs, and PrizePicks edge generation.

## Phase 2: Auth, Paywall, Billing

The app now includes:

- account registration/login/logout with secure session cookies
- SQLite-backed subscription state and usage metering
- optional paywall enforcement for premium actions
- Stripe checkout + billing portal endpoints
- Stripe webhook ingestion for subscription lifecycle updates

Database path:

- [commerce.db](/Users/josephdelallera/Documents/Playground%202/nba_model/data/commerce.db)

Environment variables for billing:

```bash
export APP_PUBLIC_URL="http://127.0.0.1:8010"
export PAYWALL_ENFORCEMENT="0"  # set to 1 to require Pro on protected endpoints
export STRIPE_SECRET_KEY="sk_live_or_test_..."
export STRIPE_WEBHOOK_SECRET="whsec_..."
export STRIPE_PRICE_ID_PRO_MONTHLY="price_..."
export STRIPE_PRICE_ID_PRO_YEARLY="price_..."
```

New API endpoints:

- `GET /api/account/status`
- `POST /api/account/register`
- `POST /api/account/login`
- `POST /api/account/logout`
- `POST /api/account/checkout-session`
- `POST /api/account/portal-session`
- `POST /api/stripe/webhook`

## Run the UI

```bash
cd "/Users/josephdelallera/Documents/Playground 2"
./venv/bin/python "./nba_model/app.py"
```

Then open the URL printed by the launcher; by default it prefers `8010` and will auto-increment if already occupied.
If you want to force a specific port, set `PORT`, e.g.:

```bash
cd "/Users/josephdelallera/Documents/Playground 2"
PORT=8001 ./nba_model/scripts/start_app.sh
```

If auto-selected, this usually resolves to [http://127.0.0.1:8010](http://127.0.0.1:8010) unless another service is already bound.

The UI will start the live-sync background worker automatically by default. You can also control it from the browser with `Start Live Sync`, `Stop`, and `Sync Now`.
By default, projections are auto-refreshed every 10 seconds and the model runs a frequent optimization loop from the live-sync config.
The board header includes a built-in UI auto-refresh countdown so you can see when the next 10-second refresh fires.
The board also includes a mode switcher (`Best of Both`, `Roto-Style Dense`, `Betr-Style Cards`), a top-edge rail, and a matchup game hub for faster slate review.
The board now includes an in-app **Competitive Gap Assessment** and **Monetization Readiness Roadmap** panel to track what is still blocking premium parity and paid launch readiness.

For a documented parity matrix and execution checklist, see:

- [rotowire_betr_gap_assessment_monetization.md](/Users/josephdelallera/Documents/Playground%202/nba_model/docs/rotowire_betr_gap_assessment_monetization.md)

## Strict Weekly Milestones

The project now has an enforceable weekly gate system for parity execution:

- Plan config: [strict_milestones_v1.json](/Users/josephdelallera/Documents/Playground%202/nba_model/config/strict_milestones_v1.json)
- Runner script: [run_strict_milestones.py](/Users/josephdelallera/Documents/Playground%202/nba_model/scripts/run_strict_milestones.py)
- Plan doc: [strict_milestone_plan_v1.md](/Users/josephdelallera/Documents/Playground%202/nba_model/docs/strict_milestone_plan_v1.md)
- Latest report: [strict_milestone_status.json](/Users/josephdelallera/Documents/Playground%202/nba_model/models/strict_milestone_status.json)

Run all milestone gates in strict order:

```bash
cd "/Users/josephdelallera/Documents/Playground 2/nba_model"
"/Users/josephdelallera/Documents/Playground 2/venv/bin/python" scripts/run_strict_milestones.py
```

The packaged downloadable archive is [nba_prediction_engine_app.zip](/Users/josephdelallera/Documents/Playground%202/nba_prediction_engine_app.zip).

## Start At Login On macOS

The project includes:

- [providers.env](/Users/josephdelallera/Documents/Playground%202/nba_model/config/providers.env)
- [start_app.sh](/Users/josephdelallera/Documents/Playground%202/nba_model/scripts/start_app.sh)
- [install_launch_agent.sh](/Users/josephdelallera/Documents/Playground%202/nba_model/scripts/install_launch_agent.sh)
- [stop_app.sh](/Users/josephdelallera/Documents/Playground%202/nba_model/scripts/stop_app.sh)
- [com.josephdelallera.nbapredictionengine.plist](/Users/josephdelallera/Documents/Playground%202/nba_model/macos/com.josephdelallera.nbapredictionengine.plist)

Fill in your API keys inside `providers.env`, then install the LaunchAgent:

```bash
cd "/Users/josephdelallera/Documents/Playground 2"
./nba_model/scripts/install_launch_agent.sh
```

The installer copies a runtime snapshot to `~/nba_prediction_engine_runtime` and installs the LaunchAgent from there. That avoids macOS background-process privacy restrictions on `Documents`.

The LaunchAgent uses `RunAtLoad` and `KeepAlive`, so the local UI and background sync loop restart automatically whenever you log into macOS.
Default launch URL: [http://127.0.0.1:8010](http://127.0.0.1:8010).

Everything runs fully in user space (no sudo/admin needed): `~/Library/LaunchAgents`, `~/nba_prediction_engine_runtime`, project-local `venv`, and project-local data/models folders.

To stop the LaunchAgent service:

```bash
"/Users/josephdelallera/Documents/Playground 2/nba_model/scripts/stop_app.sh"
```

## CLI

Train:

```bash
cd "/Users/josephdelallera/Documents/Playground 2"
./venv/bin/python "./nba_model/src/train.py"
```

Predict:

```bash
cd "/Users/josephdelallera/Documents/Playground 2"
./venv/bin/python "./nba_model/src/predict.py"
```

Run one live sync cycle manually:

```bash
cd "/Users/josephdelallera/Documents/Playground 2"
./venv/bin/python "./nba_model/src/live_sync.py" --once
```

## Deploy a public web snapshot

This repo can publish a Safari-openable hosted snapshot of the board (read-only mode):

1. Export the latest web snapshots:

```bash
cd "/Users/josephdelallera/Documents/Playground 2/nba_model"
../venv/bin/python "./scripts/export_web_snapshot.py"
```

2. Deploy with the Vercel fallback deploy script:

```bash
bash "/Users/josephdelallera/.codex/skills/vercel-deploy/scripts/deploy.sh" "/Users/josephdelallera/Documents/Playground 2/nba_model"
```

The deployed site uses packaged JSON snapshots from `ui/snapshot/` and automatically switches to read-only snapshot mode when live API endpoints are unavailable.
