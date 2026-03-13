from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import math
import mimetypes
import os
import re
import secrets
import sqlite3
import threading
import time
import uuid
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from email import policy
from email.parser import BytesParser
from urllib import error as urlerror
from urllib import request as urlrequest
from urllib.parse import parse_qs, urlencode, urlparse

import pandas as pd
import numpy as np

from src.benchmark import (
    DEFAULT_JOIN_PATH as DEFAULT_ROTOWIRE_BENCHMARK_JOIN_PATH,
    DEFAULT_REPORT_PATH as DEFAULT_ROTOWIRE_BENCHMARK_REPORT_PATH,
    DEFAULT_SNAPSHOT_PATH as DEFAULT_ROTOWIRE_BENCHMARK_SNAPSHOT_PATH,
    capture_rotowire_benchmark_snapshot,
    load_rotowire_benchmark_report as _load_rotowire_benchmark_report_raw,
    run_rotowire_benchmark,
)
from src.engine import (
    DEFAULT_ADAPTIVE_LEARNING_PROFILE_PATH,
    DEFAULT_BUNDLE_PATH,
    DEFAULT_CALIBRATION_PATH,
    DEFAULT_METRICS_PATH,
    DEFAULT_PREDICTION_MISS_LOG_PATH,
    DEFAULT_PREDICTIONS_PATH,
    DEFAULT_RECHECK_PATH,
    app_status,
    recheck_past_predictions,
    predict_engine,
    train_engine,
)
from src.features import (
    DEFAULT_CONTEXT_UPDATES_PATH,
    DEFAULT_DATA_PATH,
    DEFAULT_MPLCONFIGDIR,
    DEFAULT_PROVIDER_CONTEXT_PATH,
    DEFAULT_PRIZEPICKS_LINES_PATH,
    DEFAULT_SEASON_PRIORS_PATH,
    DEFAULT_TRAINING_UPLOAD_PATH,
    DEFAULT_UPCOMING_PATH,
    SCHEMA_GUIDE,
)
from src.importers import (
    ensure_support_files,
    import_historical_bytes,
    import_historical_text,
    import_prizepicks_lines_bytes,
    import_prizepicks_lines_text,
    import_season_priors_bytes,
    import_season_priors_text,
)
from src.data_pipeline import describe_data_contracts, pipeline_status, run_contract_drift_audit
from src.live_sync import (
    DEFAULT_GAME_NOTES_DAILY_PATH,
    DEFAULT_LIVE_CONFIG_PATH,
    DEFAULT_LIVE_GAME_ACTIONS_PATH,
    DEFAULT_LIVE_STATE_PATH,
    DEFAULT_POSTGAME_REVIEWS_PATH,
    LiveSyncManager,
    load_live_config,
    save_live_config,
)
from src.live_sync import BOXSCORE_URL_TEMPLATE, SCHEDULE_URL, SCOREBOARD_URL
from src.player_matching import normalize_player_name, normalize_team_code
from src.prizepicks import (
    DEFAULT_PRIZEPICKS_EDGES_PATH,
    EDGE_THRESHOLDS,
    MARKET_TO_COLUMN,
    generate_prizepicks_edges,
)


DEFAULT_MPLCONFIGDIR.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("LOKY_MAX_CPU_COUNT", "1")
os.environ.setdefault("MPLCONFIGDIR", str(DEFAULT_MPLCONFIGDIR))

APP_ROOT = Path(__file__).resolve().parent
UI_DIR = APP_ROOT / "ui"
APP_ARCHIVE_PATH = APP_ROOT.parent / "nba_prediction_engine_app.zip"
LIVE_SYNC_MANAGER = LiveSyncManager()
ensure_support_files()
BACKGROUND_JOB_LOCK = threading.Lock()
BACKGROUND_JOBS: dict[str, dict] = {}
BACKGROUND_JOB_THREADS: dict[str, threading.Thread] = {}
BACKGROUND_JOB_HISTORY_LIMIT = 40
BACKGROUND_JOB_GROUPS = {
    "live_sync": "live_manager",
    "in_game_refresh": "live_manager",
    "daily_refresh": "live_manager",
    "train": "model_jobs",
    "recheck": "model_jobs",
}
BACKGROUND_JOB_MAX_RUNTIME_SECONDS = {
    # Live sync can take several minutes when provider retries/backfill paths are active.
    "live_sync": 720,
    "in_game_refresh": 300,
    "daily_refresh": 1200,
    "train": 1200,
    "recheck": 1200,
}
DEFAULT_BACKGROUND_JOB_MAX_RUNTIME_SECONDS = 1800
STATUS_CACHE_TTL_SECONDS = 4.0
STATUS_CACHE_LOCK = threading.Lock()
STATUS_CACHE: dict[str, object] = {
    "generated_at_ts": 0.0,
    "include_previews": True,
    "payload": None,
}
RECENT_FORM_CACHE_LOCK = threading.Lock()
RECENT_FORM_CACHE: dict[tuple[str, int, int, str], tuple[pd.DataFrame, pd.DataFrame]] = {}
_PREDICTION_FRAME_CACHE: dict[tuple[int, int, int], pd.DataFrame] = {}
ASSISTANT_NAME = "Friday"
OPENAI_CHAT_COMPLETIONS_URL = "https://api.openai.com/v1/chat/completions"
DEFAULT_OPENAI_MODEL = "gpt-4o-mini"
ASSISTANT_RUNTIME_CONFIG_PATH = APP_ROOT / "config" / "assistant_runtime.json"
ASSISTANT_AGENT_ACTIONS = {
    "start_live_sync",
    "stop_live_sync",
    "sync_live_now",
    "sync_in_game_now",
    "run_daily_refresh",
    "train_models",
    "run_predictions",
    "run_recheck",
    "capture_benchmark",
    "run_benchmark",
    "generate_prizepicks_edges",
}

COMMERCE_DB_PATH = APP_ROOT / "data" / "commerce.db"
COMMERCE_SCHEMA_READY = False
SESSION_COOKIE_NAME = "nba_session"
SESSION_TTL_SECONDS = 60 * 60 * 24 * 30
PASSWORD_PBKDF2_ITERATIONS = 320_000
PAYWALL_ENFORCEMENT = str(os.getenv("PAYWALL_ENFORCEMENT", "0") or "").strip().lower() in {"1", "true", "yes", "on"}
APP_PUBLIC_URL = str(os.getenv("APP_PUBLIC_URL", "http://127.0.0.1:8010") or "http://127.0.0.1:8010").strip().rstrip("/")
STRIPE_SECRET_KEY = str(os.getenv("STRIPE_SECRET_KEY", "") or "").strip()
STRIPE_WEBHOOK_SECRET = str(os.getenv("STRIPE_WEBHOOK_SECRET", "") or "").strip()
STRIPE_PRICE_ID_PRO_MONTHLY = str(os.getenv("STRIPE_PRICE_ID_PRO_MONTHLY", "") or "").strip()
STRIPE_PRICE_ID_PRO_YEARLY = str(os.getenv("STRIPE_PRICE_ID_PRO_YEARLY", "") or "").strip()
STRIPE_API_BASE = "https://api.stripe.com/v1"
FREE_PLAN_CODE = "free"
PRO_PLAN_CODE = "pro"
PLAN_FEATURES = {
    FREE_PLAN_CODE: {
        "name": "Free",
        "limits": {
            "assistant_messages": 80,
            "prediction_runs": 40,
            "benchmark_runs": 6,
            "daily_refresh_runs": 8,
            "live_sync_actions": 200,
        },
    },
    PRO_PLAN_CODE: {
        "name": "Pro",
        "limits": {},
    },
}
USAGE_EVENT_KEYS = {
    "assistant_messages",
    "prediction_runs",
    "benchmark_runs",
    "daily_refresh_runs",
    "live_sync_actions",
}

UNAVAILABLE_STATUS_PATTERN = r"(?:out|ofs|suspend|suspended|inactive|g[ -]?league|two[- ]?way|dnp)"
NO_BET_MIN_HISTORY_GAMES = 8
NO_BET_POINTS_STD_LAST_10 = 8.0
NO_BET_MINUTES_STD_LAST_10 = 8.0
NO_BET_STARTER_RATE_LAST_10 = 0.35
NO_BET_MINUTES_CONFIDENCE = 0.30
NO_BET_MINUTES_PROJECTION_ERROR = 8.0
NO_BET_INJURY_RISK = 0.45
NO_BET_NEWS_RISK = 0.92
NO_BET_MIN_STARTER_PROBABILITY = 0.52
NO_BET_MIN_LINEUP_STATUS_CONFIDENCE = 0.55
NO_BET_MIN_PREGAME_LOCK_CONFIDENCE = 0.5
NO_BET_MAX_PROJECTION_ERROR_PCT = 32.0
NO_BET_MIN_PREGAME_ANCHOR_STRENGTH = 1.0
NO_BET_MIN_LINE_BOOKS_COUNT = 2.0
NO_BET_MAX_LINE_SNAPSHOT_AGE_MINUTES = 420.0
NO_BET_MIN_EDGE_TO_UNCERTAINTY = 1.5
NO_BET_SCORE_THRESHOLD = 2.0
NO_BET_REASON_LABELS = {
    "insufficient_history": "insufficient_history",
    "points_volatility": "points_volatility",
    "minutes_volatility": "minutes_volatility",
    "role_instability": "role_instability",
    "lineup_uncertainty": "lineup_uncertainty",
    "low_minutes_confidence": "low_minutes_confidence",
    "minutes_projection_unstable": "minutes_projection_unstable",
    "projection_uncertainty": "projection_uncertainty",
    "weak_market_anchor": "weak_market_anchor",
    "stale_market_snapshot": "stale_market_snapshot",
    "stale_context_data": "stale_context_data",
    "injury_risk": "injury_risk",
    "news_risk": "news_risk",
    "model_low_confidence": "model_low_confidence",
    "quality_gate_blocked": "quality_gate_blocked",
    "unavailable": "unavailable",
}
NO_BET_REASON_WEIGHTS = {
    "insufficient_history": 1.0,
    "points_volatility": 1.2,
    "minutes_volatility": 1.0,
    "role_instability": 0.8,
    "lineup_uncertainty": 0.9,
    "low_minutes_confidence": 0.8,
    "minutes_projection_unstable": 1.0,
    "projection_uncertainty": 1.1,
    "weak_market_anchor": 0.8,
    "stale_market_snapshot": 0.6,
    "stale_context_data": 0.9,
    "injury_risk": 1.3,
    "news_risk": 0.7,
    "model_low_confidence": 0.7,
    "quality_gate_blocked": 3.0,
    "unavailable": 5.0,
}
NO_BET_HARD_REASONS = {
    "unavailable",
    "quality_gate_blocked",
    "injury_risk",
}
NO_BET_SOFT_REASONS = {
    "role_instability",
    "lineup_uncertainty",
    "low_minutes_confidence",
    "minutes_projection_unstable",
    "projection_uncertainty",
    "stale_context_data",
    "stale_market_snapshot",
    "weak_market_anchor",
    "news_risk",
}
NO_BET_SOFT_SCORE_THRESHOLD = 3.2
NO_BET_ACTIONABLE_FLOOR_COUNT = 14
NO_BET_ACTIONABLE_FLOOR_RATIO = 0.08
NO_BET_MIN_ACTIONABLE_STARTERS = 4
NO_BET_SOFT_RELIEF_MIN_STARTER_PROBABILITY = 0.55
NO_BET_SOFT_RELIEF_MIN_EXPECTED_MINUTES = 20.0
NO_BET_SOFT_RELIEF_MIN_MINUTES_CONFIDENCE = 0.45
NO_BET_SOFT_RELIEF_MIN_LINEUP_CONFIDENCE = 0.45
NO_BET_SOFT_RELIEF_MIN_PREGAME_LOCK_CONFIDENCE = 0.62
NO_BET_PROJECTION_ERROR_GRACE = 8.0
RECENT_FORM_COLUMNS = [
    "recent_games_count_last_10",
    "points_avg_last_10",
    "rebounds_avg_last_10",
    "assists_avg_last_10",
    "minutes_avg_last_10",
    "points_std_last_10",
    "rebounds_std_last_10",
    "assists_std_last_10",
    "minutes_std_last_10",
    "starter_rate_last_10",
]
MARKET_TO_UNCERTAINTY_COLUMN = {
    "points": "pregame_anchor_uncertainty_points",
    "rebounds": "pregame_anchor_uncertainty_rebounds",
    "assists": "pregame_anchor_uncertainty_assists",
    "pra": "pregame_anchor_uncertainty_pra",
}
MARKET_TO_LINE_STDDEV_COLUMN = {
    "points": "line_points_stddev",
    "rebounds": "line_rebounds_stddev",
    "assists": "line_assists_stddev",
    "pra": "line_pra_stddev",
}
CONFIDENCE_TO_ERROR_PCT = {
    "high_confidence": 10.0,
    "medium_confidence": 17.0,
    "low_confidence": 26.0,
}
PLAYSTYLE_SOURCE_URLS = [
    {
        "name": "NBA Stats: LeagueDashPlayerStats",
        "url": "https://stats.nba.com/stats/leaguedashplayerstats",
        "purpose": "baseline player per-game rate inputs",
    },
    {
        "name": "NBA Stats: LeagueDashPtStats",
        "url": "https://stats.nba.com/stats/leaguedashptstats",
        "purpose": "touch/drive/catch-shoot/pull-up playtype context",
    },
    {
        "name": "NBA Stats: LeagueDashPlayerShotLocations",
        "url": "https://stats.nba.com/stats/leaguedashplayershotlocations",
        "purpose": "shot-zone attempt mix (rim/mid/three) context",
    },
    {
        "name": "NBA Live Scoreboard Feed",
        "url": SCOREBOARD_URL,
        "purpose": "live game state and in-game stat updates",
    },
    {
        "name": "NBA Schedule Feed",
        "url": SCHEDULE_URL,
        "purpose": "future slate generation and game keying",
    },
    {
        "name": "The Odds API Docs",
        "url": "https://the-odds-api.com/liveapi/guides/v4/",
        "purpose": "pregame/market anchor and line-consensus context",
    },
    {
        "name": "BALDONTLIE API Docs",
        "url": "https://docs.balldontlie.io/",
        "purpose": "injury + player directory enrichment",
    },
    {
        "name": "NBA Official Injury Report",
        "url": "https://official.nba.com/nba-injury-report-2025-26-season/",
        "purpose": "official injury status confirmation",
    },
]

V05_RUBRIC_VERSION = "v0.5"
V05_RUBRIC_DIMENSION_WEIGHTS = {
    "starter_probability_tightening": 0.16,
    "pregame_minutes_certainty": 0.16,
    "market_calibration": 0.18,
    "coverage_depth": 0.14,
    "shot_style_effect": 0.10,
    "teammate_dynamic_effect": 0.10,
    "home_away_probability_boost": 0.08,
    "runtime_reliability": 0.08,
}
V05_RUBRIC_DEFAULT_DATA_TARGETS = [
    {
        "data": "lock-window starter confirmations (T-90/T-30/T-5)",
        "why": "Tightens starter probability and minutes certainty before tip.",
        "source_type": "lineup/provider feed",
    },
    {
        "data": "expected minutes with confidence and cap reason",
        "why": "Prevents under/over projection drift for rotation-volatile players.",
        "source_type": "minutes provider + team reports",
    },
    {
        "data": "open-to-close line movement + multi-book consensus",
        "why": "Improves per-market calibration and uncertainty estimation.",
        "source_type": "odds/props provider",
    },
    {
        "data": "teammate on/off availability deltas",
        "why": "Improves usage redistribution and teammate dynamics.",
        "source_type": "lineup + on/off context feed",
    },
    {
        "data": "shot profile and playstyle refresh",
        "why": "Improves shot-style matchup adjustments by market.",
        "source_type": "player shot-location/playtype feed",
    },
    {
        "data": "home/away and travel fatigue context",
        "why": "Stabilizes home-court and away-game boost/downgrade logic.",
        "source_type": "schedule + travel context",
    },
]


def _safe_float(value: object, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float, np.integer, np.floating)):
        parsed = float(value)
        if not math.isfinite(parsed):
            return default
        return parsed
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return default
        try:
            parsed = float(stripped)
        except Exception:
            return default
        if not math.isfinite(parsed):
            return default
        return parsed
    try:
        parsed = float(value)
    except Exception:
        return default
    if not math.isfinite(parsed):
        return default
    return parsed


def _safe_int(value: object, default: int = 0) -> int:
    parsed = _safe_float(value, float("nan"))
    if not math.isfinite(parsed):
        return default
    try:
        return int(parsed)
    except Exception:
        return default


def _normalize_rotowire_benchmark_report_shape(report: dict | None) -> dict | None:
    if not isinstance(report, dict):
        return report

    normalized = dict(report)
    if isinstance(normalized.get("overall"), dict):
        return normalized

    hit_rate = _safe_float(normalized.get("hit_rate"), 0.0)
    avg_clv = _safe_float(normalized.get("avg_clv"), 0.0)
    mae_delta = _safe_float(normalized.get("mae_delta_vs_rotowire_line"), 0.0)
    projection_mae = _safe_float(normalized.get("model_projection_mae"), 0.0)
    rows = _safe_int(normalized.get("rows_evaluated"), 0)
    actionable_rows = _safe_int(normalized.get("rows_with_actionable_picks"), 0)

    calibration_payload = normalized.get("calibration")
    calibration_list = calibration_payload if isinstance(calibration_payload, list) else []
    lowest_bucket_hit_rate = 0.0
    if calibration_list:
        bucket_rates = [
            _safe_float(item.get("hit_rate"), 0.0)
            for item in calibration_list
            if isinstance(item, dict)
        ]
        if bucket_rates:
            lowest_bucket_hit_rate = min(bucket_rates)
    confidence_mae = abs(hit_rate - 0.5) if hit_rate > 0 else 0.0

    normalized["overall"] = {
        "rows": rows,
        "hit_rate": hit_rate,
        "projection_hit_rate": hit_rate,
        "market_hit_rate": 0.5,
        "clv_mean": avg_clv,
        "projection_error_minus_line_error": max(0.0, -mae_delta),
        "mean_abs_error": projection_mae,
        "mean_abs_pct_error": 0.0,
        "confidence_mae": confidence_mae,
    }
    normalized["calibration"] = {
        "buckets": calibration_list,
        "confidence_mae": confidence_mae,
        "lowest_bucket_hit_rate": lowest_bucket_hit_rate,
    }
    normalized["rows_with_actionable_picks"] = actionable_rows
    return normalized


def load_rotowire_benchmark_report(path: Path = DEFAULT_ROTOWIRE_BENCHMARK_REPORT_PATH) -> dict | None:
    return _normalize_rotowire_benchmark_report_shape(_load_rotowire_benchmark_report_raw(path=path))


def _edge_uncertainty_band_for_market(
    row: pd.Series,
    market: str,
    projection_value: float,
    base_threshold: float,
) -> float:
    projection_error_pct = _safe_float(
        row.get("projection_error_pct_estimate"),
        _safe_float(
            row.get("error_pct_estimate"),
            CONFIDENCE_TO_ERROR_PCT.get(str(row.get("confidence_flag", "")).lower(), 26.0),
        ),
    )
    model_uncertainty = abs(float(projection_value)) * max(0.0, projection_error_pct) / 100.0
    anchor_uncertainty = _safe_float(row.get(MARKET_TO_UNCERTAINTY_COLUMN.get(market, "")), 0.0)
    line_stddev = _safe_float(row.get(MARKET_TO_LINE_STDDEV_COLUMN.get(market, "")), 0.0)
    blended = (model_uncertainty * 0.65) + (anchor_uncertainty * 0.55) + (line_stddev * 0.4)
    return float(max(float(base_threshold), blended))


def _json_safe(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, (int, str, bool)):
        return value

    if hasattr(value, "item"):
        try:
            return _json_safe(value.item())
        except Exception:  # noqa: BLE001
            pass

    try:
        if pd.isna(value):
            return None
    except Exception:  # noqa: BLE001
        pass
    return value


def _now_ts() -> int:
    return int(time.time())


def _month_start_ts(ts: int | None = None) -> int:
    base = datetime.fromtimestamp(ts or _now_ts())
    month_start = datetime(base.year, base.month, 1)
    return int(month_start.timestamp())


def _commerce_connection() -> sqlite3.Connection:
    COMMERCE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(COMMERCE_DB_PATH), timeout=8.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _normalize_email(email: str) -> str:
    return str(email or "").strip().lower()


def _validate_email(email: str) -> bool:
    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email))


def _hash_password(password: str, salt_bytes: bytes | None = None, iterations: int = PASSWORD_PBKDF2_ITERATIONS) -> tuple[str, str]:
    salt = salt_bytes or secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return base64.b64encode(salt).decode("ascii"), base64.b64encode(digest).decode("ascii")


def _verify_password(password: str, salt_b64: str, digest_b64: str, iterations: int) -> bool:
    try:
        salt = base64.b64decode(salt_b64.encode("ascii"))
        expected = base64.b64decode(digest_b64.encode("ascii"))
    except Exception:  # noqa: BLE001
        return False
    candidate = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, max(1, int(iterations)))
    return hmac.compare_digest(candidate, expected)


def _token_hash(token: str) -> str:
    return hashlib.sha256(str(token or "").encode("utf-8")).hexdigest()


def _stripe_headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {STRIPE_SECRET_KEY}",
        "Content-Type": "application/x-www-form-urlencoded",
    }


def _stripe_request(path: str, form_data: dict[str, object]) -> dict:
    if not STRIPE_SECRET_KEY:
        raise ValueError("Stripe is not configured. Set STRIPE_SECRET_KEY.")
    endpoint = f"{STRIPE_API_BASE}{path}"
    payload = urlencode({key: value for key, value in form_data.items() if value is not None}).encode("utf-8")
    request = urlrequest.Request(endpoint, data=payload, headers=_stripe_headers(), method="POST")
    try:
        with urlrequest.urlopen(request, timeout=20) as response:
            body = response.read().decode("utf-8")
    except urlerror.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise ValueError(f"Stripe request failed ({exc.code}): {detail}") from exc
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"Stripe request failed: {exc}") from exc
    try:
        return json.loads(body or "{}")
    except json.JSONDecodeError as exc:  # pragma: no cover
        raise ValueError("Stripe returned invalid JSON.") from exc


def _verify_stripe_webhook_signature(payload: bytes, signature_header: str) -> bool:
    if not STRIPE_WEBHOOK_SECRET:
        return False
    if not signature_header:
        return False
    chunks = [chunk.strip() for chunk in str(signature_header).split(",") if chunk.strip()]
    timestamp = ""
    signatures: list[str] = []
    for chunk in chunks:
        if "=" not in chunk:
            continue
        key, value = chunk.split("=", 1)
        if key == "t":
            timestamp = value
        elif key == "v1":
            signatures.append(value)
    if not timestamp or not signatures:
        return False
    try:
        ts_int = int(timestamp)
    except ValueError:
        return False
    if abs(_now_ts() - ts_int) > 300:
        return False
    signed_payload = f"{timestamp}.{payload.decode('utf-8')}".encode("utf-8")
    expected = hmac.new(STRIPE_WEBHOOK_SECRET.encode("utf-8"), signed_payload, hashlib.sha256).hexdigest()
    return any(hmac.compare_digest(expected, signature) for signature in signatures)


def _ensure_commerce_schema() -> None:
    global COMMERCE_SCHEMA_READY  # noqa: PLW0603
    if COMMERCE_SCHEMA_READY:
        return
    with _commerce_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                password_salt TEXT NOT NULL,
                password_iter INTEGER NOT NULL,
                created_at_ts INTEGER NOT NULL,
                last_login_at_ts INTEGER
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                token_hash TEXT NOT NULL UNIQUE,
                created_at_ts INTEGER NOT NULL,
                expires_at_ts INTEGER NOT NULL,
                last_seen_at_ts INTEGER NOT NULL,
                ip_address TEXT,
                user_agent TEXT,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL UNIQUE,
                plan_code TEXT NOT NULL DEFAULT 'free',
                status TEXT NOT NULL DEFAULT 'inactive',
                stripe_customer_id TEXT,
                stripe_subscription_id TEXT,
                stripe_price_id TEXT,
                current_period_end_ts INTEGER,
                cancel_at_period_end INTEGER NOT NULL DEFAULT 0,
                updated_at_ts INTEGER NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS usage_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                event_key TEXT NOT NULL,
                quantity INTEGER NOT NULL DEFAULT 1,
                metadata_json TEXT,
                created_at_ts INTEGER NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stripe_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                event_type TEXT NOT NULL,
                received_at_ts INTEGER NOT NULL,
                payload_json TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_usage_user_event_ts
            ON usage_events (user_id, event_key, created_at_ts)
            """
        )
        conn.commit()
    COMMERCE_SCHEMA_READY = True


def _ensure_subscription_row(conn: sqlite3.Connection, user_id: int) -> None:
    row = conn.execute(
        "SELECT user_id FROM subscriptions WHERE user_id = ?",
        (int(user_id),),
    ).fetchone()
    if row:
        return
    conn.execute(
        """
        INSERT INTO subscriptions (user_id, plan_code, status, updated_at_ts)
        VALUES (?, ?, ?, ?)
        """,
        (int(user_id), FREE_PLAN_CODE, "inactive", _now_ts()),
    )


def _resolve_plan_from_subscription(subscription_row: sqlite3.Row | None) -> str:
    if subscription_row is None:
        return FREE_PLAN_CODE
    plan_code = str(subscription_row["plan_code"] or FREE_PLAN_CODE).strip().lower()
    if plan_code not in PLAN_FEATURES:
        plan_code = FREE_PLAN_CODE
    status = str(subscription_row["status"] or "").strip().lower()
    current_period_end_ts = _safe_int(subscription_row["current_period_end_ts"], 0)
    if plan_code == PRO_PLAN_CODE:
        if status in {"active", "trialing", "past_due"}:
            if current_period_end_ts <= 0 or current_period_end_ts >= _now_ts():
                return PRO_PLAN_CODE
    return FREE_PLAN_CODE


def _usage_summary_for_user(conn: sqlite3.Connection, user_id: int, plan_code: str) -> dict:
    period_start = _month_start_ts()
    rows = conn.execute(
        """
        SELECT event_key, SUM(quantity) AS total
        FROM usage_events
        WHERE user_id = ? AND created_at_ts >= ?
        GROUP BY event_key
        """,
        (int(user_id), period_start),
    ).fetchall()
    used_by_key = {str(row["event_key"]): int(row["total"] or 0) for row in rows}
    limits = dict(PLAN_FEATURES.get(plan_code, PLAN_FEATURES[FREE_PLAN_CODE]).get("limits", {}))
    usage = {}
    for event_key in sorted(USAGE_EVENT_KEYS):
        used = used_by_key.get(event_key, 0)
        limit = limits.get(event_key)
        remaining = None if limit is None else max(0, int(limit) - used)
        usage[event_key] = {
            "used": used,
            "limit": limit,
            "remaining": remaining,
        }
    return {
        "period_start_ts": period_start,
        "period_start": datetime.fromtimestamp(period_start).isoformat(),
        "events": usage,
    }


def _record_usage_event(
    conn: sqlite3.Connection,
    user_id: int,
    event_key: str,
    quantity: int = 1,
    metadata: dict | None = None,
) -> None:
    normalized_key = str(event_key or "").strip()
    if normalized_key not in USAGE_EVENT_KEYS:
        return
    safe_quantity = max(1, int(quantity or 1))
    metadata_json = json.dumps(_json_safe(metadata or {}), ensure_ascii=False)
    conn.execute(
        """
        INSERT INTO usage_events (user_id, event_key, quantity, metadata_json, created_at_ts)
        VALUES (?, ?, ?, ?, ?)
        """,
        (int(user_id), normalized_key, safe_quantity, metadata_json, _now_ts()),
    )


def _enforce_usage_limit(conn: sqlite3.Connection, user_id: int, plan_code: str, event_key: str, quantity: int = 1) -> None:
    normalized_key = str(event_key or "").strip()
    if normalized_key not in USAGE_EVENT_KEYS:
        return
    limits = dict(PLAN_FEATURES.get(plan_code, PLAN_FEATURES[FREE_PLAN_CODE]).get("limits", {}))
    limit = limits.get(normalized_key)
    if limit is None:
        return
    period_start = _month_start_ts()
    used_row = conn.execute(
        """
        SELECT COALESCE(SUM(quantity), 0) AS total
        FROM usage_events
        WHERE user_id = ? AND event_key = ? AND created_at_ts >= ?
        """,
        (int(user_id), normalized_key, period_start),
    ).fetchone()
    used = int((used_row["total"] if used_row else 0) or 0)
    if used + max(1, int(quantity or 1)) > int(limit):
        raise PermissionError(
            f"Monthly usage limit reached for '{normalized_key}'. "
            "Upgrade to Pro to remove this limit."
        )


def _serialize_account_row(user_row: sqlite3.Row, subscription_row: sqlite3.Row | None, usage: dict | None = None) -> dict:
    plan_code = _resolve_plan_from_subscription(subscription_row)
    subscription_payload = {
        "plan_code": plan_code,
        "plan_name": PLAN_FEATURES.get(plan_code, PLAN_FEATURES[FREE_PLAN_CODE])["name"],
        "status": str(subscription_row["status"]) if subscription_row is not None else "inactive",
        "stripe_customer_id": str(subscription_row["stripe_customer_id"]) if subscription_row is not None and subscription_row["stripe_customer_id"] else None,
        "stripe_subscription_id": str(subscription_row["stripe_subscription_id"]) if subscription_row is not None and subscription_row["stripe_subscription_id"] else None,
        "stripe_price_id": str(subscription_row["stripe_price_id"]) if subscription_row is not None and subscription_row["stripe_price_id"] else None,
        "current_period_end_ts": _safe_int(subscription_row["current_period_end_ts"], 0) if subscription_row is not None else 0,
        "cancel_at_period_end": bool(_safe_int(subscription_row["cancel_at_period_end"], 0)) if subscription_row is not None else False,
        "updated_at_ts": _safe_int(subscription_row["updated_at_ts"], 0) if subscription_row is not None else 0,
    }
    return {
        "id": _safe_int(user_row["id"], 0),
        "email": str(user_row["email"] or ""),
        "created_at_ts": _safe_int(user_row["created_at_ts"], 0),
        "last_login_at_ts": _safe_int(user_row["last_login_at_ts"], 0),
        "subscription": subscription_payload,
        "usage": usage or {"events": {}, "period_start_ts": 0, "period_start": None},
    }


def _upsert_subscription(
    conn: sqlite3.Connection,
    user_id: int,
    plan_code: str,
    status: str,
    stripe_customer_id: str | None = None,
    stripe_subscription_id: str | None = None,
    stripe_price_id: str | None = None,
    current_period_end_ts: int | None = None,
    cancel_at_period_end: bool | None = None,
) -> None:
    plan = str(plan_code or FREE_PLAN_CODE).strip().lower()
    if plan not in PLAN_FEATURES:
        plan = FREE_PLAN_CODE
    existing = conn.execute("SELECT * FROM subscriptions WHERE user_id = ?", (int(user_id),)).fetchone()
    payload = {
        "plan_code": plan,
        "status": str(status or "inactive"),
        "stripe_customer_id": stripe_customer_id,
        "stripe_subscription_id": stripe_subscription_id,
        "stripe_price_id": stripe_price_id,
        "current_period_end_ts": current_period_end_ts,
        "cancel_at_period_end": 1 if cancel_at_period_end else 0,
        "updated_at_ts": _now_ts(),
    }
    if existing is None:
        conn.execute(
            """
            INSERT INTO subscriptions (
              user_id, plan_code, status, stripe_customer_id, stripe_subscription_id, stripe_price_id,
              current_period_end_ts, cancel_at_period_end, updated_at_ts
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(user_id),
                payload["plan_code"],
                payload["status"],
                payload["stripe_customer_id"],
                payload["stripe_subscription_id"],
                payload["stripe_price_id"],
                payload["current_period_end_ts"],
                payload["cancel_at_period_end"],
                payload["updated_at_ts"],
            ),
        )
    else:
        merged = dict(existing)
        for key, value in payload.items():
            if value is not None:
                merged[key] = value
        conn.execute(
            """
            UPDATE subscriptions
            SET plan_code = ?, status = ?, stripe_customer_id = ?, stripe_subscription_id = ?, stripe_price_id = ?,
                current_period_end_ts = ?, cancel_at_period_end = ?, updated_at_ts = ?
            WHERE user_id = ?
            """,
            (
                merged.get("plan_code"),
                merged.get("status"),
                merged.get("stripe_customer_id"),
                merged.get("stripe_subscription_id"),
                merged.get("stripe_price_id"),
                merged.get("current_period_end_ts"),
                _safe_int(merged.get("cancel_at_period_end"), 0),
                _safe_int(merged.get("updated_at_ts"), _now_ts()),
                int(user_id),
            ),
        )


def _job_now_iso() -> str:
    return datetime.now().isoformat()


def _parse_iso_timestamp_seconds(value: object) -> float | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    parsed = pd.to_datetime(pd.Series([raw]), errors="coerce").iloc[0]
    if pd.isna(parsed):
        return None
    try:
        return float(parsed.timestamp())
    except Exception:
        return None


def _background_job_runtime_seconds(job: dict) -> float | None:
    launch_started = float(job.get("launch_started_monotonic") or 0.0)
    if launch_started > 0.0:
        return max(0.0, time.monotonic() - launch_started)
    started_at_seconds = _parse_iso_timestamp_seconds(job.get("started_at"))
    if started_at_seconds is None:
        return None
    return max(0.0, time.time() - started_at_seconds)


def _background_job_timeout_seconds(job: dict) -> float:
    explicit = _safe_float(job.get("max_runtime_seconds"), 0.0)
    if explicit > 0:
        return explicit
    name = str(job.get("name") or "").strip().lower()
    if name and name in BACKGROUND_JOB_MAX_RUNTIME_SECONDS:
        return float(BACKGROUND_JOB_MAX_RUNTIME_SECONDS[name])
    group_name = str(job.get("group") or "").strip().lower()
    if group_name and group_name in BACKGROUND_JOB_MAX_RUNTIME_SECONDS:
        return float(BACKGROUND_JOB_MAX_RUNTIME_SECONDS[group_name])
    return float(DEFAULT_BACKGROUND_JOB_MAX_RUNTIME_SECONDS)


def _prune_background_jobs_locked() -> None:
    if len(BACKGROUND_JOBS) <= BACKGROUND_JOB_HISTORY_LIMIT:
        return
    finished = [
        (job_id, job)
        for job_id, job in BACKGROUND_JOBS.items()
        if job.get("status") in {"completed", "failed"}
    ]
    finished.sort(key=lambda item: str(item[1].get("finished_at") or item[1].get("started_at") or ""))
    removable = max(0, len(BACKGROUND_JOBS) - BACKGROUND_JOB_HISTORY_LIMIT)
    for job_id, _job in finished[:removable]:
        BACKGROUND_JOBS.pop(job_id, None)
        BACKGROUND_JOB_THREADS.pop(job_id, None)


def _reconcile_running_jobs_locked() -> None:
    for job_id, job in list(BACKGROUND_JOBS.items()):
        if job.get("status") != "running":
            continue
        thread = BACKGROUND_JOB_THREADS.get(job_id)
        runtime_seconds = _background_job_runtime_seconds(job)
        timeout_seconds = _background_job_timeout_seconds(job)
        if thread is not None and thread.is_alive():
            if runtime_seconds is not None and runtime_seconds > timeout_seconds:
                job["status"] = "failed"
                job["finished_at"] = _job_now_iso()
                job["error"] = job.get("error") or f"Background worker exceeded timeout ({int(timeout_seconds)}s)."
                job["timed_out"] = True
                job["runtime_seconds"] = round(runtime_seconds, 3)
                BACKGROUND_JOB_THREADS.pop(job_id, None)
                continue
            continue
        launch_started = float(job.get("launch_started_monotonic") or 0.0)
        if launch_started > 0.0 and (time.monotonic() - launch_started) < 2.0:
            # Give freshly created threads a brief startup window before treating as a crash.
            continue
        job["status"] = "failed"
        job["finished_at"] = _job_now_iso()
        job["error"] = job.get("error") or "Background worker stopped unexpectedly."
        if runtime_seconds is not None:
            job["runtime_seconds"] = round(runtime_seconds, 3)
        BACKGROUND_JOB_THREADS.pop(job_id, None)


def _recent_background_jobs(limit: int = 15) -> list[dict]:
    with BACKGROUND_JOB_LOCK:
        _reconcile_running_jobs_locked()
        jobs = [dict(entry) for entry in BACKGROUND_JOBS.values()]
    jobs.sort(key=lambda entry: str(entry.get("started_at") or entry.get("created_at") or ""), reverse=True)
    return jobs[:limit]


def start_background_job(name: str, target, group: str | None = None) -> dict:
    resolved_group = str(group or BACKGROUND_JOB_GROUPS.get(name) or name)
    with BACKGROUND_JOB_LOCK:
        _reconcile_running_jobs_locked()
        for existing in BACKGROUND_JOBS.values():
            if existing.get("status") != "running":
                continue
            if existing.get("name") == name:
                return {"status": "already_running", "job": dict(existing)}
            if str(existing.get("group") or existing.get("name")) == resolved_group:
                return {"status": "group_busy", "job": dict(existing)}

        job_id = uuid.uuid4().hex
        job_record = {
            "job_id": job_id,
            "name": name,
            "group": resolved_group,
            "status": "running",
            "created_at": _job_now_iso(),
            "started_at": _job_now_iso(),
            "finished_at": None,
            "error": None,
            "result": None,
            "max_runtime_seconds": _background_job_timeout_seconds({"name": name, "group": resolved_group}),
        }
        BACKGROUND_JOBS[job_id] = job_record
        _prune_background_jobs_locked()

    def _runner() -> None:
        try:
            result = target()
            with BACKGROUND_JOB_LOCK:
                if job_id in BACKGROUND_JOBS:
                    BACKGROUND_JOBS[job_id]["status"] = "completed"
                    BACKGROUND_JOBS[job_id]["finished_at"] = _job_now_iso()
                    BACKGROUND_JOBS[job_id]["result"] = _json_safe(result)
                    BACKGROUND_JOBS[job_id].pop("launch_started_monotonic", None)
        except BaseException as exc:  # noqa: BLE001
            with BACKGROUND_JOB_LOCK:
                if job_id in BACKGROUND_JOBS:
                    BACKGROUND_JOBS[job_id]["status"] = "failed"
                    BACKGROUND_JOBS[job_id]["finished_at"] = _job_now_iso()
                    BACKGROUND_JOBS[job_id]["error"] = str(exc)
                    BACKGROUND_JOBS[job_id].pop("launch_started_monotonic", None)
        finally:
            with BACKGROUND_JOB_LOCK:
                BACKGROUND_JOB_THREADS.pop(job_id, None)

    thread = threading.Thread(target=_runner, daemon=True, name=f"nba-job-{name}-{job_id[:8]}")
    with BACKGROUND_JOB_LOCK:
        if job_id in BACKGROUND_JOBS:
            BACKGROUND_JOBS[job_id]["thread_name"] = thread.name
            BACKGROUND_JOBS[job_id]["launch_started_monotonic"] = time.monotonic()
            BACKGROUND_JOB_THREADS[job_id] = thread
    try:
        thread.start()
    except Exception as exc:  # noqa: BLE001
        with BACKGROUND_JOB_LOCK:
            if job_id in BACKGROUND_JOBS:
                BACKGROUND_JOBS[job_id]["status"] = "failed"
                BACKGROUND_JOBS[job_id]["finished_at"] = _job_now_iso()
                BACKGROUND_JOBS[job_id]["error"] = f"Failed to start background thread: {exc}"
                BACKGROUND_JOBS[job_id].pop("launch_started_monotonic", None)
            BACKGROUND_JOB_THREADS.pop(job_id, None)
            return {"status": "failed", "job": dict(BACKGROUND_JOBS.get(job_id, {"job_id": job_id, "name": name}))}
    with BACKGROUND_JOB_LOCK:
        if job_id in BACKGROUND_JOBS:
            return {"status": "started", "job": dict(BACKGROUND_JOBS[job_id])}
    return {"status": "failed", "job": {"job_id": job_id, "name": name, "error": "Job record disappeared unexpectedly."}}


def get_background_job(job_id: str) -> dict | None:
    with BACKGROUND_JOB_LOCK:
        _reconcile_running_jobs_locked()
        job = BACKGROUND_JOBS.get(job_id)
        if not job:
            return None
        return dict(job)


def _load_target_mae() -> dict[str, float]:
    fallback = {
        "points": 4.8,
        "rebounds": 2.0,
        "assists": 1.6,
    }
    if not DEFAULT_METRICS_PATH.exists():
        return fallback
    try:
        metrics = json.loads(DEFAULT_METRICS_PATH.read_text(encoding="utf-8"))
        per_target = metrics.get("per_target_metrics", {})
        for target in list(fallback.keys()):
            mae = _safe_float(per_target.get(target, {}).get("mae"), fallback[target])
            if mae > 0:
                fallback[target] = mae
    except Exception:
        return fallback
    return fallback


def _load_recheck_metrics() -> dict[str, object] | None:
    if not DEFAULT_RECHECK_PATH.exists():
        return None
    try:
        return json.loads(DEFAULT_RECHECK_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None


def _load_target_error_pct_profile() -> dict[str, float]:
    fallback = {
        "points": 12.0,
        "rebounds": 12.0,
        "assists": 12.0,
    }
    calibration_profile = None
    if DEFAULT_CALIBRATION_PATH.exists():
        try:
            calibration_profile = json.loads(DEFAULT_CALIBRATION_PATH.read_text(encoding="utf-8"))
        except Exception:
            calibration_profile = None

    if isinstance(calibration_profile, dict):
        per_target_profile = calibration_profile.get("per_target", {})
        if isinstance(per_target_profile, dict):
            for target in list(fallback.keys()):
                target_profile = per_target_profile.get(target, {})
                if isinstance(target_profile, dict):
                    observed = _safe_float(target_profile.get("mean_abs_pct_error_floor"), 0.0)
                    if observed > 0:
                        fallback[target] = max(4.0, min(45.0, observed))
            return fallback

    recheck = _load_recheck_metrics()
    if not recheck:
        return fallback

    per_target = recheck.get("per_target", {})
    for target in list(fallback.keys()):
        metric = per_target.get(target, {}) if isinstance(per_target, dict) else {}
        observed_pct_floor = _safe_float(metric.get("mean_abs_pct_error_floor"), 0.0)
        observed_pct = observed_pct_floor if observed_pct_floor > 0 else _safe_float(metric.get("mean_abs_pct_error"), fallback[target])
        fallback[target] = observed_pct if observed_pct > 0 else fallback[target]

    overall = recheck.get("overall", {})
    overall_pct_floor = _safe_float(overall.get("mean_abs_pct_error_floor"), 0.0)
    overall_pct = overall_pct_floor if overall_pct_floor > 0 else _safe_float(overall.get("mean_abs_pct_error"), 0.0)
    operational = recheck.get("operational_subset", {})
    operational_pct = _safe_float(
        operational.get("mean_abs_pct_error_floor") if isinstance(operational, dict) else 0.0,
        0.0,
    )
    for target in fallback:
        value = fallback[target]
        if overall_pct > 0:
            value = (value * 0.75) + (overall_pct * 0.25)
        if operational_pct > 0:
            value = (value * 0.85) + (operational_pct * 0.15)
        # Recheck metrics are backtest-wide and include volatile low-minute tails; project a tighter live-board baseline.
        fallback[target] = max(6.0, min(28.0, value * 0.55))

    return fallback


def _load_accuracy_hardening_profile() -> dict[str, float | int]:
    profile: dict[str, float | int] = {
        "min_edge_to_uncertainty": NO_BET_MIN_EDGE_TO_UNCERTAINTY,
        "max_projection_error_pct": NO_BET_MAX_PROJECTION_ERROR_PCT,
        "min_minutes_confidence": NO_BET_MINUTES_CONFIDENCE,
        "min_lineup_status_confidence": NO_BET_MIN_LINEUP_STATUS_CONFIDENCE,
        "min_pregame_anchor_strength": NO_BET_MIN_PREGAME_ANCHOR_STRENGTH,
        "min_line_books_count": NO_BET_MIN_LINE_BOOKS_COUNT,
        "max_line_snapshot_age_minutes": NO_BET_MAX_LINE_SNAPSHOT_AGE_MINUTES,
        "min_context_freshness_score": 0.38,
    }

    benchmark = load_rotowire_benchmark_report()
    if isinstance(benchmark, dict):
        overall = benchmark.get("overall", {})
        rows_evaluated = _safe_int(
            benchmark.get("rows_evaluated"),
            _safe_int(overall.get("rows"), 0) if isinstance(overall, dict) else 0,
        )
        hit_rate = _safe_float(overall.get("hit_rate"), 0.0) if isinstance(overall, dict) else 0.0
        clv_mean = _safe_float(overall.get("clv_mean"), 0.0) if isinstance(overall, dict) else 0.0
        projection_error_vs_line = (
            _safe_float(overall.get("projection_error_minus_line_error"), 0.0) if isinstance(overall, dict) else 0.0
        )
        if rows_evaluated >= 150:
            if hit_rate > 0 and hit_rate < 0.53:
                profile["min_edge_to_uncertainty"] = float(profile["min_edge_to_uncertainty"]) + 0.35
                profile["max_projection_error_pct"] = float(profile["max_projection_error_pct"]) - 3.0
                profile["min_minutes_confidence"] = float(profile["min_minutes_confidence"]) + 0.06
                profile["min_context_freshness_score"] = float(profile["min_context_freshness_score"]) + 0.08
            if clv_mean < 0:
                profile["min_edge_to_uncertainty"] = float(profile["min_edge_to_uncertainty"]) + 0.25
                profile["max_line_snapshot_age_minutes"] = min(
                    float(profile["max_line_snapshot_age_minutes"]),
                    240.0,
                )
            if projection_error_vs_line > 0:
                profile["max_projection_error_pct"] = float(profile["max_projection_error_pct"]) - 2.0

        calibration = benchmark.get("calibration", {})
        if isinstance(calibration, dict):
            lowest_bucket_rate = _safe_float(calibration.get("lowest_bucket_hit_rate"), 0.0)
            if lowest_bucket_rate and lowest_bucket_rate < 0.5:
                profile["min_edge_to_uncertainty"] = float(profile["min_edge_to_uncertainty"]) + 0.2

    recheck = _load_recheck_metrics()
    if isinstance(recheck, dict):
        overall = recheck.get("overall", {})
        overall_pct = _safe_float(overall.get("mean_abs_pct_error_floor"), 0.0) if isinstance(overall, dict) else 0.0
        if overall_pct <= 0:
            overall_pct = _safe_float(overall.get("mean_abs_pct_error"), 0.0) if isinstance(overall, dict) else 0.0
        if overall_pct > 26.0:
            profile["min_edge_to_uncertainty"] = float(profile["min_edge_to_uncertainty"]) + 0.25
            profile["max_projection_error_pct"] = float(profile["max_projection_error_pct"]) - 1.5
            profile["min_minutes_confidence"] = float(profile["min_minutes_confidence"]) + 0.04

    profile["min_edge_to_uncertainty"] = round(max(1.4, min(2.8, float(profile["min_edge_to_uncertainty"]))), 3)
    profile["max_projection_error_pct"] = round(max(14.0, min(40.0, float(profile["max_projection_error_pct"]))), 2)
    profile["min_minutes_confidence"] = round(max(0.2, min(0.7, float(profile["min_minutes_confidence"]))), 3)
    profile["min_lineup_status_confidence"] = round(
        max(0.3, min(0.9, float(profile["min_lineup_status_confidence"]))),
        3,
    )
    profile["min_pregame_anchor_strength"] = round(max(0.5, min(3.0, float(profile["min_pregame_anchor_strength"]))), 3)
    profile["min_line_books_count"] = int(max(1, min(5, int(float(profile["min_line_books_count"])))))
    profile["max_line_snapshot_age_minutes"] = round(
        max(60.0, min(420.0, float(profile["max_line_snapshot_age_minutes"]))),
        1,
    )
    profile["min_context_freshness_score"] = round(
        max(0.2, min(0.9, float(profile["min_context_freshness_score"]))),
        3,
    )
    return profile


def _prediction_frame_for_diagnostics(max_rows: int = 6000) -> pd.DataFrame:
    global _PREDICTION_FRAME_CACHE
    capped = max(200, int(max_rows))
    if not DEFAULT_PREDICTIONS_PATH.exists():
        return pd.DataFrame()
    try:
        stat = DEFAULT_PREDICTIONS_PATH.stat()
        signature = (stat.st_mtime_ns, stat.st_size, capped)
    except OSError:
        signature = None
    if signature is not None:
        cached = _PREDICTION_FRAME_CACHE.get(signature)
        if isinstance(cached, pd.DataFrame):
            return cached.copy()
    try:
        frame = pd.read_csv(DEFAULT_PREDICTIONS_PATH)
    except Exception:
        return pd.DataFrame()
    if frame.empty:
        return frame
    if len(frame) > capped:
        frame = frame.tail(capped).reset_index(drop=True)
    else:
        frame = frame.reset_index(drop=True)
    if signature is not None:
        _PREDICTION_FRAME_CACHE = {signature: frame}
    return frame


def _coverage_ratio(frame: pd.DataFrame, columns: list[str]) -> float:
    if frame.empty:
        return 0.0
    usable = [column for column in columns if column in frame.columns]
    if not usable:
        return 0.0
    ratios: list[float] = []
    for column in usable:
        series = frame[column]
        if series.dtype == object:
            present = series.notna() & series.astype(str).str.strip().ne("")
        else:
            present = series.notna()
        ratios.append(float(present.mean()))
    return float(sum(ratios) / max(len(ratios), 1))


def _mean_numeric(frame: pd.DataFrame, column: str, default: float = 0.0) -> float:
    if frame.empty or column not in frame.columns:
        return default
    value = pd.to_numeric(frame[column], errors="coerce").mean(skipna=True)
    if pd.isna(value):
        return default
    return float(value)


def _iso_age_seconds(timestamp_value: object) -> float | None:
    parsed = pd.to_datetime(pd.Series([timestamp_value]), errors="coerce", utc=True).iloc[0]
    if pd.isna(parsed):
        return None
    now_utc = pd.Timestamp.now(tz="UTC")
    age = float((now_utc - parsed).total_seconds())
    return max(0.0, age)


def _score_label(score: float) -> str:
    if score >= 88.0:
        return "excellent"
    if score >= 78.0:
        return "strong"
    if score >= 68.0:
        return "stable"
    if score >= 58.0:
        return "needs_work"
    return "critical"


def _runtime_reliability_snapshot(live_payload: dict | None, jobs: list[dict] | None = None) -> dict:
    live_payload = live_payload if isinstance(live_payload, dict) else {}
    live_running = bool(live_payload.get("running"))
    state = live_payload.get("state", {}) if isinstance(live_payload.get("state"), dict) else {}
    config = live_payload.get("config", {}) if isinstance(live_payload.get("config"), dict) else {}

    poll_interval_seconds = max(5.0, _safe_float(config.get("poll_interval_seconds"), 10.0))
    projection_refresh_interval_seconds = max(5.0, _safe_float(config.get("projection_refresh_interval_seconds"), 10.0))
    sync_age_seconds = _iso_age_seconds(state.get("last_sync_at"))
    sync_freshness_limit = max(30.0, poll_interval_seconds * 6.0)
    sync_freshness_score = (
        max(0.0, 1.0 - (sync_age_seconds / sync_freshness_limit))
        if sync_age_seconds is not None
        else 0.0
    )

    last_sync_duration_seconds = max(0.0, _safe_float(state.get("last_sync_duration_seconds"), 0.0))
    duration_score = (
        max(0.2, 1.0 - (last_sync_duration_seconds / max(poll_interval_seconds * 2.0, 1.0)))
        if last_sync_duration_seconds > 0
        else 0.8
    )
    cadence_score = min(1.0, 10.0 / projection_refresh_interval_seconds)
    if projection_refresh_interval_seconds <= 10.0:
        cadence_score = 1.0

    last_error = str(state.get("last_error", "") or "").strip()
    in_game_error = str(state.get("in_game_projection_last_error", "") or "").strip()
    error_score = 1.0
    if last_error:
        error_score -= 0.6
    if in_game_error:
        error_score -= 0.3
    error_score = max(0.0, min(1.0, error_score))

    recent_jobs = jobs if isinstance(jobs, list) else _recent_background_jobs(limit=30)
    failed_jobs = sum(1 for entry in recent_jobs if str(entry.get("status", "")).lower() == "failed")
    running_jobs = sum(1 for entry in recent_jobs if str(entry.get("status", "")).lower() == "running")
    job_score = (
        max(0.0, 1.0 - (failed_jobs / max(len(recent_jobs), 1)))
        if recent_jobs
        else 1.0
    )

    score = (
        (1.0 if live_running else 0.0) * 0.42
        + sync_freshness_score * 0.2
        + error_score * 0.2
        + job_score * 0.08
        + cadence_score * 0.05
        + duration_score * 0.05
    ) * 100.0
    score = max(0.0, min(100.0, score))

    return {
        "score": round(score, 2),
        "status": _score_label(score),
        "live_running": live_running,
        "sync_age_seconds": round(sync_age_seconds, 2) if sync_age_seconds is not None else None,
        "sync_freshness_score": round(sync_freshness_score, 3),
        "poll_interval_seconds": round(poll_interval_seconds, 2),
        "projection_refresh_interval_seconds": round(projection_refresh_interval_seconds, 2),
        "last_sync_duration_seconds": round(last_sync_duration_seconds, 3),
        "duration_score": round(duration_score, 3),
        "cadence_score": round(cadence_score, 3),
        "error_score": round(error_score, 3),
        "last_error": last_error or None,
        "in_game_projection_last_error": in_game_error or None,
        "failed_jobs_recent": int(failed_jobs),
        "running_jobs_recent": int(running_jobs),
        "jobs_considered": int(len(recent_jobs)),
        "job_score": round(job_score, 3),
    }


def _build_v05_recommendations(
    dimension_scores: dict[str, float],
    prediction_frame: pd.DataFrame,
    payload: dict,
    runtime_snapshot: dict,
) -> list[dict]:
    recommendations: list[dict] = []
    row_count = int(len(prediction_frame))
    live_state = payload.get("live_sync", {}).get("state", {}) if isinstance(payload.get("live_sync"), dict) else {}

    if dimension_scores.get("starter_probability_tightening", 0.0) < 80.0:
        recommendations.append(
            {
                "priority": "high",
                "dimension": "starter_probability_tightening",
                "action": "Increase lock-window lineup updates at T-90, T-30, and T-5 with stronger confidence weights.",
                "data_needed": [
                    "starter_probability",
                    "starter_certainty",
                    "lineup_status_confidence",
                    "pregame_lock_confidence",
                ],
                "current_lineup_rows_matched": _safe_int(live_state.get("lineup_rows_matched"), 0),
            }
        )
    if dimension_scores.get("pregame_minutes_certainty", 0.0) < 80.0:
        recommendations.append(
            {
                "priority": "high",
                "dimension": "pregame_minutes_certainty",
                "action": "Feed a projected-minutes source with confidence/cap reason and refresh pre-tip windows.",
                "data_needed": [
                    "expected_minutes",
                    "expected_minutes_confidence",
                    "minutes_projection_error_estimate",
                    "news_minutes_limit_mentions_24h",
                ],
            }
        )
    if dimension_scores.get("market_calibration", 0.0) < 80.0:
        recommendations.append(
            {
                "priority": "high",
                "dimension": "market_calibration",
                "action": "Capture open-to-close movement per market and increase multi-book coverage.",
                "data_needed": [
                    "line_points_open/close/movement",
                    "line_rebounds_open/close/movement",
                    "line_assists_open/close/movement",
                    "line_pra_open/close/movement",
                    "line_*_books_count",
                    "line_*_snapshot_age_minutes",
                ],
            }
        )
    if dimension_scores.get("coverage_depth", 0.0) < 78.0:
        recommendations.append(
            {
                "priority": "medium",
                "dimension": "coverage_depth",
                "action": "Expand row coverage for deep-rotation players and keep season priors synchronized.",
                "data_needed": [
                    "upcoming_slate rows for all active roster players",
                    "season_priors for all expected players",
                    "recent game history rows",
                ],
                "rows_currently_scored": row_count,
            }
        )
    if dimension_scores.get("shot_style_effect", 0.0) < 75.0:
        recommendations.append(
            {
                "priority": "medium",
                "dimension": "shot_style_effect",
                "action": "Raise playstyle confidence by refreshing shot profile context before each slate lock.",
                "data_needed": [
                    "shot_style_arc_score",
                    "shot_style_release_score",
                    "shot_style_miss_pressure",
                    "playstyle_* rates",
                ],
            }
        )
    if dimension_scores.get("teammate_dynamic_effect", 0.0) < 75.0:
        recommendations.append(
            {
                "priority": "medium",
                "dimension": "teammate_dynamic_effect",
                "action": "Increase teammate in/out and on-off deltas from lineup + injury feeds.",
                "data_needed": [
                    "teammate_usage_vacancy",
                    "teammate_continuity_score",
                    "teammate_synergy_points/rebounds/assists",
                    "teammate_on_off_*_delta",
                ],
            }
        )
    if dimension_scores.get("home_away_probability_boost", 0.0) < 72.0:
        recommendations.append(
            {
                "priority": "medium",
                "dimension": "home_away_probability_boost",
                "action": "Add stronger schedule/travel/home-town context coverage for every slate row.",
                "data_needed": [
                    "home flag coverage",
                    "home_court_points_boost",
                    "home_court_minutes_boost",
                    "hometown_advantage_score",
                    "travel_miles/rest_days",
                ],
            }
        )
    if dimension_scores.get("runtime_reliability", 0.0) < 85.0:
        recommendations.append(
            {
                "priority": "high",
                "dimension": "runtime_reliability",
                "action": "Stabilize the live loop: clear recurring job failures, keep live sync running, and keep sync age within freshness target.",
                "data_needed": [
                    "continuous live sync state",
                    "last_sync_at freshness",
                    "background job error traces",
                ],
                "runtime": runtime_snapshot,
            }
        )
    return recommendations


def _compute_v05_rubric(payload: dict | None = None, runtime_snapshot: dict | None = None) -> dict:
    payload = payload if isinstance(payload, dict) else {}
    prediction_frame = _prediction_frame_for_diagnostics()
    row_count = int(len(prediction_frame))
    live_payload = payload.get("live_sync", {}) if isinstance(payload.get("live_sync"), dict) else {}
    live_state = live_payload.get("state", {}) if isinstance(live_payload.get("state"), dict) else {}
    model_coverage = payload.get("model_coverage", {}) if isinstance(payload.get("model_coverage"), dict) else {}
    benchmark = payload.get("rotowire_benchmark", {}) if isinstance(payload.get("rotowire_benchmark"), dict) else {}
    low_conf_projection_count = _safe_int(payload.get("low_confidence_projection_count"), 0)

    starter_cov = _coverage_ratio(
        prediction_frame,
        ["starter", "starter_probability", "starter_certainty", "lineup_status_confidence", "pregame_lock_confidence"],
    )
    lineup_rows = max(
        _safe_float(live_state.get("lineup_rows_matched"), 0.0),
        _safe_float(live_state.get("starter_probability_rows"), 0.0),
    )
    lineup_norm = min(1.0, lineup_rows / max(20.0, row_count * 0.45 if row_count else 40.0))
    starter_mean = _mean_numeric(prediction_frame, "starter_probability", 0.0)
    starter_probability_tightening = (
        starter_cov * 0.52
        + lineup_norm * 0.3
        + min(1.0, starter_mean + 0.2) * 0.18
    ) * 100.0

    minutes_cov = _coverage_ratio(
        prediction_frame,
        ["expected_minutes", "expected_minutes_confidence", "minutes_projection_error_estimate", "pregame_lock_minutes_to_tipoff"],
    )
    minutes_conf_mean = _mean_numeric(prediction_frame, "expected_minutes_confidence", 0.0)
    minutes_err_mean = _mean_numeric(prediction_frame, "minutes_projection_error_estimate", 12.0)
    minutes_error_component = max(0.0, 1.0 - min(1.0, minutes_err_mean / 12.0))
    pregame_minutes_certainty = (
        minutes_cov * 0.45
        + minutes_conf_mean * 0.35
        + minutes_error_component * 0.2
    ) * 100.0

    market_cov = _coverage_ratio(
        prediction_frame,
        [
            "line_points_close",
            "line_points_consensus",
            "line_points_books_count",
            "line_points_snapshot_age_minutes",
            "line_rebounds_close",
            "line_rebounds_consensus",
            "line_rebounds_books_count",
            "line_rebounds_snapshot_age_minutes",
            "line_assists_close",
            "line_assists_consensus",
            "line_assists_books_count",
            "line_assists_snapshot_age_minutes",
            "line_pra_close",
            "line_pra_consensus",
            "line_pra_books_count",
            "line_pra_snapshot_age_minutes",
            "line_points_open",
            "line_rebounds_open",
            "line_assists_open",
            "line_pra_open",
            "line_points_movement",
            "line_rebounds_movement",
            "line_assists_movement",
            "line_pra_movement",
        ],
    )
    benchmark_rows = _safe_float(benchmark.get("rows_evaluated"), 0.0)
    benchmark_overall = benchmark.get("overall", {}) if isinstance(benchmark.get("overall"), dict) else {}
    benchmark_hit_rate = _safe_float(benchmark_overall.get("hit_rate"), 0.0)
    benchmark_clv = _safe_float(benchmark_overall.get("clv_mean"), 0.0)
    benchmark_delta = _safe_float(benchmark_overall.get("projection_error_minus_line_error"), 0.0)
    if benchmark_rows < 40:
        benchmark_quality = 0.45
    else:
        hit_component = max(0.0, min(1.0, (benchmark_hit_rate - 0.45) / 0.15))
        clv_component = max(0.0, min(1.0, (benchmark_clv + 0.02) / 0.08))
        delta_component = max(0.0, min(1.0, 1.0 - max(0.0, benchmark_delta) / 4.0))
        benchmark_quality = (hit_component * 0.4) + (clv_component * 0.3) + (delta_component * 0.3)
    market_calibration = ((market_cov * 0.58) + (benchmark_quality * 0.42)) * 100.0

    training_rows = _safe_float(model_coverage.get("training_rows"), 0.0)
    predicted_rows = _safe_float(model_coverage.get("predicted_rows"), row_count)
    season_priors_rows = _safe_float(model_coverage.get("season_priors_rows"), 0.0)
    training_norm = min(1.0, training_rows / 18000.0)
    predicted_norm = min(1.0, predicted_rows / 450.0)
    priors_norm = min(1.0, season_priors_rows / 450.0)
    low_conf_ratio = (low_conf_projection_count / max(predicted_rows, 1.0)) if predicted_rows > 0 else 1.0
    low_conf_component = max(0.0, 1.0 - min(1.0, low_conf_ratio))
    coverage_depth = (
        training_norm * 0.35
        + predicted_norm * 0.35
        + priors_norm * 0.15
        + low_conf_component * 0.15
    ) * 100.0

    shot_cov = _coverage_ratio(
        prediction_frame,
        [
            "shot_style_arc_score",
            "shot_style_release_score",
            "shot_style_volume_index",
            "shot_style_miss_pressure",
            "shot_style_points_factor",
            "shot_style_rebounds_factor",
            "shot_style_assists_factor",
            "playstyle_three_rate",
            "playstyle_rim_rate",
            "playstyle_drive_rate",
            "playstyle_assist_potential",
            "playstyle_context_confidence",
        ],
    )
    playstyle_conf_mean = _mean_numeric(prediction_frame, "playstyle_context_confidence", 0.0)
    shot_style_effect = ((shot_cov * 0.72) + (playstyle_conf_mean * 0.28)) * 100.0

    teammate_cov = _coverage_ratio(
        prediction_frame,
        [
            "teammate_usage_vacancy",
            "teammate_continuity_score",
            "teammate_star_out_flag",
            "teammate_synergy_points",
            "teammate_synergy_rebounds",
            "teammate_synergy_assists",
            "teammate_on_off_points_delta",
            "teammate_on_off_rebounds_delta",
            "teammate_on_off_assists_delta",
        ],
    )
    teammate_context_rows = _safe_float(live_state.get("teammate_context_rows"), 0.0)
    teammate_rows_norm = min(1.0, teammate_context_rows / max(15.0, row_count * 0.4 if row_count else 40.0))
    teammate_continuity_mean = _mean_numeric(prediction_frame, "teammate_continuity_score", 0.0)
    teammate_dynamic_effect = (
        teammate_cov * 0.62
        + teammate_rows_norm * 0.23
        + min(1.0, teammate_continuity_mean) * 0.15
    ) * 100.0

    home_cov = _coverage_ratio(
        prediction_frame,
        [
            "home",
            "hometown_game_flag",
            "home_court_points_boost",
            "home_court_minutes_boost",
            "hometown_advantage_score",
            "rest_days",
            "travel_miles",
            "implied_team_total",
            "spread",
            "game_total",
        ],
    )
    home_context_rows = _safe_float(live_state.get("home_context_rows"), 0.0)
    hometown_rows = _safe_float(live_state.get("hometown_context_rows"), 0.0)
    home_context_norm = min(1.0, (home_context_rows + hometown_rows) / max(20.0, row_count * 0.55 if row_count else 40.0))
    home_away_probability_boost = ((home_cov * 0.68) + (home_context_norm * 0.32)) * 100.0

    runtime = runtime_snapshot if isinstance(runtime_snapshot, dict) else _runtime_reliability_snapshot(
        live_payload,
        jobs=_recent_background_jobs(limit=30),
    )
    runtime_reliability = _safe_float(runtime.get("score"), 0.0)

    dimension_scores = {
        "starter_probability_tightening": max(0.0, min(100.0, starter_probability_tightening)),
        "pregame_minutes_certainty": max(0.0, min(100.0, pregame_minutes_certainty)),
        "market_calibration": max(0.0, min(100.0, market_calibration)),
        "coverage_depth": max(0.0, min(100.0, coverage_depth)),
        "shot_style_effect": max(0.0, min(100.0, shot_style_effect)),
        "teammate_dynamic_effect": max(0.0, min(100.0, teammate_dynamic_effect)),
        "home_away_probability_boost": max(0.0, min(100.0, home_away_probability_boost)),
        "runtime_reliability": max(0.0, min(100.0, runtime_reliability)),
    }
    overall_score = 0.0
    for key, weight in V05_RUBRIC_DIMENSION_WEIGHTS.items():
        overall_score += dimension_scores.get(key, 0.0) * float(weight)
    overall_score = max(0.0, min(100.0, overall_score))

    dimensions = {
        key: {
            "score": round(value, 2),
            "status": _score_label(value),
            "weight": V05_RUBRIC_DIMENSION_WEIGHTS.get(key, 0.0),
        }
        for key, value in dimension_scores.items()
    }
    recommendations = _build_v05_recommendations(dimension_scores, prediction_frame, payload, runtime)
    lowest_dimensions = sorted(dimensions.items(), key=lambda item: item[1].get("score", 0.0))[:3]

    return {
        "version": V05_RUBRIC_VERSION,
        "generated_at": datetime.now().isoformat(),
        "overall_score": round(overall_score, 2),
        "grade": _score_label(overall_score),
        "rows_scored": row_count,
        "dimensions": dimensions,
        "lowest_dimensions": [
            {"dimension": key, "score": details.get("score"), "status": details.get("status")}
            for key, details in lowest_dimensions
        ],
        "recommendations": recommendations,
        "additional_data_targets": V05_RUBRIC_DEFAULT_DATA_TARGETS,
        "prediction_algorithm": {
            "name": "contextual_blend_v05",
            "objective": "Keep model projections aligned while tightening pregame context reliability.",
            "components": [
                "starter_probability_tightening",
                "pregame_minutes_certainty",
                "market_anchor_calibration",
                "coverage_depth_adjustment",
                "shot_style_and_playstyle_factor",
                "teammate_dynamic_delta",
                "home_away_boost_downgrade",
            ],
            "runtime_target": "continuous live sync with low stale-state risk",
        },
        "runtime_reliability": runtime,
    }


def _build_v05_action_plan(rubric: dict | None, payload: dict | None = None) -> dict:
    rubric = rubric if isinstance(rubric, dict) else {}
    payload = payload if isinstance(payload, dict) else {}
    dimensions = rubric.get("dimensions", {}) if isinstance(rubric.get("dimensions"), dict) else {}

    dimension_order = sorted(
        dimensions.items(),
        key=lambda item: _safe_float((item[1] if isinstance(item[1], dict) else {}).get("score"), 0.0),
    )
    v1_targets = [
        {
            "dimension": key,
            "current_score": _safe_float((details if isinstance(details, dict) else {}).get("score"), 0.0),
            "target_score": 95.0,
            "target_error_pct_max": 5.0,
        }
        for key, details in dimension_order[:4]
    ]

    live_payload = payload.get("live_sync", {}) if isinstance(payload.get("live_sync"), dict) else {}
    live_state = live_payload.get("state", {}) if isinstance(live_payload.get("state"), dict) else {}
    provider_state = live_state.get("providers", {}) if isinstance(live_state.get("providers"), dict) else {}

    def _provider_status(name: str) -> dict:
        raw = provider_state.get(name, {})
        return raw if isinstance(raw, dict) else {}

    rotowire_state = _provider_status("rotowire_prizepicks")
    betr_state = _provider_status("betr")
    odds_state = _provider_status("odds")
    news_state = _provider_status("news")
    playstyle_state = _provider_status("playstyle")
    lineups_state = _provider_status("lineups")

    source_feasibility = [
        {
            "source": "RotoWire PrizePicks API feed",
            "mode": "automated",
            "enabled": bool(rotowire_state.get("enabled", False)),
            "rows_last_sync": _safe_int(rotowire_state.get("rows"), 0),
            "status": "active" if bool(rotowire_state.get("enabled", False)) else "disabled",
            "last_error": rotowire_state.get("last_error"),
        },
        {
            "source": "PrizePicks board (official app/web pages)",
            "mode": "manual_import_required",
            "enabled": False,
            "rows_last_sync": None,
            "status": "no_public_api",
            "last_error": "Authenticated browser + anti-bot controls block stable server-side ingestion.",
            "manual_import_endpoint": "/api/prizepicks/lines",
        },
        {
            "source": "BETR board",
            "mode": "manual_import_required",
            "enabled": bool(betr_state.get("enabled", False)),
            "rows_last_sync": _safe_int(betr_state.get("rows"), 0),
            "status": "no_stable_public_api",
            "last_error": betr_state.get("last_error") or "Use manual line import.",
            "manual_import_endpoint": "/api/prizepicks/lines",
        },
        {
            "source": "Odds + market consensus feeds",
            "mode": "automated",
            "enabled": bool(odds_state.get("enabled", False)),
            "rows_last_sync": _safe_int(odds_state.get("rows"), 0),
            "status": "active" if bool(odds_state.get("enabled", False)) else "disabled",
            "last_error": odds_state.get("last_error"),
        },
        {
            "source": "News aggregation (ESPN RSS, RotoWire RSS, Google News RSS)",
            "mode": "automated",
            "enabled": bool(news_state.get("enabled", False)),
            "rows_last_sync": _safe_int(news_state.get("rows"), 0),
            "status": "active" if bool(news_state.get("enabled", False)) else "disabled",
            "last_error": news_state.get("last_error"),
        },
        {
            "source": "Playstyle + shot profile context (NBA Stats)",
            "mode": "automated",
            "enabled": bool(playstyle_state.get("enabled", False)),
            "rows_last_sync": _safe_int(playstyle_state.get("rows"), 0),
            "status": "active" if bool(playstyle_state.get("enabled", False)) else "disabled",
            "last_error": playstyle_state.get("last_error"),
        },
        {
            "source": "Starter/lineup confirmations",
            "mode": "automated",
            "enabled": bool(lineups_state.get("enabled", False)),
            "rows_last_sync": _safe_int(lineups_state.get("rows"), 0),
            "status": "active" if bool(lineups_state.get("enabled", False)) else "disabled",
            "last_error": lineups_state.get("last_error"),
        },
    ]

    v2_execution_steps = [
        {
            "step": 1,
            "name": "Lock-window certainty refresh",
            "goal": "Tighten starter probability and expected minutes at T-90, T-30, T-5 windows.",
            "automated_now": True,
        },
        {
            "step": 2,
            "name": "Market calibration tightening",
            "goal": "Blend model and market anchors with stronger open-to-close weighting.",
            "automated_now": True,
        },
        {
            "step": 3,
            "name": "Coverage depth hardening",
            "goal": "Ensure deep rotation rows are present in upcoming slate + season priors.",
            "automated_now": True,
        },
        {
            "step": 4,
            "name": "Shot/playstyle and teammate dynamics update",
            "goal": "Refresh shot-style, playstyle, and teammate context before projection pass.",
            "automated_now": True,
        },
        {
            "step": 5,
            "name": "Runtime reliability audit",
            "goal": "Verify live loop freshness, error-free state, and background job health.",
            "automated_now": True,
        },
        {
            "step": 6,
            "name": "Final debug + diagnostics",
            "goal": "Run pipeline/status checks and confirm tightened accuracy + coverage gates.",
            "automated_now": True,
        },
    ]

    manual_chatbox_steps = [
        {
            "step": 1,
            "what_to_send": "PrizePicks/BETR prop lines as CSV text or file attachment.",
            "csv_header": "player_name,team,game_date,market,line,source,captured_at",
            "accepted_markets": ["points", "rebounds", "assists", "pra"],
        },
        {
            "step": 2,
            "what_to_send": "Any last-minute lineup/injury/news overrides not yet captured by feeds.",
            "csv_header": "player_name,game_date,team,starter_probability,lineup_status_confidence,expected_minutes,expected_minutes_confidence,injury_status,health_status,news_confidence_score",
        },
        {
            "step": 3,
            "what_to_send": "Ask: 'import this into lines/context and rerun sync + predict + edges'.",
            "system_actions": ["/api/prizepicks/lines", "/api/live/sync", "/api/predict", "/api/prizepicks/edges"],
        },
    ]

    accuracy_dimensions = [
        _safe_float((dimensions.get("starter_probability_tightening") or {}).get("score"), 0.0),
        _safe_float((dimensions.get("pregame_minutes_certainty") or {}).get("score"), 0.0),
        _safe_float((dimensions.get("market_calibration") or {}).get("score"), 0.0),
        _safe_float((dimensions.get("shot_style_effect") or {}).get("score"), 0.0),
        _safe_float((dimensions.get("teammate_dynamic_effect") or {}).get("score"), 0.0),
        _safe_float((dimensions.get("home_away_probability_boost") or {}).get("score"), 0.0),
    ]
    coverage_dimensions = [
        _safe_float((dimensions.get("coverage_depth") or {}).get("score"), 0.0),
        _safe_float((dimensions.get("runtime_reliability") or {}).get("score"), 0.0),
    ]
    accuracy_score = sum(accuracy_dimensions) / max(len(accuracy_dimensions), 1)
    coverage_score = sum(coverage_dimensions) / max(len(coverage_dimensions), 1)

    manual_intake_when_blocked = [
        {
            "when_needed": "PrizePicks board data not available via public API",
            "how_to_submit": "Paste CSV rows into PrizePicks Lines import (player_name, team, game_date, market, line).",
            "template": "player_name,team,game_date,market,line,source,captured_at",
        },
        {
            "when_needed": "BETR or other app-only line snapshots are inaccessible automatically",
            "how_to_submit": "Attach/export CSV or paste tabular lines using the same schema as above.",
            "template": "player_name,team,game_date,market,line,source,captured_at",
        },
        {
            "when_needed": "Breaking lineup/news context not captured by automated feeds",
            "how_to_submit": "Paste player-level updates into context feed with lineup/minutes/injury fields.",
            "template": "player_name,game_date,team,starter_probability,lineup_status_confidence,expected_minutes,expected_minutes_confidence,injury_status,health_status,news_confidence_score",
        },
    ]

    return {
        "version": V05_RUBRIC_VERSION,
        "v1_assessment_targets": v1_targets,
        "v2_execution_plan": v2_execution_steps,
        "source_feasibility": source_feasibility,
        "manual_data_intake_when_blocked": manual_intake_when_blocked,
        "manual_chatbox_steps": manual_chatbox_steps,
        "accuracy_gate_diagnostic": {
            "score": round(accuracy_score, 2),
            "target_score": 95.0,
            "error_target_pct_max": 5.0,
            "status": _score_label(accuracy_score),
        },
        "coverage_gate_diagnostic": {
            "score": round(coverage_score, 2),
            "target_score": 95.0,
            "error_target_pct_max": 5.0,
            "status": _score_label(coverage_score),
        },
        "current_live_loop": {
            "running": bool(live_payload.get("running")),
            "last_sync_at": live_state.get("last_sync_at"),
            "last_error": live_state.get("last_error"),
        },
    }


def _build_combined_status(include_previews: bool = True) -> dict:
    payload = app_status(include_previews=include_previews)
    payload["live_sync"] = LIVE_SYNC_MANAGER.ensure_running()
    payload["data_pipeline"] = pipeline_status(limit_events=25)
    payload["rotowire_benchmark"] = load_rotowire_benchmark_report()
    payload["accuracy_hardening"] = _load_accuracy_hardening_profile()
    payload["playstyle_sources"] = PLAYSTYLE_SOURCE_URLS
    payload["assistant"] = _assistant_config()
    payload["monetization"] = {
        "paywall_enforcement": PAYWALL_ENFORCEMENT,
        "plans": {code: {"name": details.get("name"), "limits": details.get("limits", {})} for code, details in PLAN_FEATURES.items()},
        "stripe_configured": bool(STRIPE_SECRET_KEY),
        "stripe_price_ids": {
            "pro_monthly": bool(STRIPE_PRICE_ID_PRO_MONTHLY),
            "pro_yearly": bool(STRIPE_PRICE_ID_PRO_YEARLY),
        },
        "public_url": APP_PUBLIC_URL,
    }
    runtime_snapshot = _runtime_reliability_snapshot(
        payload.get("live_sync"),
        jobs=_recent_background_jobs(limit=30),
    )
    payload["runtime_reliability"] = runtime_snapshot
    payload["v05_rubric"] = _compute_v05_rubric(payload, runtime_snapshot=runtime_snapshot)
    payload["v05_action_plan"] = _build_v05_action_plan(payload.get("v05_rubric"), payload)
    return payload


def _load_assistant_runtime_config() -> dict:
    if not ASSISTANT_RUNTIME_CONFIG_PATH.exists():
        return {}
    try:
        payload = json.loads(ASSISTANT_RUNTIME_CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _save_assistant_runtime_config(payload: dict) -> None:
    ASSISTANT_RUNTIME_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    ASSISTANT_RUNTIME_CONFIG_PATH.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def _resolve_assistant_credentials() -> tuple[str, str, str]:
    runtime = _load_assistant_runtime_config()
    runtime_key = str(runtime.get("openai_api_key", "") or "").strip()
    runtime_model = str(runtime.get("openai_model", "") or "").strip()
    env_key = str(os.environ.get("OPENAI_API_KEY", "") or "").strip()
    env_model = str(os.environ.get("OPENAI_MODEL", "") or "").strip()

    api_key = runtime_key or env_key
    model = runtime_model or env_model or DEFAULT_OPENAI_MODEL
    key_source = "runtime" if runtime_key else ("env" if env_key else "none")
    return api_key, model, key_source


def _mask_secret(value: str) -> str | None:
    secret = str(value or "").strip()
    if not secret:
        return None
    if len(secret) <= 8:
        return "*" * len(secret)
    return f"{secret[:4]}...{secret[-4:]}"


def _assistant_config() -> dict:
    api_key, model, key_source = _resolve_assistant_credentials()
    live_enabled = bool(api_key)
    return {
        "name": ASSISTANT_NAME,
        "provider": "openai",
        "mode": "live" if live_enabled else "local_fallback",
        "enabled": live_enabled,
        "model": model,
        "requires_api_key": not live_enabled,
        "key_source": key_source,
        "api_key_masked": _mask_secret(api_key),
        "runtime_config_path": str(ASSISTANT_RUNTIME_CONFIG_PATH),
        "agent_enabled": True,
        "agent_actions": sorted(ASSISTANT_AGENT_ACTIONS),
    }


def _assistant_context_snapshot(board_date: str | None, max_cards: int = 18) -> dict:
    board_payload = build_player_board(board_date)
    cards = board_payload.get("cards")
    if not isinstance(cards, list):
        cards = []

    ranked_cards = sorted(
        cards,
        key=lambda card: (
            0 if bool(card.get("is_starter")) else 1,
            0 if bool(card.get("is_actionable")) else 1,
            _safe_float(card.get("projection_error_pct_estimate"), 999.0),
            -_safe_float(card.get("confidence_pct"), 0.0),
            -_safe_float(card.get("projected_points"), 0.0),
        ),
    )

    focus_cards = []
    for card in ranked_cards[: max(1, int(max_cards))]:
        focus_cards.append(
            {
                "player_name": card.get("player_name"),
                "team": card.get("team"),
                "opponent": card.get("opponent"),
                "game_date": card.get("game_date"),
                "is_starter": bool(card.get("is_starter")),
                "is_actionable": bool(card.get("is_actionable")),
                "no_bet": bool(card.get("no_bet")),
                "projected_points": round(_safe_float(card.get("projected_points"), 0.0), 2),
                "projected_rebounds": round(_safe_float(card.get("projected_rebounds"), 0.0), 2),
                "projected_assists": round(_safe_float(card.get("projected_assists"), 0.0), 2),
                "projected_pra": round(_safe_float(card.get("projected_pra"), 0.0), 2),
                "projected_draftkings_points": round(_safe_float(card.get("projected_draftkings_points"), 0.0), 2),
                "projected_fanduel_points": round(_safe_float(card.get("projected_fanduel_points"), 0.0), 2),
                "confidence_pct": round(_safe_float(card.get("confidence_pct"), 0.0), 2),
                "error_pct_estimate": round(_safe_float(card.get("projection_error_pct_estimate"), 0.0), 2),
                "starter_probability": round(_safe_float(card.get("starter_probability"), 0.0), 3),
                "no_bet_reason_text": card.get("no_bet_reason_text"),
            }
        )

    return {
        "board_date": board_payload.get("board_date"),
        "available_dates": board_payload.get("available_dates"),
        "summary": board_payload.get("summary", {}),
        "focus_cards": focus_cards,
    }


def _assistant_system_prompt() -> str:
    return (
        f"You are {ASSISTANT_NAME}, the in-app NBA projection assistant. "
        "Use the provided board context as your source of truth. "
        "Give concise answers with concrete numbers (PTS, REB, AST, PRA, DK, FD) when available. "
        "If confidence is low or no-bet flags are present, say that directly."
    )


def _openai_chat_completion(messages: list[dict], model: str, api_key: str) -> str:
    body = {
        "model": model,
        "temperature": 0.2,
        "max_tokens": 500,
        "messages": messages,
    }
    request = urlrequest.Request(
        OPENAI_CHAT_COMPLETIONS_URL,
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urlrequest.urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urlerror.HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Assistant provider error ({exc.code}): {details}") from exc
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Assistant provider request failed: {exc}") from exc

    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        raise RuntimeError("Assistant provider returned no choices.")
    message = choices[0].get("message", {}) if isinstance(choices[0], dict) else {}
    content = message.get("content")
    if isinstance(content, str) and content.strip():
        return content.strip()
    if isinstance(content, list):
        text_parts = [
            str(part.get("text", "")).strip()
            for part in content
            if isinstance(part, dict) and str(part.get("text", "")).strip()
        ]
        if text_parts:
            return "\n".join(text_parts)
    raise RuntimeError("Assistant provider returned an empty response.")


def _assistant_local_fallback(message: str, context: dict) -> str:
    summary = context.get("summary") if isinstance(context, dict) else {}
    if not isinstance(summary, dict):
        summary = {}
    cards = context.get("focus_cards") if isinstance(context, dict) else []
    if not isinstance(cards, list):
        cards = []

    lines: list[str] = [
        f"{ASSISTANT_NAME} is running in local fallback mode (OPENAI_API_KEY not configured).",
        f"Board date: {context.get('board_date') or 'unknown'}",
        (
            "Summary: "
            f"players={summary.get('total_players', summary.get('players_total', 0))}, "
            f"starters={summary.get('starters', summary.get('starters_count', 0))}, "
            f"actionable={summary.get('actionable_players', summary.get('actionable_count', 0))}, "
            f"no_bet={summary.get('no_bet_players', summary.get('no_bet_count', 0))}"
        ),
    ]

    if cards:
        lines.append("Top context cards:")
        for card in cards[:5]:
            lines.append(
                "- "
                f"{card.get('player_name')} ({card.get('team')} vs {card.get('opponent')}): "
                f"PTS {card.get('projected_points')}, REB {card.get('projected_rebounds')}, "
                f"AST {card.get('projected_assists')}, PRA {card.get('projected_pra')}, "
                f"conf {card.get('confidence_pct')}%, err {card.get('error_pct_estimate')}%"
            )

    if message:
        lines.append("Ask again after adding OPENAI_API_KEY for richer natural-language analysis.")
    return "\n".join(lines)


def _assistant_parse_agent_actions(message: str) -> list[dict]:
    lowered = str(message or "").lower()
    actions: list[dict] = []

    def add(action: str, **params: object) -> None:
        if action not in ASSISTANT_AGENT_ACTIONS:
            return
        if any(entry.get("action") == action for entry in actions):
            return
        actions.append({"action": action, "params": params})

    if ("start" in lowered and "live" in lowered) or "resume live sync" in lowered:
        add("start_live_sync")
    if ("stop" in lowered and "live" in lowered) or "pause live sync" in lowered:
        add("stop_live_sync")
    if "in-game" in lowered and any(token in lowered for token in ["refresh", "sync", "update"]):
        add("sync_in_game_now")
    if ("sync now" in lowered or "refresh live" in lowered or "run sync" in lowered) and "in-game" not in lowered:
        add("sync_live_now")
    if "daily refresh" in lowered or (
        "sync" in lowered and "train" in lowered and "predict" in lowered and "recheck" in lowered
    ):
        add("run_daily_refresh")
    if "train" in lowered and "model" in lowered:
        add("train_models")
    if "predict" in lowered:
        add("run_predictions", predict_all=("predict all" in lowered))
    if "recheck" in lowered or "backtest" in lowered:
        lookback_match = re.search(r"(\d+)\s*day", lowered)
        lookback_days = int(lookback_match.group(1)) if lookback_match else None
        add("run_recheck", lookback_days=lookback_days)
    if "capture" in lowered and "benchmark" in lowered:
        add("capture_benchmark")
    if "run benchmark" in lowered or "benchmark report" in lowered:
        lookback_match = re.search(r"(\d+)\s*day", lowered)
        lookback_days = int(lookback_match.group(1)) if lookback_match else 28
        add("run_benchmark", lookback_days=lookback_days)
    if "prizepicks" in lowered and ("edge" in lowered or "edges" in lowered):
        date_match = re.search(r"(20\d{2}-\d{2}-\d{2})", lowered)
        slate_date = date_match.group(1) if date_match else None
        add("generate_prizepicks_edges", slate_date=slate_date)

    return actions


def _assistant_execute_agent_action(action: str, params: dict | None = None) -> dict:
    params = params if isinstance(params, dict) else {}

    if action == "start_live_sync":
        return {"action": action, "result": LIVE_SYNC_MANAGER.start()}
    if action == "stop_live_sync":
        return {"action": action, "result": LIVE_SYNC_MANAGER.stop()}
    if action == "sync_live_now":
        return {"action": action, "result": start_background_job("live_sync", LIVE_SYNC_MANAGER.sync_once)}
    if action == "sync_in_game_now":
        return {"action": action, "result": start_background_job("in_game_refresh", LIVE_SYNC_MANAGER.in_game_refresh_once)}
    if action == "run_daily_refresh":
        return {"action": action, "result": start_background_job("daily_refresh", run_daily_refresh_pipeline)}
    if action == "train_models":
        lookback_days_raw = pd.to_numeric(params.get("lookback_days"), errors="coerce")
        lookback_days = int(lookback_days_raw) if pd.notna(lookback_days_raw) and lookback_days_raw > 0 else None
        return {
            "action": action,
            "result": start_background_job("train", lambda: train_engine(lookback_days=lookback_days)),
        }
    if action == "run_predictions":
        predict_all = bool(params.get("predict_all", False))
        return {
            "action": action,
            "result": start_background_job("predict", lambda: predict_engine(predict_all=predict_all), group="model_jobs"),
        }
    if action == "run_recheck":
        lookback_days_raw = pd.to_numeric(params.get("lookback_days"), errors="coerce")
        lookback_days = int(lookback_days_raw) if pd.notna(lookback_days_raw) and lookback_days_raw > 0 else None
        sample_rows_raw = pd.to_numeric(params.get("sample_rows"), errors="coerce")
        sample_rows = int(sample_rows_raw) if pd.notna(sample_rows_raw) and sample_rows_raw > 0 else None
        return {
            "action": action,
            "result": start_background_job(
                "recheck",
                lambda: recheck_past_predictions(
                    lookback_days=lookback_days,
                    sample_rows=sample_rows,
                ),
            ),
        }
    if action == "capture_benchmark":
        return {"action": action, "result": capture_rotowire_benchmark_snapshot()}
    if action == "run_benchmark":
        lookback_days_raw = pd.to_numeric(params.get("lookback_days"), errors="coerce")
        lookback_days = int(lookback_days_raw) if pd.notna(lookback_days_raw) and lookback_days_raw > 0 else 28
        return {
            "action": action,
            "result": start_background_job(
                "rotowire_benchmark",
                lambda: run_rotowire_benchmark(lookback_days=max(1, lookback_days)),
                group="model_jobs",
            ),
        }
    if action == "generate_prizepicks_edges":
        slate_date = params.get("slate_date")
        slate_date_text = str(slate_date).strip() if slate_date is not None else ""
        return {
            "action": action,
            "result": generate_prizepicks_edges(slate_date=slate_date_text or None),
        }

    raise ValueError(f"Unsupported agent action: {action}")


def _assistant_execute_actions_from_message(message: str) -> dict:
    planned_actions = _assistant_parse_agent_actions(message)
    results: list[dict] = []
    for planned in planned_actions:
        action_name = str(planned.get("action") or "").strip()
        params = planned.get("params")
        try:
            results.append(_assistant_execute_agent_action(action_name, params if isinstance(params, dict) else {}))
        except Exception as exc:  # noqa: BLE001
            results.append(
                {
                    "action": action_name,
                    "error": str(exc),
                }
            )
    return {"planned_actions": planned_actions, "results": results}


def _assistant_action_report_text(agent_report: dict | None) -> str:
    if not isinstance(agent_report, dict):
        return ""
    results = agent_report.get("results")
    if not isinstance(results, list) or not results:
        return ""
    lines = ["Agent actions executed:"]
    for item in results:
        if not isinstance(item, dict):
            continue
        action_name = str(item.get("action") or "unknown")
        if item.get("error"):
            lines.append(f"- {action_name}: failed ({item.get('error')})")
            continue
        result = item.get("result")
        if isinstance(result, dict):
            status = str(result.get("status") or "").strip().lower()
            job = result.get("job")
            job_id = job.get("job_id") if isinstance(job, dict) else None
            if status == "started" and job_id:
                lines.append(f"- {action_name}: queued (job {job_id})")
            elif status == "already_running" and job_id:
                lines.append(f"- {action_name}: already running (job {job_id})")
            elif status == "group_busy" and job_id:
                lines.append(f"- {action_name}: skipped, group busy (job {job_id})")
            elif status == "failed":
                lines.append(f"- {action_name}: failed to queue")
            elif job_id:
                lines.append(f"- {action_name}: {status or 'ok'} (job {job_id})")
            else:
                lines.append(f"- {action_name}: ok")
        else:
            lines.append(f"- {action_name}: ok")
    return "\n".join(lines)


def _assistant_chat_reply(
    message: str,
    board_date: str | None,
    conversation: list[dict] | None,
    agent_mode: bool = False,
) -> dict:
    message_text = str(message or "").strip()
    if not message_text:
        raise ValueError("message is required.")

    assistant = _assistant_config()
    api_key, model_name, _key_source = _resolve_assistant_credentials()
    agent_report = _assistant_execute_actions_from_message(message_text) if agent_mode else None
    context = _assistant_context_snapshot(board_date, max_cards=20)
    action_report_text = _assistant_action_report_text(agent_report)

    if assistant.get("mode") != "live":
        fallback_reply = _assistant_local_fallback(message_text, context)
        if action_report_text:
            fallback_reply = f"{action_report_text}\n\n{fallback_reply}"
        return {
            "assistant": assistant,
            "reply": fallback_reply,
            "context": context,
            "agent_report": agent_report,
            "generated_at": datetime.now().isoformat(),
        }

    history: list[dict] = []
    if isinstance(conversation, list):
        for entry in conversation[-8:]:
            if not isinstance(entry, dict):
                continue
            role = str(entry.get("role", "")).strip().lower()
            content = str(entry.get("content", "")).strip()
            if role not in {"user", "assistant"} or not content:
                continue
            history.append({"role": role, "content": content[:2000]})

    context_json = json.dumps(context, ensure_ascii=False)
    messages = [
        {"role": "system", "content": _assistant_system_prompt()},
        {"role": "system", "content": f"Board context JSON:\n{context_json}"},
        *(
            [{"role": "system", "content": f"Agent execution report:\n{action_report_text}"}]
            if action_report_text
            else []
        ),
        *history,
        {"role": "user", "content": message_text},
    ]

    try:
        reply = _openai_chat_completion(
            messages=messages,
            model=str(model_name or assistant.get("model") or DEFAULT_OPENAI_MODEL),
            api_key=str(api_key or "").strip(),
        )
        return {
            "assistant": assistant,
            "reply": reply,
            "context": context,
            "agent_report": agent_report,
            "generated_at": datetime.now().isoformat(),
        }
    except Exception as exc:  # noqa: BLE001
        assistant_fallback = dict(assistant)
        assistant_fallback["mode"] = "local_fallback"
        fallback_reply = _assistant_local_fallback(message_text, context)
        if action_report_text:
            fallback_reply = f"{action_report_text}\n\n{fallback_reply}"
        return {
            "assistant": assistant_fallback,
            "reply": fallback_reply,
            "context": context,
            "agent_report": agent_report,
            "warning": str(exc),
            "generated_at": datetime.now().isoformat(),
        }


def _assistant_update_config(
    openai_api_key: object = None,
    openai_model: object = None,
    clear_api_key: bool = False,
    test_connection: bool = False,
) -> dict:
    config = _load_assistant_runtime_config()
    updated_fields: list[str] = []

    if clear_api_key:
        if "openai_api_key" in config:
            config.pop("openai_api_key", None)
            updated_fields.append("openai_api_key")

    if openai_api_key is not None:
        key_value = str(openai_api_key or "").strip()
        if key_value:
            config["openai_api_key"] = key_value
            updated_fields.append("openai_api_key")
        else:
            if "openai_api_key" in config:
                config.pop("openai_api_key", None)
                updated_fields.append("openai_api_key")

    if openai_model is not None:
        model_value = str(openai_model or "").strip()
        if model_value:
            config["openai_model"] = model_value
            updated_fields.append("openai_model")
        else:
            if "openai_model" in config:
                config.pop("openai_model", None)
                updated_fields.append("openai_model")

    _save_assistant_runtime_config(config)

    assistant = _assistant_config()
    response: dict[str, object] = {
        "status": "ok",
        "assistant": assistant,
        "updated_fields": sorted(set(updated_fields)),
    }

    if test_connection and assistant.get("mode") == "live":
        api_key, model, _ = _resolve_assistant_credentials()
        try:
            _openai_chat_completion(
                messages=[
                    {"role": "system", "content": "You are a test endpoint."},
                    {"role": "user", "content": "Reply with: Friday live OK"},
                ],
                model=model,
                api_key=api_key,
            )
            response["connection_test"] = {"ok": True}
        except Exception as exc:  # noqa: BLE001
            response["connection_test"] = {"ok": False, "error": str(exc)}

    return response


def _combined_app_status_cached(include_previews: bool = True, force_refresh: bool = False) -> dict:
    now_ts = time.monotonic()
    with STATUS_CACHE_LOCK:
        cached_payload = STATUS_CACHE.get("payload")
        cached_at = float(STATUS_CACHE.get("generated_at_ts") or 0.0)
        cached_preview_mode = bool(STATUS_CACHE.get("include_previews", True))
        if (
            not force_refresh
            and cached_payload is not None
            and cached_preview_mode == include_previews
            and (now_ts - cached_at) <= STATUS_CACHE_TTL_SECONDS
        ):
            return dict(cached_payload)

    payload = combined_app_status(include_previews=include_previews)
    with STATUS_CACHE_LOCK:
        STATUS_CACHE["generated_at_ts"] = now_ts
        STATUS_CACHE["include_previews"] = include_previews
        STATUS_CACHE["payload"] = payload
    return dict(payload)


def _resolve_board_date(predictions: pd.DataFrame, requested_date: str | None) -> tuple[str | None, list[str]]:
    if predictions.empty or "game_date" not in predictions.columns:
        return None, []

    normalized_dates = (
        predictions["game_date"]
        .dropna()
        .astype(str)
        .map(lambda value: pd.to_datetime(value, errors="coerce"))
        .dropna()
        .dt.strftime("%Y-%m-%d")
    )
    dates = sorted(normalized_dates.unique().tolist())
    if not dates:
        return None, []

    available_dates = ["all"] + dates
    requested = str(requested_date or "").strip()
    if requested.lower() == "all":
        return "all", available_dates
    if requested and requested in dates:
        return requested, available_dates

    # Default to today's slate (or nearest active slate) so the UI does not try to render all rows at once.
    if not requested:
        today = datetime.now().strftime("%Y-%m-%d")
        counts = normalized_dates.value_counts()
        future_counts = counts[counts.index >= today]
        if today in dates:
            return today, available_dates
        if not future_counts.empty:
            return str(future_counts.idxmax()), available_dates
        return str(counts.idxmax()), available_dates

    today = datetime.now().strftime("%Y-%m-%d")
    counts = normalized_dates.value_counts()
    future_counts = counts[counts.index >= today]

    if not future_counts.empty:
        today_count = int(future_counts.get(today, 0))
        max_count = int(future_counts.max())
        if today_count >= max(50, int(round(max_count * 0.75))):
            return today, available_dates
        return str(future_counts.idxmax()), available_dates

    if today in dates:
        return today, available_dates
    return str(counts.idxmax()), available_dates


def _predictions_fail_sanity(predictions: pd.DataFrame) -> bool:
    if predictions.empty or "predicted_points" not in predictions.columns:
        return False

    points = pd.to_numeric(predictions["predicted_points"], errors="coerce")
    if points.notna().sum() < 50:
        return False

    minutes = pd.to_numeric(predictions.get("predicted_minutes"), errors="coerce")
    mean_points = float(points.mean(skipna=True))
    p90_points = float(points.quantile(0.90))
    mean_minutes = float(minutes.mean(skipna=True)) if minutes.notna().any() else float("nan")

    return mean_points < 2.0 and p90_points < 5.0 and (pd.isna(mean_minutes) or mean_minutes > 10.0)


def _empty_recent_form_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=["player_key", "team_key", *RECENT_FORM_COLUMNS])


def _load_recent_form_metrics(board_date: str | None) -> tuple[pd.DataFrame, pd.DataFrame]:
    active_training_path = DEFAULT_TRAINING_UPLOAD_PATH if DEFAULT_TRAINING_UPLOAD_PATH.exists() else DEFAULT_DATA_PATH
    if not active_training_path.exists():
        return _empty_recent_form_frame(), _empty_recent_form_frame()

    cache_key: tuple[str, int, int, str] | None = None
    try:
        stats = active_training_path.stat()
        cache_key = (
            str(active_training_path),
            int(stats.st_mtime_ns),
            int(stats.st_size),
            str(board_date or ""),
        )
    except OSError:
        cache_key = None

    if cache_key is not None:
        with RECENT_FORM_CACHE_LOCK:
            cached = RECENT_FORM_CACHE.get(cache_key)
        if cached is not None:
            cached_team, cached_name = cached
            return cached_team.copy(), cached_name.copy()

    try:
        history = pd.read_csv(active_training_path)
    except Exception:
        return _empty_recent_form_frame(), _empty_recent_form_frame()

    required = {"player_name", "game_date"}
    if not required.issubset(set(history.columns)):
        return _empty_recent_form_frame(), _empty_recent_form_frame()

    history = history.copy()
    for column in ["minutes", "points", "rebounds", "assists", "starter"]:
        if column not in history.columns:
            history[column] = pd.NA
        history[column] = pd.to_numeric(history[column], errors="coerce")

    if "team" not in history.columns:
        history["team"] = ""
    history["team"] = history["team"].fillna("").astype(str)

    history["game_date"] = pd.to_datetime(history["game_date"], errors="coerce")
    history = history.dropna(subset=["player_name", "game_date"]).copy()
    if history.empty:
        return _empty_recent_form_frame(), _empty_recent_form_frame()

    if board_date and board_date != "all":
        cutoff = pd.to_datetime(board_date, errors="coerce")
        if pd.notna(cutoff):
            history = history[history["game_date"] < cutoff].copy()
            if history.empty:
                return _empty_recent_form_frame(), _empty_recent_form_frame()

    history["player_key"] = history["player_name"].map(normalize_player_name)
    history["team_key"] = history["team"].map(normalize_team_code)

    def aggregate_recent(frame: pd.DataFrame, group_columns: list[str]) -> pd.DataFrame:
        if frame.empty:
            return pd.DataFrame(columns=[*group_columns, *RECENT_FORM_COLUMNS])

        ordered = frame[
            [
                *group_columns,
                "game_date",
                "points",
                "rebounds",
                "assists",
                "minutes",
                "starter",
            ]
        ].copy()
        ordered = ordered.sort_values([*group_columns, "game_date"])
        recent = ordered.groupby(group_columns, dropna=False, sort=False).tail(10)
        grouped = recent.groupby(group_columns, dropna=False, sort=False)

        counts = grouped.size().astype(float).rename("recent_games_count_last_10")
        means = grouped[["points", "rebounds", "assists", "minutes"]].mean().rename(
            columns={
                "points": "points_avg_last_10",
                "rebounds": "rebounds_avg_last_10",
                "assists": "assists_avg_last_10",
                "minutes": "minutes_avg_last_10",
            }
        )
        stds = grouped[["points", "rebounds", "assists", "minutes"]].std(ddof=0).fillna(0.0).rename(
            columns={
                "points": "points_std_last_10",
                "rebounds": "rebounds_std_last_10",
                "assists": "assists_std_last_10",
                "minutes": "minutes_std_last_10",
            }
        )
        starter_rate = grouped["starter"].mean().fillna(0.0).rename("starter_rate_last_10")
        return pd.concat([counts, means, stds, starter_rate], axis=1).reset_index()

    team_metrics = aggregate_recent(history, ["player_key", "team_key"])
    name_metrics = aggregate_recent(history, ["player_key"])

    if "team_key" not in name_metrics.columns:
        name_metrics["team_key"] = ""
    else:
        name_metrics["team_key"] = ""

    if team_metrics.empty:
        team_metrics = _empty_recent_form_frame()
    else:
        team_metrics = team_metrics[["player_key", "team_key", *RECENT_FORM_COLUMNS]]

    if name_metrics.empty:
        name_metrics = _empty_recent_form_frame()
    else:
        name_metrics = name_metrics[["player_key", "team_key", *RECENT_FORM_COLUMNS]]

    if cache_key is not None:
        with RECENT_FORM_CACHE_LOCK:
            RECENT_FORM_CACHE.clear()
            RECENT_FORM_CACHE[cache_key] = (team_metrics.copy(), name_metrics.copy())

    return team_metrics, name_metrics


def _blend_anchor_series(sources: list[tuple[pd.Series, float]]) -> tuple[pd.Series, pd.Series]:
    if not sources:
        empty = pd.Series(dtype=float)
        return empty, empty
    index = sources[0][0].index
    weighted_sum = pd.Series(0.0, index=index, dtype=float)
    weight_sum = pd.Series(0.0, index=index, dtype=float)
    source_count = pd.Series(0.0, index=index, dtype=float)
    for series, weight in sources:
        numeric = pd.to_numeric(series, errors="coerce")
        valid = numeric.notna()
        weighted_sum.loc[valid] += numeric.loc[valid] * float(weight)
        weight_sum.loc[valid] += float(weight)
        source_count.loc[valid] += 1.0
    return weighted_sum / weight_sum.replace(0.0, float("nan")), source_count


def _ensure_pregame_anchor_columns(board: pd.DataFrame) -> pd.DataFrame:
    if board.empty:
        return board

    anchored = board.copy()

    def _series_or_nan(column: str) -> pd.Series:
        if column in anchored.columns:
            return pd.to_numeric(anchored[column], errors="coerce")
        return pd.Series(float("nan"), index=anchored.index, dtype=float)

    specs = [
        ("points", "line_points", "pts_season", "points_avg_last_5"),
        ("rebounds", "line_rebounds", "reb_season", "rebounds_avg_last_5"),
        ("assists", "line_assists", "ast_season", "assists_avg_last_5"),
    ]
    for target, line_column, season_column, form_column in specs:
        anchor_column = f"pregame_anchor_{target}"
        gap_column = f"pregame_anchor_gap_{target}"
        source_column = f"pregame_anchor_sources_{target}"
        prediction_column = f"predicted_{target}"
        if prediction_column not in anchored.columns:
            continue
        if anchor_column not in anchored.columns:
            model_series = _series_or_nan(prediction_column)
            line_series = _series_or_nan(line_column)
            season_series = _series_or_nan(season_column)
            form_series = _series_or_nan(form_column)
            anchor, source_count = _blend_anchor_series(
                [
                    (line_series, 0.5),
                    (form_series, 0.2),
                    (season_series, 0.2),
                    (model_series, 0.1),
                ]
            )
            anchored[anchor_column] = anchor.round(2)
            anchored[source_column] = source_count.fillna(0).astype(int)
        if gap_column not in anchored.columns:
            anchored[gap_column] = (
                pd.to_numeric(anchored[prediction_column], errors="coerce")
                - pd.to_numeric(anchored[anchor_column], errors="coerce")
            ).round(2)

    if "predicted_pra" in anchored.columns:
        if "pregame_anchor_pra" not in anchored.columns:
            line_pra = _series_or_nan("line_pra")
            model_pra = _series_or_nan("predicted_pra")
            form_pra = _series_or_nan("points_avg_last_5") + _series_or_nan("rebounds_avg_last_5") + _series_or_nan("assists_avg_last_5")
            season_pra = _series_or_nan("pts_season") + _series_or_nan("reb_season") + _series_or_nan("ast_season")
            anchor, source_count = _blend_anchor_series(
                [
                    (line_pra, 0.5),
                    (form_pra, 0.2),
                    (season_pra, 0.2),
                    (model_pra, 0.1),
                ]
            )
            anchored["pregame_anchor_pra"] = anchor.round(2)
            anchored["pregame_anchor_sources_pra"] = source_count.fillna(0).astype(int)
        if "pregame_anchor_gap_pra" not in anchored.columns:
            anchored["pregame_anchor_gap_pra"] = (
                _series_or_nan("predicted_pra") - _series_or_nan("pregame_anchor_pra")
            ).round(2)

    source_columns = [column for column in anchored.columns if column.startswith("pregame_anchor_sources_")]
    if source_columns:
        anchored["pregame_anchor_strength"] = (
            anchored[source_columns]
            .apply(pd.to_numeric, errors="coerce")
            .fillna(0.0)
            .mean(axis=1)
            .clip(lower=0.0, upper=4.0)
            .round(2)
        )
    elif "pregame_anchor_strength" not in anchored.columns:
        anchored["pregame_anchor_strength"] = 0.0

    return anchored


def _apply_v05_projection_algorithm(board: pd.DataFrame) -> pd.DataFrame:
    if board.empty:
        return board

    adjusted = board.copy()

    def _series_or_default(column: str, default: float = 0.0) -> pd.Series:
        if column in adjusted.columns:
            return pd.to_numeric(adjusted[column], errors="coerce").fillna(default)
        return pd.Series(default, index=adjusted.index, dtype=float)

    starter_prob = _series_or_default("starter_probability", 0.0).clip(lower=0.0, upper=1.0)
    starter_flag = _series_or_default("starter", 0.0).clip(lower=0.0, upper=1.0)
    starter_prob = starter_prob.where(starter_prob.gt(0), starter_flag)
    lineup_conf = _series_or_default("lineup_status_confidence", 0.0).clip(lower=0.0, upper=1.0)
    minutes_conf = _series_or_default("expected_minutes_confidence", 0.0).clip(lower=0.0, upper=1.0)
    pregame_lock_conf = _series_or_default("pregame_lock_confidence", 0.0).clip(lower=0.0, upper=1.0)
    minutes_context = _series_or_default("minutes_playable_context", 0.0).clip(lower=0.0, upper=48.0)
    historical_games = _series_or_default("historical_games_used", 0.0).clip(lower=0.0, upper=82.0)
    season_priors_available = _series_or_default("season_priors_available", 0.0).clip(lower=0.0, upper=1.0)
    coverage_depth = (
        (historical_games.clip(lower=0.0, upper=18.0) / 18.0) * 0.75
        + season_priors_available * 0.25
    ).clip(lower=0.0, upper=1.0)
    market_quality = _series_or_default("line_data_freshness_score", 0.0).clip(lower=0.0, upper=1.0)
    teammate_continuity = _series_or_default("teammate_continuity_score", 0.0).clip(lower=0.0, upper=1.0)
    style_confidence = _series_or_default("playstyle_context_confidence", 0.0).clip(lower=0.0, upper=1.0)
    home_flag = _series_or_default("home", 0.0).clip(lower=0.0, upper=1.0)
    hometown_bonus = _series_or_default("hometown_advantage_score", 0.0).clip(lower=0.0, upper=2.0)

    market_specs = {
        "points": {
            "pred_col": "predicted_points",
            "anchor_col": "pregame_anchor_points",
            "season_col": "pts_season",
            "shot_factor_col": "shot_style_points_factor",
            "teammate_delta_col": "teammate_synergy_points",
            "home_boost_col": "home_court_points_boost",
        },
        "rebounds": {
            "pred_col": "predicted_rebounds",
            "anchor_col": "pregame_anchor_rebounds",
            "season_col": "reb_season",
            "shot_factor_col": "shot_style_rebounds_factor",
            "teammate_delta_col": "teammate_synergy_rebounds",
            "home_boost_col": "home_court_minutes_boost",
        },
        "assists": {
            "pred_col": "predicted_assists",
            "anchor_col": "pregame_anchor_assists",
            "season_col": "ast_season",
            "shot_factor_col": "shot_style_assists_factor",
            "teammate_delta_col": "teammate_synergy_assists",
            "home_boost_col": "home_court_minutes_boost",
        },
    }

    for market, spec in market_specs.items():
        pred_col = spec["pred_col"]
        if pred_col not in adjusted.columns:
            continue

        base_projection = _series_or_default(pred_col, 0.0).clip(lower=0.0)
        anchor_projection = _series_or_default(spec["anchor_col"], float("nan"))
        has_anchor = anchor_projection.notna()
        shot_factor = _series_or_default(spec["shot_factor_col"], 1.0).clip(lower=0.82, upper=1.28)
        teammate_delta = _series_or_default(spec["teammate_delta_col"], 0.0).clip(lower=-8.0, upper=8.0)
        home_boost = _series_or_default(spec["home_boost_col"], 0.0).clip(lower=-5.0, upper=5.0)

        context_projection = (
            (base_projection * shot_factor)
            + teammate_delta * (0.55 + 0.45 * teammate_continuity)
            + home_boost * (0.35 + 0.65 * home_flag)
            + hometown_bonus * (0.12 if market == "points" else 0.07)
        )

        model_weight = (
            0.58
            + coverage_depth * 0.2
            + starter_prob * 0.06
            + minutes_conf * 0.06
            - (1.0 - market_quality) * 0.08
        ).clip(lower=0.45, upper=0.86)
        anchor_weight = (
            0.18
            + market_quality * 0.22
            + (1.0 - coverage_depth) * 0.1
        ).clip(lower=0.08, upper=0.46)
        context_weight = (
            0.24
            + minutes_conf * 0.1
            + lineup_conf * 0.08
            + pregame_lock_conf * 0.08
            + style_confidence * 0.06
        ).clip(lower=0.08, upper=0.4)

        anchor_weight = anchor_weight.where(has_anchor, 0.0)
        weight_sum = (model_weight + anchor_weight + context_weight).replace(0.0, 1.0)
        model_weight = model_weight / weight_sum
        anchor_weight = anchor_weight / weight_sum
        context_weight = context_weight / weight_sum

        blended = (
            base_projection * model_weight
            + anchor_projection.fillna(base_projection) * anchor_weight
            + context_projection * context_weight
        )

        starter_tightening = pd.Series(
            np.where(starter_prob < 0.35, 0.84, np.where(starter_prob < 0.5, 0.92, 1.0)),
            index=adjusted.index,
            dtype=float,
        )
        minutes_tightening = pd.Series(
            np.where(minutes_context < 12.0, 0.78, np.where(minutes_context < 18.0, 0.9, 1.0)),
            index=adjusted.index,
            dtype=float,
        )
        blended = blended * starter_tightening * minutes_tightening

        season_col = spec["season_col"]
        if season_col in adjusted.columns:
            season_baseline = _series_or_default(season_col, float("nan"))
            low_history_mask = historical_games.lt(5.0) & season_baseline.notna()
            blended.loc[low_history_mask] = (
                blended.loc[low_history_mask] * 0.65
                + season_baseline.loc[low_history_mask] * 0.35
            )

        blended = blended.clip(lower=0.0)
        adjusted[f"v05_projected_{market}"] = blended.round(3)
        adjusted[f"v05_projection_delta_{market}"] = (blended - base_projection).round(3)
        adjusted[f"v05_model_weight_{market}"] = model_weight.round(3)
        adjusted[f"v05_anchor_weight_{market}"] = anchor_weight.round(3)
        adjusted[f"v05_context_weight_{market}"] = context_weight.round(3)
        adjusted[pred_col] = blended

        delta = blended - base_projection
        for band, multiplier in (("p10", 0.85), ("p50", 1.0), ("p90", 1.15)):
            band_col = f"{pred_col}_{band}"
            if band_col in adjusted.columns:
                adjusted[band_col] = (
                    pd.to_numeric(adjusted[band_col], errors="coerce")
                    + delta * multiplier
                ).clip(lower=0.0)

        p10_col = f"{pred_col}_p10"
        p50_col = f"{pred_col}_p50"
        p90_col = f"{pred_col}_p90"
        if p10_col in adjusted.columns and p50_col in adjusted.columns:
            adjusted[p10_col] = np.minimum(
                pd.to_numeric(adjusted[p10_col], errors="coerce"),
                pd.to_numeric(adjusted[p50_col], errors="coerce"),
            )
        if p50_col in adjusted.columns and p90_col in adjusted.columns:
            adjusted[p90_col] = np.maximum(
                pd.to_numeric(adjusted[p90_col], errors="coerce"),
                pd.to_numeric(adjusted[p50_col], errors="coerce"),
            )

    if {"predicted_points", "predicted_rebounds", "predicted_assists"}.issubset(adjusted.columns):
        adjusted["predicted_pra"] = (
            pd.to_numeric(adjusted["predicted_points"], errors="coerce")
            + pd.to_numeric(adjusted["predicted_rebounds"], errors="coerce")
            + pd.to_numeric(adjusted["predicted_assists"], errors="coerce")
        ).clip(lower=0.0)
        adjusted["v05_projected_pra"] = pd.to_numeric(adjusted["predicted_pra"], errors="coerce").round(3)

    if {"predicted_points_p10", "predicted_rebounds_p10", "predicted_assists_p10"}.issubset(adjusted.columns):
        adjusted["predicted_pra_p10"] = (
            pd.to_numeric(adjusted["predicted_points_p10"], errors="coerce")
            + pd.to_numeric(adjusted["predicted_rebounds_p10"], errors="coerce")
            + pd.to_numeric(adjusted["predicted_assists_p10"], errors="coerce")
        ).clip(lower=0.0)
    if {"predicted_points_p50", "predicted_rebounds_p50", "predicted_assists_p50"}.issubset(adjusted.columns):
        adjusted["predicted_pra_p50"] = (
            pd.to_numeric(adjusted["predicted_points_p50"], errors="coerce")
            + pd.to_numeric(adjusted["predicted_rebounds_p50"], errors="coerce")
            + pd.to_numeric(adjusted["predicted_assists_p50"], errors="coerce")
        ).clip(lower=0.0)
    if {"predicted_points_p90", "predicted_rebounds_p90", "predicted_assists_p90"}.issubset(adjusted.columns):
        adjusted["predicted_pra_p90"] = (
            pd.to_numeric(adjusted["predicted_points_p90"], errors="coerce")
            + pd.to_numeric(adjusted["predicted_rebounds_p90"], errors="coerce")
            + pd.to_numeric(adjusted["predicted_assists_p90"], errors="coerce")
        ).clip(lower=0.0)

    if {
        "predicted_points",
        "predicted_rebounds",
        "predicted_assists",
        "predicted_steals",
        "predicted_blocks",
        "predicted_turnovers",
        "predicted_three_points_made",
    }.issubset(adjusted.columns):
        points = pd.to_numeric(adjusted["predicted_points"], errors="coerce").fillna(0.0)
        rebounds = pd.to_numeric(adjusted["predicted_rebounds"], errors="coerce").fillna(0.0)
        assists = pd.to_numeric(adjusted["predicted_assists"], errors="coerce").fillna(0.0)
        steals = pd.to_numeric(adjusted["predicted_steals"], errors="coerce").fillna(0.0)
        blocks = pd.to_numeric(adjusted["predicted_blocks"], errors="coerce").fillna(0.0)
        turnovers = pd.to_numeric(adjusted["predicted_turnovers"], errors="coerce").fillna(0.0)
        threes = pd.to_numeric(adjusted["predicted_three_points_made"], errors="coerce").fillna(0.0)
        categories_at_ten = (
            pd.concat([points, rebounds, assists, steals, blocks], axis=1) >= 10.0
        ).sum(axis=1)
        double_bonus = (categories_at_ten >= 2).astype(float) * 1.5
        triple_bonus = (categories_at_ten >= 3).astype(float) * 3.0
        adjusted["predicted_draftkings_points"] = (
            points
            + (1.25 * rebounds)
            + (1.5 * assists)
            + (2.0 * steals)
            + (2.0 * blocks)
            - (0.5 * turnovers)
            + (0.5 * threes)
            + double_bonus
            + triple_bonus
        ).clip(lower=0.0)
        adjusted["predicted_fanduel_points"] = (
            points
            + (1.2 * rebounds)
            + (1.5 * assists)
            + (3.0 * steals)
            + (3.0 * blocks)
            - turnovers
        ).clip(lower=0.0)

    adjusted["v05_projection_algorithm"] = "contextual_blend_v05"
    adjusted["v05_coverage_depth_score"] = (coverage_depth * 100.0).round(2)
    adjusted["v05_context_reliability_score"] = (
        (
            starter_prob * 0.2
            + lineup_conf * 0.16
            + minutes_conf * 0.16
            + pregame_lock_conf * 0.14
            + market_quality * 0.14
            + teammate_continuity * 0.1
            + style_confidence * 0.1
        )
        * 100.0
    ).round(2)
    return adjusted


def build_player_board(board_date: str | None = None) -> dict:
    if not DEFAULT_PREDICTIONS_PATH.exists():
        return {
            "board_date": board_date,
            "available_dates": [],
            "generated_at": datetime.now().isoformat(),
            "summary": {
                "total_players": 0,
                "playable_players": 0,
                "non_playable_players": 0,
                "no_bet_players": 0,
                "actionable_players": 0,
                "starters": 0,
                "likely_non_starters": 0,
                "high_confidence_under_10pct": 0,
                "elite_confidence_under_5pct": 0,
                "over_10pct_error": 0,
                "popular_high_confidence": 0,
            },
            "cross_reference": {
                "status": "no_predictions",
                "message": "No predictions file exists yet. Run predictions first.",
                "lines_rows": 0,
                "matched_players": 0,
            },
            "cards": [],
        }

    predictions = pd.read_csv(DEFAULT_PREDICTIONS_PATH)
    if _predictions_fail_sanity(predictions):
        try:
            predict_engine(input_path=DEFAULT_UPCOMING_PATH if DEFAULT_UPCOMING_PATH.exists() else None, predict_all=False)
            predictions = pd.read_csv(DEFAULT_PREDICTIONS_PATH)
        except Exception:
            pass

    if predictions.empty:
        return {
            "board_date": board_date,
            "available_dates": [],
            "generated_at": datetime.now().isoformat(),
            "summary": {
                "total_players": 0,
                "playable_players": 0,
                "non_playable_players": 0,
                "no_bet_players": 0,
                "actionable_players": 0,
                "starters": 0,
                "likely_non_starters": 0,
                "high_confidence_under_10pct": 0,
                "elite_confidence_under_5pct": 0,
                "over_10pct_error": 0,
                "popular_high_confidence": 0,
            },
            "cross_reference": {
                "status": "empty_predictions",
                "message": "Predictions file is empty.",
                "lines_rows": 0,
                "matched_players": 0,
            },
            "cards": [],
        }

    predictions = predictions.copy()
    predictions["game_date"] = pd.to_datetime(predictions["game_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    selected_date, available_dates = _resolve_board_date(predictions, board_date)
    if selected_date and selected_date != "all":
        board = predictions[predictions["game_date"] == selected_date].copy()
    else:
        board = predictions.copy()

    if board.empty and available_dates:
        fallback_date = next((value for value in available_dates if value != "all"), None)
        selected_date = fallback_date
        if fallback_date:
            board = predictions[predictions["game_date"] == fallback_date].copy()

    if board.empty:
        return {
            "board_date": selected_date,
            "available_dates": available_dates,
            "generated_at": datetime.now().isoformat(),
            "summary": {
                "total_players": 0,
                "playable_players": 0,
                "non_playable_players": 0,
                "no_bet_players": 0,
                "actionable_players": 0,
                "starters": 0,
                "likely_non_starters": 0,
                "high_confidence_under_10pct": 0,
                "elite_confidence_under_5pct": 0,
                "over_10pct_error": 0,
                "popular_high_confidence": 0,
            },
            "cross_reference": {
                "status": "no_rows_for_date",
                "message": f"No prediction rows found for {selected_date or 'selected date'}.",
                "lines_rows": 0,
                "matched_players": 0,
            },
            "cards": [],
        }

    hardening_profile = _load_accuracy_hardening_profile()
    min_edge_to_uncertainty = _safe_float(
        hardening_profile.get("min_edge_to_uncertainty"),
        NO_BET_MIN_EDGE_TO_UNCERTAINTY,
    )
    max_projection_error_pct = _safe_float(
        hardening_profile.get("max_projection_error_pct"),
        NO_BET_MAX_PROJECTION_ERROR_PCT,
    )
    min_minutes_confidence = _safe_float(
        hardening_profile.get("min_minutes_confidence"),
        NO_BET_MINUTES_CONFIDENCE,
    )
    min_lineup_confidence = _safe_float(
        hardening_profile.get("min_lineup_status_confidence"),
        NO_BET_MIN_LINEUP_STATUS_CONFIDENCE,
    )
    min_anchor_strength = _safe_float(
        hardening_profile.get("min_pregame_anchor_strength"),
        NO_BET_MIN_PREGAME_ANCHOR_STRENGTH,
    )
    min_line_books_count = _safe_float(
        hardening_profile.get("min_line_books_count"),
        NO_BET_MIN_LINE_BOOKS_COUNT,
    )
    max_line_snapshot_age_minutes = _safe_float(
        hardening_profile.get("max_line_snapshot_age_minutes"),
        NO_BET_MAX_LINE_SNAPSHOT_AGE_MINUTES,
    )
    min_context_freshness_score = _safe_float(
        hardening_profile.get("min_context_freshness_score"),
        0.38,
    )

    board["player_key"] = board["player_name"].map(normalize_player_name)
    board["team_key"] = board["team"].map(normalize_team_code)
    recent_form_team, recent_form_name = _load_recent_form_metrics(selected_date)
    if not recent_form_team.empty:
        board = board.merge(recent_form_team, on=["player_key", "team_key"], how="left")
    if not recent_form_name.empty:
        board = board.merge(recent_form_name, on=["player_key"], how="left", suffixes=("", "_name_fallback"))
        for column in RECENT_FORM_COLUMNS:
            fallback_column = f"{column}_name_fallback"
            if fallback_column in board.columns:
                board[column] = board[column].combine_first(board[fallback_column])
                board = board.drop(columns=[fallback_column])
    for column in RECENT_FORM_COLUMNS:
        if column not in board.columns:
            board[column] = 0.0
        board[column] = pd.to_numeric(board[column], errors="coerce").fillna(0.0)

    if DEFAULT_UPCOMING_PATH.exists():
        upcoming = pd.read_csv(DEFAULT_UPCOMING_PATH)
        if not upcoming.empty:
            upcoming = upcoming.copy()
            upcoming["game_date"] = pd.to_datetime(upcoming["game_date"], errors="coerce").dt.strftime("%Y-%m-%d")
            if selected_date:
                upcoming = upcoming[upcoming["game_date"] == selected_date].copy()
            upcoming["player_key"] = upcoming["player_name"].map(normalize_player_name)
            upcoming["team_key"] = upcoming["team"].map(normalize_team_code)
            merge_columns = [column for column in ["player_key", "team_key", "game_date", "starter", "expected_minutes"] if column in upcoming.columns]
            board = board.merge(
                upcoming[merge_columns].drop_duplicates(subset=["player_key", "team_key", "game_date"], keep="last"),
                on=["player_key", "team_key", "game_date"],
                how="left",
                suffixes=("", "_upcoming"),
            )

    if "starter" not in board.columns:
        board["starter"] = pd.NA
    if "starter_upcoming" in board.columns:
        board["starter"] = board["starter"].combine_first(board["starter_upcoming"])
    if "expected_minutes_upcoming" in board.columns:
        board["expected_minutes"] = board["expected_minutes"].combine_first(board["expected_minutes_upcoming"])

    def _board_series(column_name: str, default_value: object) -> pd.Series:
        if column_name in board.columns:
            return board[column_name]
        return pd.Series(default_value, index=board.index)

    board["starter_numeric"] = pd.to_numeric(board["starter"], errors="coerce")
    board["is_starter"] = board["starter_numeric"].fillna(0).ge(0.5)
    board["starter_probability_numeric"] = pd.to_numeric(
        _board_series("starter_probability", 0.0),
        errors="coerce",
    ).fillna(0.0)
    board["lineup_status_confidence_numeric"] = pd.to_numeric(
        _board_series("lineup_status_confidence", 0.0),
        errors="coerce",
    ).fillna(0.0)
    board["expected_minutes_confidence_numeric"] = pd.to_numeric(
        _board_series("expected_minutes_confidence", 0.0),
        errors="coerce",
    ).fillna(0.0)
    board["pregame_lock_confidence_numeric"] = pd.to_numeric(
        _board_series("pregame_lock_confidence", pd.NA),
        errors="coerce",
    ).fillna(
        (
            board["lineup_status_confidence_numeric"] * 0.55
            + board["expected_minutes_confidence_numeric"] * 0.45
        )
    ).clip(lower=0.0, upper=1.0)
    board["expected_minutes_numeric"] = pd.to_numeric(_board_series("expected_minutes", 0.0), errors="coerce").fillna(0.0)
    board["predicted_minutes_numeric"] = pd.to_numeric(_board_series("predicted_minutes", 0.0), errors="coerce").fillna(0.0)

    status_text = (
        _board_series("injury_status", "").fillna("").astype(str)
        + " "
        + _board_series("health_status", "").fillna("").astype(str)
        + " "
        + _board_series("suspension_status", "").fillna("").astype(str)
    ).str.lower()
    board["unavailable"] = status_text.str.contains(UNAVAILABLE_STATUS_PATTERN, regex=True)
    board.loc[board["unavailable"], "is_starter"] = False
    board["minutes_playable_context"] = board["predicted_minutes_numeric"].where(
        board["predicted_minutes_numeric"].gt(0),
        board["expected_minutes_numeric"],
    ).fillna(0.0)

    def start_label(row: pd.Series) -> str:
        if bool(row.get("unavailable")):
            return "Not Starting (Unavailable)"
        if bool(row.get("is_starter")):
            return "Starter"
        minutes = _safe_float(row.get("expected_minutes_numeric"), 0.0)
        if minutes >= 24:
            return "Likely Starter"
        if minutes >= 15:
            return "Likely Bench"
        return "Not Starting / Unknown"

    board["start_label"] = board.apply(start_label, axis=1)
    low_confidence_mask = _board_series("confidence_flag", "").fillna("").eq("low_confidence")
    board["is_playable"] = (
        (~board["unavailable"])
        & (
            board["is_starter"]
            | board["minutes_playable_context"].ge(16.0)
            | board["starter_probability_numeric"].ge(NO_BET_MIN_STARTER_PROBABILITY)
            | board["pregame_lock_confidence_numeric"].ge(NO_BET_MIN_PREGAME_LOCK_CONFIDENCE)
        )
        & (
            board["is_starter"]
            | board["lineup_status_confidence_numeric"].ge(min_lineup_confidence)
            | board["expected_minutes_confidence_numeric"].ge(min_minutes_confidence)
            | board["pregame_lock_confidence_numeric"].ge(NO_BET_SOFT_RELIEF_MIN_PREGAME_LOCK_CONFIDENCE)
        )
        & ~(low_confidence_mask & ~board["is_starter"] & board["minutes_playable_context"].lt(20.0))
    )
    board = _ensure_pregame_anchor_columns(board)
    board = _apply_v05_projection_algorithm(board)

    line_age_columns = [
        "line_points_snapshot_age_minutes",
        "line_rebounds_snapshot_age_minutes",
        "line_assists_snapshot_age_minutes",
        "line_pra_snapshot_age_minutes",
    ]
    available_line_age_columns = [column for column in line_age_columns if column in board.columns]
    line_age_frame = (
        board[available_line_age_columns].apply(pd.to_numeric, errors="coerce")
        if available_line_age_columns
        else pd.DataFrame(index=board.index)
    )
    min_line_age = (
        line_age_frame.min(axis=1, skipna=True)
        if not line_age_frame.empty
        else pd.Series(float("nan"), index=board.index, dtype=float)
    )
    line_freshness_score = (
        1.0 - min_line_age.fillna(max_line_snapshot_age_minutes).clip(lower=0.0, upper=max_line_snapshot_age_minutes)
        / max(max_line_snapshot_age_minutes, 1.0)
    ).clip(lower=0.0, upper=1.0)
    board["line_data_freshness_score"] = line_freshness_score.round(3)
    news_confidence_numeric = pd.to_numeric(
        _board_series("news_confidence_score", 0.0),
        errors="coerce",
    ).fillna(0.0)
    board["context_freshness_score"] = (
        line_freshness_score * 0.52
        + board["lineup_status_confidence_numeric"] * 0.24
        + board["expected_minutes_confidence_numeric"] * 0.18
        + news_confidence_numeric * 0.06
    ).clip(lower=0.0, upper=1.0).round(3)

    def derive_no_bet_reasons(row: pd.Series) -> list[str]:
        reasons: list[str] = []
        starter_prob = _safe_float(row.get("starter_probability"), 0.0)
        minutes_playable_context = _safe_float(row.get("minutes_playable_context"), 0.0)
        lineup_confidence = _safe_float(row.get("lineup_status_confidence"), 0.0)
        minutes_confidence = _safe_float(row.get("expected_minutes_confidence"), 0.0)
        pregame_lock_confidence = _safe_float(row.get("pregame_lock_confidence"), 0.0)
        projection_error_pct = _safe_float(row.get("projection_error_pct_estimate"), 0.0)
        context_freshness = _safe_float(row.get("context_freshness_score"), 0.0)
        soft_relief_candidate = (
            bool(row.get("is_starter"))
            and starter_prob >= NO_BET_SOFT_RELIEF_MIN_STARTER_PROBABILITY
            and minutes_playable_context >= NO_BET_SOFT_RELIEF_MIN_EXPECTED_MINUTES
            and minutes_confidence >= NO_BET_SOFT_RELIEF_MIN_MINUTES_CONFIDENCE
            and lineup_confidence >= NO_BET_SOFT_RELIEF_MIN_LINEUP_CONFIDENCE
            and pregame_lock_confidence >= NO_BET_SOFT_RELIEF_MIN_PREGAME_LOCK_CONFIDENCE
        )
        if _safe_int(row.get("historical_games_used"), 0) < NO_BET_MIN_HISTORY_GAMES:
            reasons.append("insufficient_history")
        if _safe_float(row.get("points_std_last_10"), 0.0) > NO_BET_POINTS_STD_LAST_10:
            reasons.append("points_volatility")
        if _safe_float(row.get("minutes_std_last_10"), 0.0) > NO_BET_MINUTES_STD_LAST_10:
            reasons.append("minutes_volatility")
        if (
            _safe_float(row.get("starter_rate_last_10"), 0.0) < NO_BET_STARTER_RATE_LAST_10
            and minutes_playable_context < 26.0
            and starter_prob < 0.4
            and not bool(row.get("is_starter"))
        ):
            reasons.append("role_instability")
        if (
            not bool(row.get("is_starter"))
            and minutes_playable_context < 28.0
            and starter_prob < NO_BET_MIN_STARTER_PROBABILITY
            and lineup_confidence < min_lineup_confidence
            and pregame_lock_confidence < NO_BET_MIN_PREGAME_LOCK_CONFIDENCE
        ):
            reasons.append("lineup_uncertainty")
        if (
            minutes_confidence < min_minutes_confidence
            and minutes_playable_context < 22.0
            and pregame_lock_confidence < NO_BET_SOFT_RELIEF_MIN_PREGAME_LOCK_CONFIDENCE
        ):
            reasons.append("low_minutes_confidence")
        if _safe_float(row.get("minutes_projection_error_estimate"), 0.0) > NO_BET_MINUTES_PROJECTION_ERROR:
            reasons.append("minutes_projection_unstable")
        severe_projection_error = projection_error_pct > (max_projection_error_pct + NO_BET_PROJECTION_ERROR_GRACE)
        if projection_error_pct > max_projection_error_pct and (severe_projection_error or not soft_relief_candidate):
            reasons.append("projection_uncertainty")
        if _safe_float(row.get("pregame_anchor_strength"), 0.0) < min_anchor_strength:
            reasons.append("weak_market_anchor")
        line_book_counts = [
            _safe_float(row.get("line_points_books_count"), float("nan")),
            _safe_float(row.get("line_rebounds_books_count"), float("nan")),
            _safe_float(row.get("line_assists_books_count"), float("nan")),
            _safe_float(row.get("line_pra_books_count"), float("nan")),
        ]
        observed_books = [value for value in line_book_counts if not pd.isna(value)]
        if observed_books and max(observed_books) < min_line_books_count:
            reasons.append("weak_market_anchor")
        line_ages = [
            _safe_float(row.get("line_points_snapshot_age_minutes"), float("nan")),
            _safe_float(row.get("line_rebounds_snapshot_age_minutes"), float("nan")),
            _safe_float(row.get("line_assists_snapshot_age_minutes"), float("nan")),
            _safe_float(row.get("line_pra_snapshot_age_minutes"), float("nan")),
        ]
        observed_ages = [value for value in line_ages if not pd.isna(value)]
        if observed_ages and min(observed_ages) > max_line_snapshot_age_minutes:
            reasons.append("stale_market_snapshot")
        if (
            context_freshness < min_context_freshness_score
            and pregame_lock_confidence < NO_BET_SOFT_RELIEF_MIN_PREGAME_LOCK_CONFIDENCE
            and (not soft_relief_candidate or not bool(row.get("is_starter")))
        ):
            reasons.append("stale_context_data")
        if _safe_float(row.get("injury_risk_score"), 0.0) >= NO_BET_INJURY_RISK:
            reasons.append("injury_risk")
        if (
            _safe_float(row.get("news_risk_score"), 0.0) >= NO_BET_NEWS_RISK
            and (
                not bool(row.get("is_starter"))
                or _safe_float(row.get("news_confidence_score"), 0.0) < 0.4
            )
        ):
            reasons.append("news_risk")
        if (
            str(row.get("confidence_flag", "")).lower() == "low_confidence"
            and _safe_int(row.get("historical_games_used"), 0) < 5
            and minutes_playable_context < 20.0
            and not bool(row.get("is_starter"))
        ):
            reasons.append("model_low_confidence")
        if bool(row.get("prediction_quality_blocked", False)):
            reasons.append("quality_gate_blocked")
        if bool(row.get("unavailable")):
            reasons.append("unavailable")
        return reasons

    board["no_bet_reasons"] = board.apply(derive_no_bet_reasons, axis=1)
    board["no_bet_score"] = board["no_bet_reasons"].map(
        lambda reasons: float(sum(NO_BET_REASON_WEIGHTS.get(reason, 1.0) for reason in set(reasons)))
    )

    def resolve_no_bet(row: pd.Series) -> bool:
        reason_set = set(row.get("no_bet_reasons", []) or [])
        score = _safe_float(row.get("no_bet_score"), 0.0)
        starter_prob = _safe_float(row.get("starter_probability"), 0.0)
        minutes_playable_context = _safe_float(row.get("minutes_playable_context"), 0.0)
        minutes_confidence = _safe_float(row.get("expected_minutes_confidence"), 0.0)
        lineup_confidence = _safe_float(row.get("lineup_status_confidence"), 0.0)
        pregame_lock_confidence = _safe_float(row.get("pregame_lock_confidence"), 0.0)
        if not reason_set:
            return False
        if reason_set.intersection(NO_BET_HARD_REASONS):
            return True
        if reason_set.issubset(NO_BET_SOFT_REASONS):
            soft_relief = (
                bool(row.get("is_starter"))
                and starter_prob >= NO_BET_SOFT_RELIEF_MIN_STARTER_PROBABILITY
                and minutes_playable_context >= NO_BET_SOFT_RELIEF_MIN_EXPECTED_MINUTES
                and minutes_confidence >= NO_BET_SOFT_RELIEF_MIN_MINUTES_CONFIDENCE
                and lineup_confidence >= NO_BET_SOFT_RELIEF_MIN_LINEUP_CONFIDENCE
                and pregame_lock_confidence >= NO_BET_SOFT_RELIEF_MIN_PREGAME_LOCK_CONFIDENCE
            )
            if soft_relief:
                return False
            return score >= NO_BET_SOFT_SCORE_THRESHOLD
        return score >= NO_BET_SCORE_THRESHOLD

    board["no_bet"] = board.apply(resolve_no_bet, axis=1)
    board["no_bet_reason_text"] = board["no_bet_reasons"].map(
        lambda reasons: ", ".join(NO_BET_REASON_LABELS.get(reason, reason) for reason in reasons)
    )
    hard_block_mask = board["no_bet_reasons"].map(
        lambda reasons: bool(set(reasons or []).intersection(NO_BET_HARD_REASONS))
    )
    board["hard_no_bet"] = hard_block_mask
    actionable_count = int((board["is_playable"] & ~board["no_bet"]).sum())
    available_players = int((~board["unavailable"]).sum())
    starter_candidates = int((~board["unavailable"] & board["is_starter"]).sum())
    starter_floor = min(NO_BET_MIN_ACTIONABLE_STARTERS, max(starter_candidates, 0))
    actionable_floor_target = min(
        max(
            NO_BET_ACTIONABLE_FLOOR_COUNT,
            int(round(available_players * NO_BET_ACTIONABLE_FLOOR_RATIO)),
            starter_floor,
        ),
        max(available_players, 0),
    )
    if actionable_count < actionable_floor_target:
        override_pool = board[
            board["no_bet"]
            & (~board["hard_no_bet"])
            & (~board["unavailable"])
            & (
                board["is_playable"]
                | board["is_starter"]
                | board["starter_probability_numeric"].ge(0.42)
                | board["minutes_playable_context"].ge(16.0)
            )
        ].copy()
        if not override_pool.empty:
            def _override_numeric(column: str, default: float = 0.0) -> pd.Series:
                if column in override_pool.columns:
                    return pd.to_numeric(override_pool[column], errors="coerce")
                return pd.Series(default, index=override_pool.index, dtype=float)

            quality_score = _override_numeric("prediction_quality_score", 0.0).fillna(0.0)
            projection_error = _override_numeric("projection_error_pct_estimate", np.nan).fillna(
                _override_numeric("error_pct_estimate", 35.0).fillna(35.0)
            )
            override_pool["override_rank"] = (
                (override_pool["starter_probability_numeric"] * 1.4)
                + (override_pool["expected_minutes_confidence_numeric"] * 1.15)
                + (override_pool["lineup_status_confidence_numeric"] * 1.0)
                + (override_pool["pregame_lock_confidence_numeric"] * 1.2)
                + (quality_score * 1.5)
                + ((override_pool["minutes_playable_context"] / 32.0).clip(lower=0.0, upper=1.0) * 0.8)
                - (override_pool["no_bet_score"] * 0.55)
                - ((projection_error / 40.0).clip(lower=0.0, upper=2.0) * 0.75)
            )
            override_pool = override_pool.sort_values(
                ["override_rank", "no_bet_score"],
                ascending=[False, True],
            )
            needed = max(0, actionable_floor_target - actionable_count)
            override_index = override_pool.head(needed).index
            if len(override_index) > 0:
                board.loc[override_index, "no_bet"] = False
                board.loc[override_index, "is_playable"] = True
                board.loc[override_index, "no_bet_reason_text"] = board.loc[
                    override_index, "no_bet_reason_text"
                ].map(
                    lambda text: "watchlist_override"
                    if not str(text or "").strip()
                    else f"{text}, watchlist_override"
                )

    board["is_actionable"] = board["is_playable"] & ~board["no_bet"]

    target_error_pct = _load_target_error_pct_profile()
    metric_mae = _load_target_mae()
    confidence_fallback_map = {
        "high_confidence": 10.0,
        "medium_confidence": 17.0,
        "low_confidence": 26.0,
    }

    def estimate_error_pct(row: pd.Series) -> float:
        model_error_pct = _safe_float(row.get("projection_error_pct_estimate"), 0.0)
        if model_error_pct > 0:
            base_error = model_error_pct
        else:
            parts: list[float] = []
            for target in ["points", "rebounds", "assists"]:
                prediction_column = f"predicted_{target}"
                if prediction_column not in row or pd.isna(row[prediction_column]):
                    continue
                prediction_value = abs(_safe_float(row[prediction_column], 0.0))
                denominator = max(prediction_value, 1.0)
                mae = metric_mae.get(target, 0.0)
                historical_pct = _safe_float(target_error_pct.get(target), 0.0)
                mae_based_pct = (mae / denominator) * 100.0 if mae > 0 else None

                if mae_based_pct is not None:
                    if historical_pct > 0:
                        parts.append(min(mae_based_pct, historical_pct))
                    else:
                        parts.append(mae_based_pct)
                elif historical_pct > 0:
                    parts.append(historical_pct)
            if parts:
                base_error = float(sum(parts) / len(parts))
            else:
                confidence_flag = str(row.get("confidence_flag", "low_confidence"))
                base_error = float(confidence_fallback_map.get(confidence_flag, 26.0))

        quality_score = _safe_float(row.get("prediction_quality_score"), 1.0)
        quality_penalty = max(0.0, (0.85 - quality_score) * 20.0)

        games_used = _safe_int(row.get("historical_games_used"), 0)
        confidence_flag = str(row.get("confidence_flag", "low_confidence"))
        history_bonus = min(games_used, 30) * 0.22
        confidence_bonus = {
            "high_confidence": 2.5,
            "medium_confidence": 1.0,
            "low_confidence": 0.0,
        }.get(confidence_flag, 0.0)
        starter_bonus = 1.0 if bool(row.get("is_starter")) else 0.0
        nonstarter_penalty = 2.5 if str(row.get("start_label", "")).lower().startswith("not starting") else 0.0
        unavailable_penalty = 6.0 if bool(row.get("unavailable")) else 0.0
        predicted_minutes = _safe_float(row.get("predicted_minutes"), _safe_float(row.get("expected_minutes_numeric"), 0.0))
        if predicted_minutes < 8:
            minutes_penalty = 12.0
        elif predicted_minutes < 14:
            minutes_penalty = 7.0
        elif predicted_minutes < 20:
            minutes_penalty = 3.0
        else:
            minutes_penalty = 0.0

        adjusted_error = (
            base_error
            - history_bonus
            - confidence_bonus
            - starter_bonus
            + nonstarter_penalty
            + unavailable_penalty
            + minutes_penalty
            + quality_penalty
        )
        adjusted_error = max(4.0, min(45.0, adjusted_error))
        return float(round(adjusted_error, 1))

    board["error_pct_estimate"] = board.apply(estimate_error_pct, axis=1).clip(lower=1.0, upper=99.0)
    board.loc[board["no_bet"], "error_pct_estimate"] = (
        board.loc[board["no_bet"], "error_pct_estimate"] + 8.0
    ).clip(lower=8.0, upper=70.0)
    board["confidence_pct"] = (100.0 - board["error_pct_estimate"]).clip(lower=1.0, upper=99.0).round(1)

    def error_band(error_pct: float) -> str:
        if error_pct <= 5.0:
            return "elite_5pct"
        if error_pct <= 10.0:
            return "high_10pct"
        if error_pct <= 15.0:
            return "medium_15pct"
        return "high_error"

    board["error_band"] = board["error_pct_estimate"].map(error_band)

    popularity_metric = pd.to_numeric(_board_series("predicted_draftkings_points", pd.NA), errors="coerce").fillna(
        pd.to_numeric(_board_series("predicted_pra", 0.0), errors="coerce").fillna(0.0)
    )
    board["popularity_metric"] = popularity_metric
    popular_candidates = board[
        board["is_starter"]
        & board["error_pct_estimate"].le(10.0)
        & ~board["no_bet"]
    ].sort_values(["popularity_metric", "confidence_pct"], ascending=[False, False])
    popular_keys = set(popular_candidates.head(20)["player_key"].tolist())
    board["popular_high_confidence"] = board["player_key"].isin(popular_keys)

    cross_reference_status = "no_lines_file"
    cross_reference_message = (
        "No PrizePicks lines were available for cross-reference. "
        "PrizePicks board scraping is blocked by anti-bot controls; import lines manually."
    )
    lines_rows = 0
    matched_players = 0
    line_map: dict[tuple[str, str], list[dict]] = {}

    if DEFAULT_PRIZEPICKS_LINES_PATH.exists():
        lines = pd.read_csv(DEFAULT_PRIZEPICKS_LINES_PATH)
        lines_rows = int(len(lines))
        if not lines.empty:
            lines = lines.copy()
            lines["game_date"] = pd.to_datetime(lines["game_date"], errors="coerce").dt.strftime("%Y-%m-%d")
            if selected_date:
                lines = lines[lines["game_date"] == selected_date].copy()
            if not lines.empty:
                lines["player_key"] = lines["player_name"].map(normalize_player_name)
                lines["team_key"] = lines["team"].map(normalize_team_code)
                for row in lines.itertuples(index=False):
                    key = (str(row.player_key or ""), str(row.team_key or ""))
                    line_map.setdefault(key, []).append(
                        {
                            "market": str(getattr(row, "market", "")).lower(),
                            "line": _safe_float(getattr(row, "line", None), default=float("nan")),
                            "selection_type": str(getattr(row, "selection_type", "") or ""),
                            "source": str(getattr(row, "source", "") or ""),
                        }
                    )
                cross_reference_status = "lines_loaded_for_date"
                cross_reference_message = f"Cross-referencing imported PrizePicks lines for {selected_date}."
            else:
                cross_reference_status = "lines_missing_for_date"
                cross_reference_message = (
                    f"PrizePicks lines file has data, but no rows for {selected_date}. "
                    "Import lines for that date to enable card-level comparisons."
                )
        else:
            cross_reference_status = "lines_file_empty"
            cross_reference_message = "PrizePicks lines file exists but is empty."

    def build_market_references(row: pd.Series) -> list[dict]:
        key = (str(row.get("player_key", "")), str(row.get("team_key", "")))
        candidates = line_map.get(key, [])
        if not candidates and key[0]:
            # Fallback: name-only match when team is missing in imported lines.
            candidates = line_map.get((key[0], ""), [])
        references: list[dict] = []
        for entry in candidates:
            market = str(entry.get("market", "")).lower()
            projection_column = MARKET_TO_COLUMN.get(market)
            if not projection_column or projection_column not in row:
                continue
            line_value = entry.get("line")
            if pd.isna(line_value):
                continue
            projection_value = _safe_float(row.get(projection_column), default=float("nan"))
            if pd.isna(projection_value):
                continue
            edge_value = projection_value - float(line_value)
            threshold = float(EDGE_THRESHOLDS.get(market, 1.0))
            uncertainty_band = _edge_uncertainty_band_for_market(
                row,
                market,
                projection_value,
                threshold,
            )
            edge_to_uncertainty = float(abs(edge_value) / max(uncertainty_band, 1e-6))
            required_edge_ratio = float(min_edge_to_uncertainty)
            if _safe_float(row.get("context_freshness_score"), 0.0) < min_context_freshness_score:
                required_edge_ratio += 0.2
            if _safe_float(row.get("line_data_freshness_score"), 0.0) < 0.25:
                required_edge_ratio += 0.15
            if bool(row.get("no_bet", False)):
                recommendation = "Pass"
            elif edge_to_uncertainty < required_edge_ratio:
                recommendation = "Pass"
            elif edge_value >= uncertainty_band:
                recommendation = "Higher"
            elif edge_value <= -uncertainty_band:
                recommendation = "Lower"
            else:
                recommendation = "Pass"
            references.append(
                {
                    "market": market,
                    "line": round(float(line_value), 2),
                    "projection": round(float(projection_value), 2),
                    "edge": round(float(edge_value), 2),
                    "edge_threshold": round(float(threshold), 2),
                    "uncertainty_band": round(float(uncertainty_band), 2),
                    "edge_to_uncertainty": round(edge_to_uncertainty, 3),
                    "required_edge_to_uncertainty": round(required_edge_ratio, 3),
                    "recommendation": recommendation,
                }
            )
        return sorted(references, key=lambda item: item["market"])

    def _lineup_conditional_projection(base_projection: float, row: dict[str, object]) -> dict[str, float]:
        starter_prob = max(0.0, min(1.0, _safe_float(row.get("starter_probability"), 0.0)))
        lineup_conf = max(0.0, min(1.0, _safe_float(row.get("lineup_status_confidence"), 0.0)))
        lock_conf = max(0.0, min(1.0, _safe_float(row.get("pregame_lock_confidence"), 0.0)))
        start_uplift = 0.04 + (1.0 - starter_prob) * 0.12 + lineup_conf * 0.05 + lock_conf * 0.04
        bench_penalty = 0.15 + starter_prob * 0.08 + (1.0 - lineup_conf) * 0.07 + (1.0 - lock_conf) * 0.05
        as_starter = max(0.0, base_projection * (1.0 + start_uplift))
        as_non_starter = max(0.0, base_projection * (1.0 - bench_penalty))
        return {
            "as_starter": round(as_starter, 2),
            "as_non_starter": round(as_non_starter, 2),
            "starter_prob": round(starter_prob, 3),
        }

    def _teammate_scenario_projection(base_projection: float, row: dict[str, object], market: str) -> dict[str, float]:
        vacancy = max(0.0, _safe_float(row.get("teammate_usage_vacancy"), 0.0))
        continuity = max(0.0, min(1.0, _safe_float(row.get("teammate_continuity_score"), 0.6)))
        star_out = 1.0 if bool(_safe_float(row.get("teammate_star_out_flag"), 0.0) >= 0.5) else 0.0
        synergy_map = {
            "points": _safe_float(row.get("teammate_synergy_points"), 0.0),
            "rebounds": _safe_float(row.get("teammate_synergy_rebounds"), 0.0),
            "assists": _safe_float(row.get("teammate_synergy_assists"), 0.0),
            "pra": (
                _safe_float(row.get("teammate_synergy_points"), 0.0)
                + _safe_float(row.get("teammate_synergy_rebounds"), 0.0)
                + _safe_float(row.get("teammate_synergy_assists"), 0.0)
            ),
        }
        synergy = float(synergy_map.get(market, 0.0))
        delta = (
            base_projection * (0.02 + 0.03 * min(vacancy, 1.8))
            + 0.45 * synergy
            + 0.5 * star_out
            - 0.6 * max(0.0, 1.0 - continuity)
        )
        if_teammate_out = max(0.0, base_projection + delta)
        if_teammate_in = max(0.0, base_projection - (delta * 0.82))
        return {
            "if_out": round(if_teammate_out, 2),
            "if_in": round(if_teammate_in, 2),
        }

    def _confidence_decomposition(row: dict[str, object], market: str) -> dict[str, float]:
        line_books = _safe_float(row.get(f"line_{market}_books_count"), 0.0) if market != "pra" else _safe_float(row.get("line_pra_books_count"), 0.0)
        line_age = _safe_float(row.get(f"line_{market}_snapshot_age_minutes"), float("nan")) if market != "pra" else _safe_float(row.get("line_pra_snapshot_age_minutes"), float("nan"))
        line_fresh = 0.0 if pd.isna(line_age) else max(0.0, min(1.0, 1.0 - line_age / 360.0))
        market_data = max(0.0, min(1.0, min(line_books, 5.0) / 5.0 * 0.6 + line_fresh * 0.4))
        minutes_conf = max(0.0, min(1.0, _safe_float(row.get("expected_minutes_confidence"), 0.0)))
        lineup_conf = max(0.0, min(1.0, _safe_float(row.get("lineup_status_confidence"), 0.0)))
        lock_conf = max(0.0, min(1.0, _safe_float(row.get("pregame_lock_confidence"), 0.0)))
        error_pct = max(0.0, min(80.0, _safe_float(row.get("projection_error_pct_estimate"), _safe_float(row.get("error_pct_estimate"), 25.0))))
        model_conf = max(0.0, min(1.0, 1.0 - error_pct / 100.0))

        weight_map = {
            "points": {"minutes": 0.24, "lineup": 0.2, "lock": 0.18, "market": 0.22, "model": 0.16},
            "rebounds": {"minutes": 0.3, "lineup": 0.18, "lock": 0.14, "market": 0.2, "model": 0.18},
            "assists": {"minutes": 0.22, "lineup": 0.24, "lock": 0.16, "market": 0.2, "model": 0.18},
            "pra": {"minutes": 0.25, "lineup": 0.2, "lock": 0.16, "market": 0.2, "model": 0.19},
        }
        weights = weight_map[market]
        score = (
            minutes_conf * weights["minutes"]
            + lineup_conf * weights["lineup"]
            + lock_conf * weights["lock"]
            + market_data * weights["market"]
            + model_conf * weights["model"]
        )
        return {
            "minutes": round(minutes_conf, 3),
            "lineup": round(lineup_conf, 3),
            "lock": round(lock_conf, 3),
            "market_data": round(market_data, 3),
            "model": round(model_conf, 3),
            "score_pct": round(score * 100.0, 1),
        }

    cards: list[dict] = []
    for row in board.itertuples(index=False):
        row_dict = row._asdict()
        market_references = build_market_references(pd.Series(row_dict))
        if market_references:
            matched_players += 1
        base_points = max(0.0, _safe_float(row_dict.get("predicted_points"), 0.0))
        base_rebounds = max(0.0, _safe_float(row_dict.get("predicted_rebounds"), 0.0))
        base_assists = max(0.0, _safe_float(row_dict.get("predicted_assists"), 0.0))
        base_pra = max(0.0, _safe_float(row_dict.get("predicted_pra"), base_points + base_rebounds + base_assists))
        lineup_scenarios = {
            "points": _lineup_conditional_projection(base_points, row_dict),
            "rebounds": _lineup_conditional_projection(base_rebounds, row_dict),
            "assists": _lineup_conditional_projection(base_assists, row_dict),
            "pra": _lineup_conditional_projection(base_pra, row_dict),
        }
        teammate_scenarios = {
            "points": _teammate_scenario_projection(base_points, row_dict, "points"),
            "rebounds": _teammate_scenario_projection(base_rebounds, row_dict, "rebounds"),
            "assists": _teammate_scenario_projection(base_assists, row_dict, "assists"),
            "pra": _teammate_scenario_projection(base_pra, row_dict, "pra"),
        }
        confidence_decomposition = {
            "points": _confidence_decomposition(row_dict, "points"),
            "rebounds": _confidence_decomposition(row_dict, "rebounds"),
            "assists": _confidence_decomposition(row_dict, "assists"),
            "pra": _confidence_decomposition(row_dict, "pra"),
        }

        cards.append(
            {
                "player_name": str(row_dict.get("player_name", "")),
                "team": str(row_dict.get("team", "")),
                "opponent": str(row_dict.get("opponent", "")),
                "game_date": str(row_dict.get("game_date", "")),
                "home": bool(_safe_int(row_dict.get("home"), 0)),
                "position": str(row_dict.get("position", "") or ""),
                "is_starter": bool(row_dict.get("is_starter", False)),
                "start_label": str(row_dict.get("start_label", "Not Starting / Unknown")),
                "is_playable": bool(row_dict.get("is_playable", False)),
                "is_actionable": bool(row_dict.get("is_actionable", False)),
                "no_bet": bool(row_dict.get("no_bet", False)),
                "no_bet_score": round(_safe_float(row_dict.get("no_bet_score"), 0.0), 2),
                "no_bet_reasons": list(row_dict.get("no_bet_reasons", [])) if isinstance(row_dict.get("no_bet_reasons"), list) else [],
                "no_bet_reason_text": str(row_dict.get("no_bet_reason_text", "") or ""),
                "live_projection_in_game_flag": bool(_safe_int(row_dict.get("live_projection_in_game_flag"), 0)),
                "live_projection_source": str(row_dict.get("live_projection_source", "") or ""),
                "live_projection_updated_at": str(row_dict.get("live_projection_updated_at", "") or ""),
                "live_game_id": str(row_dict.get("live_game_id", "") or ""),
                "live_minutes_played": round(_safe_float(row_dict.get("live_minutes_played"), 0.0), 3),
                "live_points_current": round(_safe_float(row_dict.get("live_points_current"), 0.0), 3),
                "live_rebounds_current": round(_safe_float(row_dict.get("live_rebounds_current"), 0.0), 3),
                "live_assists_current": round(_safe_float(row_dict.get("live_assists_current"), 0.0), 3),
                "live_steals_current": round(_safe_float(row_dict.get("live_steals_current"), 0.0), 3),
                "live_blocks_current": round(_safe_float(row_dict.get("live_blocks_current"), 0.0), 3),
                "live_turnovers_current": round(_safe_float(row_dict.get("live_turnovers_current"), 0.0), 3),
                "live_three_points_current": round(_safe_float(row_dict.get("live_three_points_current"), 0.0), 3),
                "starter_probability": round(_safe_float(row_dict.get("starter_probability"), 0.0), 3),
                "starter_certainty": round(_safe_float(row_dict.get("starter_certainty"), 0.0), 3),
                "pregame_lock_confidence": round(_safe_float(row_dict.get("pregame_lock_confidence"), 0.0), 3),
                "pregame_lock_tier": str(row_dict.get("pregame_lock_tier", "") or ""),
                "pregame_lock_window_stage": str(row_dict.get("pregame_lock_window_stage", "") or ""),
                "pregame_lock_minutes_to_tipoff": round(_safe_float(row_dict.get("pregame_lock_minutes_to_tipoff"), float("nan")), 2),
                "pregame_lock_window_weight": round(_safe_float(row_dict.get("pregame_lock_window_weight"), 0.0), 3),
                "commence_time_utc": str(row_dict.get("commence_time_utc", "") or ""),
                "pregame_line_freshness_score": round(_safe_float(row_dict.get("pregame_line_freshness_score"), 0.0), 3),
                "pregame_min_line_age_minutes": round(_safe_float(row_dict.get("pregame_min_line_age_minutes"), float("nan")), 2),
                "lineup_status_label": str(row_dict.get("lineup_status_label", "") or ""),
                "lineup_status_confidence": round(_safe_float(row_dict.get("lineup_status_confidence"), 0.0), 3),
                "injury_status": str(row_dict.get("injury_status", "") or ""),
                "health_status": str(row_dict.get("health_status", "") or ""),
                "suspension_status": str(row_dict.get("suspension_status", "") or ""),
                "injury_risk_score": round(_safe_float(row_dict.get("injury_risk_score"), 0.0), 3),
                "injury_minutes_multiplier": round(_safe_float(row_dict.get("injury_minutes_multiplier"), 1.0), 3),
                "home_court_points_boost": round(_safe_float(row_dict.get("home_court_points_boost"), 0.0), 3),
                "home_court_minutes_boost": round(_safe_float(row_dict.get("home_court_minutes_boost"), 0.0), 3),
                "hometown_game_flag": bool(_safe_float(row_dict.get("hometown_game_flag"), 0.0) >= 0.5),
                "hometown_advantage_score": round(_safe_float(row_dict.get("hometown_advantage_score"), 0.0), 3),
                "teammate_usage_vacancy": round(_safe_float(row_dict.get("teammate_usage_vacancy"), 0.0), 3),
                "teammate_continuity_score": round(_safe_float(row_dict.get("teammate_continuity_score"), 0.0), 3),
                "teammate_star_out_flag": bool(_safe_float(row_dict.get("teammate_star_out_flag"), 0.0) >= 0.5),
                "teammate_synergy_points": round(_safe_float(row_dict.get("teammate_synergy_points"), 0.0), 3),
                "teammate_synergy_rebounds": round(_safe_float(row_dict.get("teammate_synergy_rebounds"), 0.0), 3),
                "teammate_synergy_assists": round(_safe_float(row_dict.get("teammate_synergy_assists"), 0.0), 3),
                "teammate_on_off_points_delta": round(_safe_float(row_dict.get("teammate_on_off_points_delta"), 0.0), 3),
                "teammate_on_off_rebounds_delta": round(_safe_float(row_dict.get("teammate_on_off_rebounds_delta"), 0.0), 3),
                "teammate_on_off_assists_delta": round(_safe_float(row_dict.get("teammate_on_off_assists_delta"), 0.0), 3),
                "shot_style_arc_label": str(row_dict.get("shot_style_arc_label", "") or ""),
                "shot_style_arc_score": round(_safe_float(row_dict.get("shot_style_arc_score"), 0.0), 3),
                "shot_style_release_label": str(row_dict.get("shot_style_release_label", "") or ""),
                "shot_style_release_score": round(_safe_float(row_dict.get("shot_style_release_score"), 0.0), 3),
                "shot_style_volume_index": round(_safe_float(row_dict.get("shot_style_volume_index"), 0.0), 3),
                "shot_style_miss_pressure": round(_safe_float(row_dict.get("shot_style_miss_pressure"), 0.0), 3),
                "team_shot_miss_pressure": round(_safe_float(row_dict.get("team_shot_miss_pressure"), 0.0), 3),
                "opponent_shot_miss_pressure": round(_safe_float(row_dict.get("opponent_shot_miss_pressure"), 0.0), 3),
                "opponent_avg_height_inches": round(_safe_float(row_dict.get("opponent_avg_height_inches"), 0.0), 3),
                "opponent_height_advantage_inches": round(_safe_float(row_dict.get("opponent_height_advantage_inches"), 0.0), 3),
                "shot_style_tall_mismatch_penalty": round(_safe_float(row_dict.get("shot_style_tall_mismatch_penalty"), 0.0), 3),
                "shot_style_pace_bonus": round(_safe_float(row_dict.get("shot_style_pace_bonus"), 0.0), 3),
                "shot_style_rebound_environment": round(_safe_float(row_dict.get("shot_style_rebound_environment"), 0.0), 3),
                "shot_style_points_factor": round(_safe_float(row_dict.get("shot_style_points_factor"), 1.0), 4),
                "shot_style_three_points_factor": round(_safe_float(row_dict.get("shot_style_three_points_factor"), 1.0), 4),
                "shot_style_rebounds_factor": round(_safe_float(row_dict.get("shot_style_rebounds_factor"), 1.0), 4),
                "shot_style_assists_factor": round(_safe_float(row_dict.get("shot_style_assists_factor"), 1.0), 4),
                "shot_style_turnovers_factor": round(_safe_float(row_dict.get("shot_style_turnovers_factor"), 1.0), 4),
                "playstyle_shot_profile_source": str(row_dict.get("playstyle_shot_profile_source", "") or ""),
                "playstyle_primary_role": str(row_dict.get("playstyle_primary_role", "") or ""),
                "playstyle_scoring_mode": str(row_dict.get("playstyle_scoring_mode", "") or ""),
                "playstyle_rim_rate": round(_safe_float(row_dict.get("playstyle_rim_rate"), 0.0), 4),
                "playstyle_mid_range_rate": round(_safe_float(row_dict.get("playstyle_mid_range_rate"), 0.0), 4),
                "playstyle_three_rate": round(_safe_float(row_dict.get("playstyle_three_rate"), 0.0), 4),
                "playstyle_catch_shoot_rate": round(_safe_float(row_dict.get("playstyle_catch_shoot_rate"), 0.0), 4),
                "playstyle_pull_up_rate": round(_safe_float(row_dict.get("playstyle_pull_up_rate"), 0.0), 4),
                "playstyle_drive_rate": round(_safe_float(row_dict.get("playstyle_drive_rate"), 0.0), 4),
                "playstyle_assist_potential": round(_safe_float(row_dict.get("playstyle_assist_potential"), 0.0), 4),
                "playstyle_paint_touch_rate": round(_safe_float(row_dict.get("playstyle_paint_touch_rate"), 0.0), 4),
                "playstyle_post_touch_rate": round(_safe_float(row_dict.get("playstyle_post_touch_rate"), 0.0), 4),
                "playstyle_elbow_touch_rate": round(_safe_float(row_dict.get("playstyle_elbow_touch_rate"), 0.0), 4),
                "playstyle_rebound_chance_rate": round(_safe_float(row_dict.get("playstyle_rebound_chance_rate"), 0.0), 4),
                "playstyle_offball_activity_rate": round(_safe_float(row_dict.get("playstyle_offball_activity_rate"), 0.0), 4),
                "playstyle_usage_proxy": round(_safe_float(row_dict.get("playstyle_usage_proxy"), 0.0), 4),
                "playstyle_defensive_event_rate": round(_safe_float(row_dict.get("playstyle_defensive_event_rate"), 0.0), 4),
                "playstyle_context_confidence": round(_safe_float(row_dict.get("playstyle_context_confidence"), 0.0), 4),
                "news_article_count_24h": round(_safe_float(row_dict.get("news_article_count_24h"), 0.0), 3),
                "news_risk_score": round(_safe_float(row_dict.get("news_risk_score"), 0.0), 3),
                "news_confidence_score": round(_safe_float(row_dict.get("news_confidence_score"), 0.0), 3),
                "expected_minutes_confidence": round(_safe_float(row_dict.get("expected_minutes_confidence"), 0.0), 3),
                "minutes_projection_error_estimate": round(_safe_float(row_dict.get("minutes_projection_error_estimate"), 0.0), 3),
                "points_std_last_10": round(_safe_float(row_dict.get("points_std_last_10"), 0.0), 3),
                "rebounds_std_last_10": round(_safe_float(row_dict.get("rebounds_std_last_10"), 0.0), 3),
                "assists_std_last_10": round(_safe_float(row_dict.get("assists_std_last_10"), 0.0), 3),
                "minutes_std_last_10": round(_safe_float(row_dict.get("minutes_std_last_10"), 0.0), 3),
                "starter_rate_last_10": round(_safe_float(row_dict.get("starter_rate_last_10"), 0.0), 3),
                "historical_games_used": _safe_int(row_dict.get("historical_games_used"), 0),
                "season_priors_available": bool(row_dict.get("season_priors_available", False)),
                "prediction_quality_score": round(_safe_float(row_dict.get("prediction_quality_score"), 1.0), 3),
                "prediction_quality_blocked": bool(row_dict.get("prediction_quality_blocked", False)),
                "prediction_quality_issues": str(row_dict.get("prediction_quality_issues", "") or ""),
                "confidence_flag": str(row_dict.get("confidence_flag", "")),
                "confidence_pct": _safe_float(row_dict.get("confidence_pct"), 0.0),
                "error_pct_estimate": _safe_float(row_dict.get("error_pct_estimate"), 0.0),
                "projection_error_pct_estimate": _safe_float(row_dict.get("projection_error_pct_estimate"), 0.0),
                "projection_confidence_pct": _safe_float(row_dict.get("projection_confidence_pct"), 0.0),
                "v05_projection_algorithm": str(row_dict.get("v05_projection_algorithm", "") or ""),
                "v05_coverage_depth_score": _safe_float(row_dict.get("v05_coverage_depth_score"), 0.0),
                "v05_context_reliability_score": _safe_float(row_dict.get("v05_context_reliability_score"), 0.0),
                "error_band": str(row_dict.get("error_band", "high_error")),
                "popular_high_confidence": bool(row_dict.get("popular_high_confidence", False)),
                "v05_projected_points": round(_safe_float(row_dict.get("v05_projected_points"), float("nan")), 2),
                "v05_projected_rebounds": round(_safe_float(row_dict.get("v05_projected_rebounds"), float("nan")), 2),
                "v05_projected_assists": round(_safe_float(row_dict.get("v05_projected_assists"), float("nan")), 2),
                "v05_projected_pra": round(_safe_float(row_dict.get("v05_projected_pra"), float("nan")), 2),
                "projected_points": round(_safe_float(row_dict.get("predicted_points"), 0.0), 2),
                "projected_points_p10": round(_safe_float(row_dict.get("predicted_points_p10"), float("nan")), 2),
                "projected_points_p50": round(_safe_float(row_dict.get("predicted_points_p50"), float("nan")), 2),
                "projected_points_p90": round(_safe_float(row_dict.get("predicted_points_p90"), float("nan")), 2),
                "projected_rebounds": round(_safe_float(row_dict.get("predicted_rebounds"), 0.0), 2),
                "projected_rebounds_p10": round(_safe_float(row_dict.get("predicted_rebounds_p10"), float("nan")), 2),
                "projected_rebounds_p50": round(_safe_float(row_dict.get("predicted_rebounds_p50"), float("nan")), 2),
                "projected_rebounds_p90": round(_safe_float(row_dict.get("predicted_rebounds_p90"), float("nan")), 2),
                "projected_assists": round(_safe_float(row_dict.get("predicted_assists"), 0.0), 2),
                "projected_assists_p10": round(_safe_float(row_dict.get("predicted_assists_p10"), float("nan")), 2),
                "projected_assists_p50": round(_safe_float(row_dict.get("predicted_assists_p50"), float("nan")), 2),
                "projected_assists_p90": round(_safe_float(row_dict.get("predicted_assists_p90"), float("nan")), 2),
                "projected_pra": round(_safe_float(row_dict.get("predicted_pra"), 0.0), 2),
                "projected_pra_p10": round(_safe_float(row_dict.get("predicted_pra_p10"), float("nan")), 2),
                "projected_pra_p50": round(_safe_float(row_dict.get("predicted_pra_p50"), float("nan")), 2),
                "projected_pra_p90": round(_safe_float(row_dict.get("predicted_pra_p90"), float("nan")), 2),
                "projected_draftkings_points": round(_safe_float(row_dict.get("predicted_draftkings_points"), 0.0), 2),
                "projected_draftkings_points_p10": round(_safe_float(row_dict.get("predicted_draftkings_points_p10"), float("nan")), 2),
                "projected_draftkings_points_p90": round(_safe_float(row_dict.get("predicted_draftkings_points_p90"), float("nan")), 2),
                "projected_fanduel_points": round(_safe_float(row_dict.get("predicted_fanduel_points"), 0.0), 2),
                "projected_fanduel_points_p10": round(_safe_float(row_dict.get("predicted_fanduel_points_p10"), float("nan")), 2),
                "projected_fanduel_points_p90": round(_safe_float(row_dict.get("predicted_fanduel_points_p90"), float("nan")), 2),
                "projected_steals": round(_safe_float(row_dict.get("predicted_steals"), 0.0), 2),
                "projected_blocks": round(_safe_float(row_dict.get("predicted_blocks"), 0.0), 2),
                "projected_turnovers": round(_safe_float(row_dict.get("predicted_turnovers"), 0.0), 2),
                "projected_three_points_made": round(_safe_float(row_dict.get("predicted_three_points_made"), 0.0), 2),
                "line_points": round(_safe_float(row_dict.get("line_points"), float("nan")), 2),
                "line_points_consensus": round(_safe_float(row_dict.get("line_points_consensus"), float("nan")), 2),
                "line_points_stddev": round(_safe_float(row_dict.get("line_points_stddev"), float("nan")), 3),
                "line_points_books_count": round(_safe_float(row_dict.get("line_points_books_count"), float("nan")), 1),
                "line_points_snapshot_age_minutes": round(_safe_float(row_dict.get("line_points_snapshot_age_minutes"), float("nan")), 1),
                "line_points_open": round(_safe_float(row_dict.get("line_points_open"), float("nan")), 2),
                "line_points_close": round(_safe_float(row_dict.get("line_points_close"), float("nan")), 2),
                "line_points_movement": round(_safe_float(row_dict.get("line_points_movement"), float("nan")), 3),
                "line_rebounds": round(_safe_float(row_dict.get("line_rebounds"), float("nan")), 2),
                "line_rebounds_consensus": round(_safe_float(row_dict.get("line_rebounds_consensus"), float("nan")), 2),
                "line_rebounds_stddev": round(_safe_float(row_dict.get("line_rebounds_stddev"), float("nan")), 3),
                "line_rebounds_books_count": round(_safe_float(row_dict.get("line_rebounds_books_count"), float("nan")), 1),
                "line_rebounds_snapshot_age_minutes": round(_safe_float(row_dict.get("line_rebounds_snapshot_age_minutes"), float("nan")), 1),
                "line_rebounds_open": round(_safe_float(row_dict.get("line_rebounds_open"), float("nan")), 2),
                "line_rebounds_close": round(_safe_float(row_dict.get("line_rebounds_close"), float("nan")), 2),
                "line_rebounds_movement": round(_safe_float(row_dict.get("line_rebounds_movement"), float("nan")), 3),
                "line_assists": round(_safe_float(row_dict.get("line_assists"), float("nan")), 2),
                "line_assists_consensus": round(_safe_float(row_dict.get("line_assists_consensus"), float("nan")), 2),
                "line_assists_stddev": round(_safe_float(row_dict.get("line_assists_stddev"), float("nan")), 3),
                "line_assists_books_count": round(_safe_float(row_dict.get("line_assists_books_count"), float("nan")), 1),
                "line_assists_snapshot_age_minutes": round(_safe_float(row_dict.get("line_assists_snapshot_age_minutes"), float("nan")), 1),
                "line_assists_open": round(_safe_float(row_dict.get("line_assists_open"), float("nan")), 2),
                "line_assists_close": round(_safe_float(row_dict.get("line_assists_close"), float("nan")), 2),
                "line_assists_movement": round(_safe_float(row_dict.get("line_assists_movement"), float("nan")), 3),
                "line_pra": round(_safe_float(row_dict.get("line_pra"), float("nan")), 2),
                "line_pra_consensus": round(_safe_float(row_dict.get("line_pra_consensus"), float("nan")), 2),
                "line_pra_stddev": round(_safe_float(row_dict.get("line_pra_stddev"), float("nan")), 3),
                "line_pra_books_count": round(_safe_float(row_dict.get("line_pra_books_count"), float("nan")), 1),
                "line_pra_snapshot_age_minutes": round(_safe_float(row_dict.get("line_pra_snapshot_age_minutes"), float("nan")), 1),
                "line_pra_open": round(_safe_float(row_dict.get("line_pra_open"), float("nan")), 2),
                "line_pra_close": round(_safe_float(row_dict.get("line_pra_close"), float("nan")), 2),
                "line_pra_movement": round(_safe_float(row_dict.get("line_pra_movement"), float("nan")), 3),
                "line_three_points_made": round(_safe_float(row_dict.get("line_three_points_made"), float("nan")), 2),
                "line_points_rebounds": round(_safe_float(row_dict.get("line_points_rebounds"), float("nan")), 2),
                "line_points_assists": round(_safe_float(row_dict.get("line_points_assists"), float("nan")), 2),
                "line_rebounds_assists": round(_safe_float(row_dict.get("line_rebounds_assists"), float("nan")), 2),
                "line_steals": round(_safe_float(row_dict.get("line_steals"), float("nan")), 2),
                "line_blocks": round(_safe_float(row_dict.get("line_blocks"), float("nan")), 2),
                "line_turnovers": round(_safe_float(row_dict.get("line_turnovers"), float("nan")), 2),
                "line_steals_blocks": round(_safe_float(row_dict.get("line_steals_blocks"), float("nan")), 2),
                "pregame_anchor_points": round(_safe_float(row_dict.get("pregame_anchor_points"), float("nan")), 2),
                "pregame_anchor_rebounds": round(_safe_float(row_dict.get("pregame_anchor_rebounds"), float("nan")), 2),
                "pregame_anchor_assists": round(_safe_float(row_dict.get("pregame_anchor_assists"), float("nan")), 2),
                "pregame_anchor_pra": round(_safe_float(row_dict.get("pregame_anchor_pra"), float("nan")), 2),
                "pregame_anchor_gap_points": round(_safe_float(row_dict.get("pregame_anchor_gap_points"), float("nan")), 2),
                "pregame_anchor_gap_rebounds": round(_safe_float(row_dict.get("pregame_anchor_gap_rebounds"), float("nan")), 2),
                "pregame_anchor_gap_assists": round(_safe_float(row_dict.get("pregame_anchor_gap_assists"), float("nan")), 2),
                "pregame_anchor_gap_pra": round(_safe_float(row_dict.get("pregame_anchor_gap_pra"), float("nan")), 2),
                "pregame_anchor_uncertainty_points": round(_safe_float(row_dict.get("pregame_anchor_uncertainty_points"), float("nan")), 3),
                "pregame_anchor_uncertainty_rebounds": round(_safe_float(row_dict.get("pregame_anchor_uncertainty_rebounds"), float("nan")), 3),
                "pregame_anchor_uncertainty_assists": round(_safe_float(row_dict.get("pregame_anchor_uncertainty_assists"), float("nan")), 3),
                "pregame_anchor_uncertainty_pra": round(_safe_float(row_dict.get("pregame_anchor_uncertainty_pra"), float("nan")), 3),
                "pregame_anchor_strength": round(_safe_float(row_dict.get("pregame_anchor_strength"), 0.0), 2),
                "line_data_freshness_score": round(_safe_float(row_dict.get("line_data_freshness_score"), 0.0), 3),
                "context_freshness_score": round(_safe_float(row_dict.get("context_freshness_score"), 0.0), 3),
                "scenario_cards": {
                    "lineup_conditional": lineup_scenarios,
                    "teammate_in_out": teammate_scenarios,
                },
                "confidence_decomposition": confidence_decomposition,
                "market_references": market_references,
            }
        )

    cards.sort(
        key=lambda item: (
            0 if item.get("is_actionable") else 1,
            0 if item.get("is_playable") else 1,
            0 if not item.get("no_bet") else 1,
            0 if item["is_starter"] else 1,
            0 if item["error_pct_estimate"] <= 10.0 else 1,
            item["error_pct_estimate"],
            -item["projected_draftkings_points"],
            item["player_name"].lower(),
        )
    )

    summary = {
        "total_players": len(cards),
        "playable_players": sum(1 for card in cards if card.get("is_playable")),
        "non_playable_players": sum(1 for card in cards if not card.get("is_playable")),
        "no_bet_players": sum(1 for card in cards if card.get("no_bet")),
        "actionable_players": sum(1 for card in cards if card.get("is_actionable")),
        "starters": sum(1 for card in cards if card["is_starter"]),
        "likely_non_starters": sum(1 for card in cards if card["start_label"].lower().startswith("not starting")),
        "high_confidence_under_10pct": sum(1 for card in cards if card["error_pct_estimate"] <= 10.0 and not card.get("no_bet")),
        "elite_confidence_under_5pct": sum(1 for card in cards if card["error_pct_estimate"] <= 5.0 and not card.get("no_bet")),
        "over_10pct_error": sum(1 for card in cards if card["error_pct_estimate"] > 10.0),
        "popular_high_confidence": sum(1 for card in cards if card["popular_high_confidence"]),
    }

    return {
        "board_date": selected_date,
        "available_dates": available_dates,
        "generated_at": datetime.now().isoformat(),
        "summary": summary,
        "cross_reference": {
            "status": cross_reference_status,
            "message": cross_reference_message,
            "lines_rows": lines_rows,
            "matched_players": matched_players,
        },
        "accuracy_hardening": hardening_profile,
        "cards": cards,
    }


def combined_app_status(include_previews: bool = True) -> dict:
    payload = _build_combined_status(include_previews=include_previews)
    live_state = payload["live_sync"].get("state", {})
    live_config = payload["live_sync"].get("config", {})
    projection_refresh_seconds = _safe_int(
        live_config.get("projection_refresh_interval_seconds"),
        10,
    )
    optimization_refresh_seconds = _safe_int(
        live_config.get("optimization_interval_seconds"),
        10,
    )
    payload["automation"] = {
        "projection_refresh_interval_minutes": _safe_int(live_config.get("projection_refresh_interval_minutes"), 1),
        "projection_refresh_interval_seconds": projection_refresh_seconds,
        "prediction_min_interval_seconds": _safe_int(live_config.get("prediction_min_interval_seconds"), 300),
        "prediction_on_context_change_only": bool(live_config.get("prediction_on_context_change_only", True)),
        "prediction_max_rows_per_cycle": _safe_int(live_config.get("prediction_max_rows_per_cycle"), 400),
        "force_provider_refresh_every_poll": bool(live_config.get("force_provider_refresh_every_poll", True)),
        "shot_style_context_refresh_interval_seconds": _safe_int(
            live_config.get("shot_style_context_refresh_interval_seconds"),
            300,
        ),
        "in_game_projection_refresh_interval_seconds": _safe_int(
            live_config.get("in_game_projection_refresh_interval_seconds"),
            10,
        ),
        "auto_refresh_in_game_projections": bool(live_config.get("auto_refresh_in_game_projections", True)),
        "optimization_interval_minutes": _safe_int(live_config.get("optimization_interval_minutes"), 1),
        "optimization_interval_seconds": optimization_refresh_seconds,
        "run_heavy_model_tasks_in_live_sync": bool(live_config.get("run_heavy_model_tasks_in_live_sync", False)),
        "auto_self_optimize_hourly": bool(live_config.get("auto_self_optimize_hourly", True)),
        "auto_retrain_each_interval": bool(live_config.get("auto_retrain_each_interval", False)),
        "retrain_interval_seconds": _safe_int(live_config.get("retrain_interval_seconds"), projection_refresh_seconds),
        "last_sync_duration_seconds": _safe_float(live_state.get("last_sync_duration_seconds"), 0.0),
        "last_projection_refresh_at": live_state.get("last_projection_refresh_at"),
        "next_projection_refresh_due_at": live_state.get("next_projection_refresh_due_at"),
        "last_in_game_projection_refresh_at": live_state.get("last_in_game_projection_refresh_at"),
        "next_in_game_projection_refresh_due_at": live_state.get("next_in_game_projection_refresh_due_at"),
        "last_retrain_refresh_at": live_state.get("last_retrain_refresh_at"),
        "next_retrain_due_at": live_state.get("next_retrain_due_at"),
        "in_game_projection_rows_updated": _safe_int(live_state.get("in_game_projection_rows_updated"), 0),
        "in_game_projection_players_tracked": _safe_int(live_state.get("in_game_projection_players_tracked"), 0),
        "in_game_projection_games_tracked": _safe_int(live_state.get("in_game_projection_games_tracked"), 0),
        "in_game_projection_live_games_active": _safe_int(live_state.get("in_game_projection_live_games_active"), 0),
        "in_game_projection_note": live_state.get("in_game_projection_note"),
        "in_game_projection_last_error": live_state.get("in_game_projection_last_error"),
        "prediction_rows_used": _safe_int(live_state.get("prediction_rows_used"), 0),
        "last_optimization_at": live_state.get("last_optimization_at"),
        "next_optimization_due_at": live_state.get("next_optimization_due_at"),
        "optimization_summary": live_state.get("optimization_summary", {}),
    }
    active_training_path = DEFAULT_TRAINING_UPLOAD_PATH if DEFAULT_TRAINING_UPLOAD_PATH.exists() else DEFAULT_DATA_PATH
    recheck_payload = _load_recheck_metrics()
    payload["recheck"] = recheck_payload
    model_coverage = payload.get("model_coverage")
    if isinstance(model_coverage, dict) and isinstance(recheck_payload, dict):
        model_coverage["recheck_evaluated_rows"] = _safe_int(recheck_payload.get("evaluated_rows"), 0)
        model_coverage["recheck_sample_rows"] = _safe_int(recheck_payload.get("sample_rows"), 0)
        model_coverage["recheck_lookback_days"] = _safe_int(recheck_payload.get("lookback_days"), 0)
        overall = recheck_payload.get("overall", {})
        if isinstance(overall, dict):
            model_coverage["recheck_overall_mean_abs_pct_error"] = _safe_float(overall.get("mean_abs_pct_error"), 0.0)
    payload["downloads"].update(
        {
            "training_data": active_training_path.name if active_training_path.exists() else None,
            "upcoming_slate": DEFAULT_UPCOMING_PATH.name if DEFAULT_UPCOMING_PATH.exists() else None,
            "context_updates": DEFAULT_CONTEXT_UPDATES_PATH.name if DEFAULT_CONTEXT_UPDATES_PATH.exists() else None,
            "provider_context_updates": DEFAULT_PROVIDER_CONTEXT_PATH.name if DEFAULT_PROVIDER_CONTEXT_PATH.exists() else None,
            "live_game_actions": DEFAULT_LIVE_GAME_ACTIONS_PATH.name if DEFAULT_LIVE_GAME_ACTIONS_PATH.exists() else None,
            "postgame_reviews": DEFAULT_POSTGAME_REVIEWS_PATH.name if DEFAULT_POSTGAME_REVIEWS_PATH.exists() else None,
            "game_notes_daily": DEFAULT_GAME_NOTES_DAILY_PATH.name if DEFAULT_GAME_NOTES_DAILY_PATH.exists() else None,
            "live_sync_config": DEFAULT_LIVE_CONFIG_PATH.name if DEFAULT_LIVE_CONFIG_PATH.exists() else None,
            "live_sync_state": DEFAULT_LIVE_STATE_PATH.name if DEFAULT_LIVE_STATE_PATH.exists() else None,
            "app_archive": APP_ARCHIVE_PATH.name if APP_ARCHIVE_PATH.exists() else None,
            "engine_recheck": DEFAULT_RECHECK_PATH.name if DEFAULT_RECHECK_PATH.exists() else None,
            "rotowire_benchmark_snapshot": DEFAULT_ROTOWIRE_BENCHMARK_SNAPSHOT_PATH.name
            if DEFAULT_ROTOWIRE_BENCHMARK_SNAPSHOT_PATH.exists()
            else None,
            "rotowire_benchmark_report": DEFAULT_ROTOWIRE_BENCHMARK_REPORT_PATH.name
            if DEFAULT_ROTOWIRE_BENCHMARK_REPORT_PATH.exists()
            else None,
            "rotowire_benchmark_joined": DEFAULT_ROTOWIRE_BENCHMARK_JOIN_PATH.name
            if DEFAULT_ROTOWIRE_BENCHMARK_JOIN_PATH.exists()
            else None,
            "prediction_miss_log": DEFAULT_PREDICTION_MISS_LOG_PATH.name
            if DEFAULT_PREDICTION_MISS_LOG_PATH.exists()
            else None,
            "adaptive_learning_profile": DEFAULT_ADAPTIVE_LEARNING_PROFILE_PATH.name
            if DEFAULT_ADAPTIVE_LEARNING_PROFILE_PATH.exists()
            else None,
        }
    )
    payload["background_jobs"] = _recent_background_jobs()
    return payload


def _download_link(path: Path | None) -> str | None:
    if path is None or not path.exists():
        return None
    return f"/downloads/{path.name}"


def live_source_catalog() -> dict:
    live = LIVE_SYNC_MANAGER.ensure_running()
    config = live.get("config", {})
    state = live.get("state", {})
    providers_config = config.get("providers", {})
    providers_state = state.get("providers", {})

    odds_config = providers_config.get("odds", {})
    odds_state = providers_state.get("odds", {})
    props_config = providers_config.get("player_props", {})
    props_state = providers_state.get("player_props", {})
    rotowire_config = providers_config.get("rotowire_prizepicks", {})
    rotowire_state = providers_state.get("rotowire_prizepicks", {})
    betr_config = providers_config.get("betr", {})
    betr_state = providers_state.get("betr", {})
    benchmark_report = load_rotowire_benchmark_report()
    lineups_config = providers_config.get("lineups", {})
    lineups_state = providers_state.get("lineups", {})
    playstyle_config = providers_config.get("playstyle", {})
    playstyle_state = providers_state.get("playstyle", {})
    profiles_config = providers_config.get("player_profiles", {})
    profiles_state = providers_state.get("player_profiles", {})
    news_config = providers_config.get("news", {})
    news_state = providers_state.get("news", {})
    injuries_config = providers_config.get("injuries", {})
    injuries_state = providers_state.get("injuries", {})
    game_notes_config = providers_config.get("game_notes", {})
    game_notes_state = providers_state.get("game_notes", {})
    cloud_archive_config = providers_config.get("cloud_archive", {})
    neon_config = providers_config.get("neon_sync", {})
    neon_state = providers_state.get("neon_sync", {})

    odds_base = str(odds_config.get("base_url", "") or "").rstrip("/")
    injuries_base = str(injuries_config.get("base_url", "") or "").rstrip("/")
    cloud_archive_path = str(
        state.get("cloud_archive_path")
        or cloud_archive_config.get("archive_path")
        or ""
    ).strip()
    latest_report_url = (
        injuries_state.get("latest_report_url")
        or state.get("official_injury_report")
        or injuries_config.get("official_report_page")
    )
    neon_host = (
        state.get("neon_sync_database_host")
        or neon_state.get("database_host")
        or urlparse(str(neon_config.get("database_url") or "")).hostname
    )

    sources = [
        {
            "name": "PrizePicks Board (Manual)",
            "description": "Manual prop board source for line entry. Public API is not available.",
            "url": "https://app.prizepicks.com/board",
            "page_url": "https://www.prizepicks.com/sport/nba",
            "enabled": False,
            "rows": None,
            "last_error": "Requires authenticated browser session; import lines manually in this app.",
            "note": "Use the PrizePicks Lines upload/paste section to ingest board lines from these pages.",
        },
        {
            "name": "PrizePicks Live Squares (Manual)",
            "description": "Manual Live Squares page reference. No public API endpoint for automated ingestion.",
            "url": "https://www.prizepicks.com/livesquares",
            "enabled": False,
            "rows": None,
            "last_error": "Requires authenticated browser session; import lines manually in this app.",
            "note": "Reference this page for Live Squares lines and paste/import them into PrizePicks Lines.",
        },
        {
            "name": "NBA Schedule Feed",
            "description": "Official upcoming game schedule JSON used for slate generation.",
            "url": SCHEDULE_URL,
            "enabled": bool(config.get("auto_build_upcoming_slate", True)),
            "rows": state.get("scheduled_games_found"),
            "last_error": state.get("last_error"),
        },
        {
            "name": "NBA Live Scoreboard Feed",
            "description": "Official same-day live game status and scoreboard feed.",
            "url": SCOREBOARD_URL,
            "enabled": bool(config.get("auto_build_upcoming_slate", True)),
            "rows": state.get("games_seen"),
            "last_error": state.get("last_error"),
        },
        {
            "name": "NBA Boxscore Feed Template",
            "description": "Official completed-game player boxscore feed template.",
            "url": BOXSCORE_URL_TEMPLATE,
            "enabled": bool(config.get("auto_retrain_on_new_results", True)),
            "rows": state.get("completed_rows_appended"),
            "last_error": state.get("last_error"),
        },
        {
            "name": "Live Game Notes Stream",
            "description": "10-second live game action notes derived from active boxscores.",
            "url": _download_link(DEFAULT_LIVE_GAME_ACTIONS_PATH),
            "enabled": bool(game_notes_config.get("enabled", False)),
            "rows": game_notes_state.get("live_rows") or state.get("game_notes_live_rows"),
            "last_error": game_notes_state.get("last_error"),
            "note": game_notes_state.get("note"),
        },
        {
            "name": "Postgame Review Notes",
            "description": "Per-player postgame review signals compiled from final games and recap text.",
            "url": _download_link(DEFAULT_POSTGAME_REVIEWS_PATH),
            "enabled": bool(game_notes_config.get("enabled", False)),
            "rows": game_notes_state.get("postgame_rows") or state.get("postgame_review_rows"),
            "last_error": game_notes_state.get("last_error"),
            "note": game_notes_state.get("note"),
        },
        {
            "name": "Compiled Game Notes Context",
            "description": "Daily compiled notes context merged into projection features.",
            "url": _download_link(DEFAULT_GAME_NOTES_DAILY_PATH),
            "enabled": bool(game_notes_config.get("enabled", False)),
            "rows": game_notes_state.get("compiled_rows") or state.get("game_notes_daily_rows"),
            "last_error": game_notes_state.get("last_error"),
            "note": game_notes_state.get("note"),
        },
        {
            "name": "NBA Stats Playstyle Profiles",
            "description": "Player shot/playstyle context from NBA Stats endpoints (dash player stats, touch-point stats, shot locations).",
            "url": str(playstyle_config.get("base_url") or "https://stats.nba.com/stats"),
            "docs_url": "https://stats.nba.com/",
            "enabled": bool(playstyle_config.get("enabled", False)),
            "rows": playstyle_state.get("rows"),
            "last_error": playstyle_state.get("last_error"),
            "note": playstyle_state.get("note")
            or (
                f"Merge cadence: {_safe_int(playstyle_config.get('refresh_interval_seconds'), 1)}s; "
                f"remote fetch cadence: {_safe_int(playstyle_config.get('remote_refresh_interval_seconds'), 1800)}s."
            ),
        },
        {
            "name": "Player Profile / Bio Context",
            "description": "Structured player profile context used for age/role/background stability features.",
            "url": str(profiles_config.get("wikipedia_summary_template") or "https://en.wikipedia.org/api/rest_v1/page/summary/{title}"),
            "enabled": bool(profiles_config.get("enabled", False)),
            "rows": profiles_state.get("rows"),
            "last_error": profiles_state.get("last_error"),
            "note": profiles_state.get("note"),
        },
        {
            "name": "News Context (Google/ESPN/RotoWire RSS)",
            "description": "Rolling news feed ingestion for lineup/injury/role volatility adjustments.",
            "url": str(news_config.get("google_news_template") or news_config.get("espn_rss_url") or "https://news.google.com/rss"),
            "docs_url": "https://news.google.com/",
            "enabled": bool(news_config.get("enabled", False)),
            "rows": news_state.get("rows"),
            "last_error": news_state.get("last_error"),
            "note": news_state.get("note"),
        },
        {
            "name": "The Odds API (v4)",
            "description": "Lines/markets context source for spreads, totals, and implied team totals.",
            "url": odds_base or "https://api.the-odds-api.com/v4",
            "docs_url": "https://the-odds-api.com/liveapi/guides/v4/",
            "enabled": bool(odds_config.get("enabled", False)),
            "rows": odds_state.get("rows"),
            "last_error": odds_state.get("last_error"),
            "note": odds_state.get("note"),
        },
        {
            "name": "The Odds API Player Props",
            "description": "Per-player market context from pregame player prop markets when credits are available.",
            "url": odds_base or "https://api.the-odds-api.com/v4",
            "docs_url": "https://the-odds-api.com/liveapi/guides/v4/",
            "enabled": bool(props_config.get("enabled", False)),
            "rows": props_state.get("rows"),
            "last_error": props_state.get("last_error"),
            "note": props_state.get("note"),
        },
        {
            "name": "RotoWire PrizePicks Lines",
            "description": "Public RotoWire Picks feed used to pull live PrizePicks-style prop lines and compare/adjust against other providers.",
            "url": str(rotowire_config.get("lines_url") or "https://www.rotowire.com/picks/api/lines.php"),
            "page_url": "https://www.rotowire.com/picks/prizepicks/",
            "enabled": bool(rotowire_config.get("enabled", False)),
            "rows": rotowire_state.get("rows"),
            "last_error": rotowire_state.get("last_error"),
            "note": rotowire_state.get("note"),
        },
        {
            "name": "BETR Board (Manual / Limited Public Access)",
            "description": "BETR board reference. This app tracks BETR as a manual source because a stable public prop-line API is not currently available.",
            "url": str(betr_config.get("board_url") or "https://www.betr.app/"),
            "page_url": "https://www.betr.app/",
            "enabled": bool(betr_config.get("enabled", False)),
            "rows": betr_state.get("rows"),
            "last_error": betr_state.get("last_error"),
            "note": betr_state.get("note") or betr_config.get("note"),
        },
        {
            "name": "RotoWire Benchmark",
            "description": "Formal benchmark of model vs market-line anchor over rolling completed games.",
            "url": _download_link(DEFAULT_ROTOWIRE_BENCHMARK_REPORT_PATH),
            "enabled": True,
            "rows": benchmark_report.get("rows_evaluated") if isinstance(benchmark_report, dict) else 0,
            "last_error": None,
            "note": (
                f"Last report generated at {benchmark_report.get('generated_at')}"
                if isinstance(benchmark_report, dict) and benchmark_report.get("generated_at")
                else "No benchmark report yet. Run the RotoWire benchmark action after snapshots and game results accumulate."
            ),
        },
        {
            "name": "Adaptive Miss-Learning Loop",
            "description": "Detects misses from completed outcomes, updates a miss log, and refreshes adaptive bias corrections used in projections.",
            "url": _download_link(DEFAULT_PREDICTION_MISS_LOG_PATH),
            "enabled": True,
            "rows": _safe_int(state.get("adaptive_learning_rows_total"), 0),
            "last_error": state.get("adaptive_learning_last_error"),
            "note": (
                f"14d miss rate: {round(_safe_float(state.get('adaptive_learning_miss_rate_14d'), 0.0), 2)}% | "
                f"last run: {state.get('adaptive_learning_last_run_at')}"
            ),
        },
        {
            "name": "NBA Daily Lineups Feed",
            "description": "Confirmed/expected lineup statuses and positions for starter context.",
            "url": "https://stats.nba.com/js/data/leaders/00_daily_lineups_YYYYMMDD.json",
            "enabled": bool(lineups_config.get("enabled", False)),
            "rows": lineups_state.get("rows"),
            "last_error": lineups_state.get("last_error"),
            "note": lineups_state.get("note"),
        },
        {
            "name": "BALDONTLIE Injuries API",
            "description": "Structured injury provider endpoint used when available.",
            "url": f"{injuries_base}/player_injuries" if injuries_base else "https://api.balldontlie.io/v1/player_injuries",
            "docs_url": "https://docs.balldontlie.io/",
            "enabled": bool(injuries_config.get("enabled", False)),
            "rows": injuries_state.get("rows"),
            "last_error": injuries_state.get("last_error"),
            "note": injuries_state.get("note"),
        },
        {
            "name": "NBA Official Injury Report",
            "description": "Official NBA injury report page and latest report PDF when discovered.",
            "url": latest_report_url,
            "page_url": injuries_config.get("official_report_page"),
            "enabled": bool(injuries_config.get("enabled", False)),
            "rows": injuries_state.get("records_loaded"),
            "last_error": injuries_state.get("official_report_error"),
        },
        {
            "name": "Cloud Archive Bundle",
            "description": "Cloud-backed historical bundle storing every pulled dataset snapshot for continuous learning.",
            "url": cloud_archive_path or None,
            "enabled": bool(cloud_archive_config.get("enabled", False)),
            "rows": state.get("cloud_archive_rows_synced"),
            "last_error": state.get("cloud_archive_last_error"),
            "note": state.get("cloud_archive_note"),
        },
        {
            "name": "Neon Postgres Sync",
            "description": "Optional Neon Postgres archive stream for remote dataset backups and recovery.",
            "url": (f"https://{neon_host}" if neon_host else "https://neon.tech/"),
            "docs_url": "https://neon.tech/docs/",
            "enabled": bool(neon_config.get("enabled", False)),
            "rows": state.get("neon_sync_rows_synced"),
            "last_error": state.get("neon_sync_last_error") or neon_state.get("last_error"),
            "note": (
                state.get("neon_sync_note")
                or neon_state.get("note")
                or "Set NEON_DATABASE_URL and enable providers.neon_sync to activate."
            ),
        },
    ]

    local_links = [
        {
            "name": "Current Predictions (CSV)",
            "description": "Latest projected player outputs for all available slates.",
            "url": _download_link(DEFAULT_PREDICTIONS_PATH),
        },
        {
            "name": "Upcoming Slate (CSV)",
            "description": "Current future player-game rows being scored by the model.",
            "url": _download_link(DEFAULT_UPCOMING_PATH),
        },
        {
            "name": "Provider Context Updates (CSV)",
            "description": "Latest merged odds/injury provider context aligned to players.",
            "url": _download_link(DEFAULT_PROVIDER_CONTEXT_PATH),
        },
        {
            "name": "Live Game Actions (CSV)",
            "description": "Rolling 10-second live game action notes history.",
            "url": _download_link(DEFAULT_LIVE_GAME_ACTIONS_PATH),
        },
        {
            "name": "Postgame Reviews (CSV)",
            "description": "Per-player postgame review notes generated from completed games.",
            "url": _download_link(DEFAULT_POSTGAME_REVIEWS_PATH),
        },
        {
            "name": "Game Notes Daily (CSV)",
            "description": "Compiled daily game-notes context used in live projections.",
            "url": _download_link(DEFAULT_GAME_NOTES_DAILY_PATH),
        },
        {
            "name": "Historical Training Data (CSV)",
            "description": "Canonical one-row-per-player-game training history file.",
            "url": _download_link(DEFAULT_TRAINING_UPLOAD_PATH if DEFAULT_TRAINING_UPLOAD_PATH.exists() else DEFAULT_DATA_PATH),
        },
        {
            "name": "Live Sync State (JSON)",
            "description": "Current live sync state, provider stats, and latest sync metadata.",
            "url": _download_link(DEFAULT_LIVE_STATE_PATH),
        },
        {
            "name": "Prediction Miss Log (CSV)",
            "description": "Historical log of projection misses and residuals from completed games.",
            "url": _download_link(DEFAULT_PREDICTION_MISS_LOG_PATH),
        },
        {
            "name": "Adaptive Learning Profile (JSON)",
            "description": "Current self-correction profile derived from miss history.",
            "url": _download_link(DEFAULT_ADAPTIVE_LEARNING_PROFILE_PATH),
        },
        {
            "name": "Engine Metrics (JSON)",
            "description": "Latest training metrics for each prediction target.",
            "url": _download_link(DEFAULT_METRICS_PATH),
        },
        {
            "name": "Engine Recheck (JSON)",
            "description": "Historical validation metrics and target-by-target percent-error summaries.",
            "url": _download_link(DEFAULT_RECHECK_PATH),
        },
        {
            "name": "RotoWire Benchmark Snapshots (CSV)",
            "description": "Timestamped projection-vs-line snapshots used for formal benchmark evaluation.",
            "url": _download_link(DEFAULT_ROTOWIRE_BENCHMARK_SNAPSHOT_PATH),
        },
        {
            "name": "RotoWire Benchmark Report (JSON)",
            "description": "2-4 week benchmark summary with hit rate, CLV, calibration, and per-market deltas.",
            "url": _download_link(DEFAULT_ROTOWIRE_BENCHMARK_REPORT_PATH),
        },
        {
            "name": "RotoWire Benchmark Joined Rows (CSV)",
            "description": "Row-level join of snapshot lines, model projections, and realized outcomes.",
            "url": _download_link(DEFAULT_ROTOWIRE_BENCHMARK_JOIN_PATH),
        },
    ]

    actions = [
        {"label": "Sync Live Feeds Now", "endpoint": "/api/live/sync-async", "method": "POST"},
        {"label": "Refresh In-Game Projections Now", "endpoint": "/api/live/in-game-sync", "method": "POST"},
        {"label": "Train Models Now", "endpoint": "/api/train-async", "method": "POST", "developer_only": True},
        {"label": "Run Predictions Now", "endpoint": "/api/predict", "method": "POST", "developer_only": True},
        {"label": "Capture Benchmark Snapshot", "endpoint": "/api/benchmark/capture", "method": "POST", "developer_only": True},
        {"label": "Run RotoWire Benchmark", "endpoint": "/api/benchmark/rotowire", "method": "POST", "developer_only": True},
        {"label": "Run Recheck Now", "endpoint": "/api/recheck", "method": "POST", "developer_only": True},
        {"label": "Run Daily Refresh Pipeline", "endpoint": "/api/daily/refresh-async", "method": "POST", "developer_only": True},
        {"label": "Refresh Dashboard", "endpoint": "/api/status", "method": "GET"},
    ]

    return {
        "running": bool(live.get("running")),
        "updated_at": state.get("last_sync_at"),
        "sources": sources,
        "local_links": [entry for entry in local_links if entry.get("url")],
        "actions": actions,
    }


def run_daily_refresh_pipeline() -> dict:
    live_config = LIVE_SYNC_MANAGER.ensure_running().get("config", {})
    lookback_days_raw = pd.to_numeric(live_config.get("model_training_lookback_days"), errors="coerce")
    lookback_days = int(lookback_days_raw) if pd.notna(lookback_days_raw) and lookback_days_raw > 0 else None

    steps: list[dict] = []
    sync_payload = LIVE_SYNC_MANAGER.sync_once()
    steps.append({"step": "live_sync", "result": sync_payload})

    train_payload = train_engine(lookback_days=lookback_days)
    steps.append({"step": "train", "result": train_payload})

    predict_payload = predict_engine()
    steps.append({"step": "predict", "result": predict_payload})

    try:
        benchmark_capture_payload = capture_rotowire_benchmark_snapshot()
        steps.append({"step": "benchmark_capture", "result": benchmark_capture_payload})
    except Exception as exc:  # noqa: BLE001
        steps.append({"step": "benchmark_capture", "error": str(exc)})

    sample_rows = None
    training_rows = 0
    if isinstance(train_payload, dict):
        training_rows = _safe_int(train_payload.get("training_rows"), 0)
        if training_rows > 5000:
            sample_rows = min(training_rows, 3000)

    recheck_payload = recheck_past_predictions(lookback_days=lookback_days, sample_rows=sample_rows if sample_rows and sample_rows > 0 else None)
    steps.append({"step": "recheck", "result": recheck_payload})

    try:
        benchmark_payload = run_rotowire_benchmark(lookback_days=max(14, lookback_days or 28))
        steps.append({"step": "rotowire_benchmark", "result": benchmark_payload})
    except Exception as exc:  # noqa: BLE001
        steps.append({"step": "rotowire_benchmark", "error": str(exc)})

    board_payload = build_player_board()
    return {
        "status": "ok",
        "ran_at": datetime.now().isoformat(),
        "training_lookback_days": lookback_days,
        "steps": steps,
        "board_summary": board_payload.get("summary", {}),
    }


def _register_account(email: str, password: str) -> dict:
    _ensure_commerce_schema()
    normalized_email = _normalize_email(email)
    if not normalized_email or not _validate_email(normalized_email):
        raise ValueError("A valid email address is required.")
    if len(str(password or "")) < 8:
        raise ValueError("Password must be at least 8 characters.")

    salt_b64, digest_b64 = _hash_password(password)
    created_at_ts = _now_ts()
    try:
        with _commerce_connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO users (email, password_hash, password_salt, password_iter, created_at_ts, last_login_at_ts)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (normalized_email, digest_b64, salt_b64, PASSWORD_PBKDF2_ITERATIONS, created_at_ts, created_at_ts),
            )
            user_id = int(cursor.lastrowid or 0)
            _ensure_subscription_row(conn, user_id)
            subscription_row = conn.execute("SELECT * FROM subscriptions WHERE user_id = ?", (user_id,)).fetchone()
            usage = _usage_summary_for_user(conn, user_id, _resolve_plan_from_subscription(subscription_row))
            user_row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
            conn.commit()
    except sqlite3.IntegrityError as exc:
        raise ValueError("An account with this email already exists.") from exc

    if user_row is None:
        raise ValueError("Unable to create account.")
    return _serialize_account_row(user_row, subscription_row, usage=usage)


def _create_session_for_user(user_id: int, ip_address: str | None = None, user_agent: str | None = None) -> str:
    _ensure_commerce_schema()
    token = secrets.token_urlsafe(40)
    token_digest = _token_hash(token)
    now_ts = _now_ts()
    expires_at_ts = now_ts + SESSION_TTL_SECONDS
    with _commerce_connection() as conn:
        conn.execute(
            """
            INSERT INTO sessions (user_id, token_hash, created_at_ts, expires_at_ts, last_seen_at_ts, ip_address, user_agent)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (int(user_id), token_digest, now_ts, expires_at_ts, now_ts, ip_address, user_agent),
        )
        conn.execute("UPDATE users SET last_login_at_ts = ? WHERE id = ?", (now_ts, int(user_id)))
        conn.commit()
    return token


def _clear_session_by_token(token: str) -> None:
    _ensure_commerce_schema()
    if not token:
        return
    with _commerce_connection() as conn:
        conn.execute("DELETE FROM sessions WHERE token_hash = ?", (_token_hash(token),))
        conn.commit()


def _account_from_session_token(token: str) -> dict | None:
    _ensure_commerce_schema()
    if not token:
        return None
    token_digest = _token_hash(token)
    now_ts = _now_ts()
    with _commerce_connection() as conn:
        session_row = conn.execute(
            """
            SELECT id, user_id, expires_at_ts
            FROM sessions
            WHERE token_hash = ?
            LIMIT 1
            """,
            (token_digest,),
        ).fetchone()
        if session_row is None:
            return None
        if _safe_int(session_row["expires_at_ts"], 0) <= now_ts:
            conn.execute("DELETE FROM sessions WHERE id = ?", (_safe_int(session_row["id"]),))
            conn.commit()
            return None
        user_id = _safe_int(session_row["user_id"], 0)
        user_row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
        if user_row is None:
            return None
        _ensure_subscription_row(conn, user_id)
        subscription_row = conn.execute("SELECT * FROM subscriptions WHERE user_id = ?", (user_id,)).fetchone()
        plan_code = _resolve_plan_from_subscription(subscription_row)
        usage = _usage_summary_for_user(conn, user_id, plan_code)
        conn.execute("UPDATE sessions SET last_seen_at_ts = ? WHERE id = ?", (now_ts, _safe_int(session_row["id"], 0)))
        conn.commit()
        return _serialize_account_row(user_row, subscription_row, usage=usage)


def _login_account(email: str, password: str) -> dict:
    _ensure_commerce_schema()
    normalized_email = _normalize_email(email)
    with _commerce_connection() as conn:
        user_row = conn.execute("SELECT * FROM users WHERE email = ?", (normalized_email,)).fetchone()
        if user_row is None:
            raise PermissionError("Invalid email or password.")
        verified = _verify_password(
            password=str(password or ""),
            salt_b64=str(user_row["password_salt"] or ""),
            digest_b64=str(user_row["password_hash"] or ""),
            iterations=_safe_int(user_row["password_iter"], PASSWORD_PBKDF2_ITERATIONS),
        )
        if not verified:
            raise PermissionError("Invalid email or password.")
    return {"user_id": _safe_int(user_row["id"], 0)}


def _stripe_plan_price_id(interval: str) -> str:
    normalized_interval = str(interval or "monthly").strip().lower()
    if normalized_interval == "yearly":
        if not STRIPE_PRICE_ID_PRO_YEARLY:
            raise ValueError("STRIPE_PRICE_ID_PRO_YEARLY is not configured.")
        return STRIPE_PRICE_ID_PRO_YEARLY
    if not STRIPE_PRICE_ID_PRO_MONTHLY:
        raise ValueError("STRIPE_PRICE_ID_PRO_MONTHLY is not configured.")
    return STRIPE_PRICE_ID_PRO_MONTHLY


def _create_stripe_checkout_session(account: dict, interval: str = "monthly") -> dict:
    if account.get("subscription", {}).get("plan_code") == PRO_PLAN_CODE:
        raise ValueError("Account already has an active Pro plan.")
    user_id = _safe_int(account.get("id"), 0)
    email = str(account.get("email") or "")
    price_id = _stripe_plan_price_id(interval)
    session = _stripe_request(
        "/checkout/sessions",
        {
            "mode": "subscription",
            "success_url": f"{APP_PUBLIC_URL}/?billing=success",
            "cancel_url": f"{APP_PUBLIC_URL}/?billing=cancel",
            "line_items[0][price]": price_id,
            "line_items[0][quantity]": 1,
            "client_reference_id": user_id,
            "customer_email": email,
            "metadata[user_id]": user_id,
            "metadata[plan_code]": PRO_PLAN_CODE,
        },
    )
    return {
        "checkout_session_id": session.get("id"),
        "checkout_url": session.get("url"),
        "mode": session.get("mode"),
    }


def _create_stripe_portal_session(account: dict) -> dict:
    customer_id = str(account.get("subscription", {}).get("stripe_customer_id") or "").strip()
    if not customer_id:
        raise ValueError("No Stripe customer is linked to this account yet.")
    session = _stripe_request(
        "/billing_portal/sessions",
        {
            "customer": customer_id,
            "return_url": f"{APP_PUBLIC_URL}/?billing=portal",
        },
    )
    return {
        "portal_url": session.get("url"),
        "portal_session_id": session.get("id"),
    }


def _find_user_id_by_stripe_customer(conn: sqlite3.Connection, customer_id: str | None) -> int | None:
    if not customer_id:
        return None
    row = conn.execute(
        "SELECT user_id FROM subscriptions WHERE stripe_customer_id = ? LIMIT 1",
        (str(customer_id),),
    ).fetchone()
    if row is None:
        return None
    return _safe_int(row["user_id"], 0) or None


def _apply_stripe_subscription_object(conn: sqlite3.Connection, subscription_obj: dict, fallback_user_id: int | None = None) -> int | None:
    if not isinstance(subscription_obj, dict):
        return fallback_user_id
    customer_id = str(subscription_obj.get("customer") or "")
    subscription_id = str(subscription_obj.get("id") or "")
    status = str(subscription_obj.get("status") or "inactive")
    current_period_end_ts = _safe_int(subscription_obj.get("current_period_end"), 0)
    cancel_at_period_end = bool(subscription_obj.get("cancel_at_period_end", False))
    items = subscription_obj.get("items", {}) if isinstance(subscription_obj.get("items"), dict) else {}
    item_data = items.get("data", []) if isinstance(items, dict) else []
    price_id = ""
    if isinstance(item_data, list) and item_data:
        first_item = item_data[0] if isinstance(item_data[0], dict) else {}
        price_obj = first_item.get("price", {}) if isinstance(first_item, dict) else {}
        if isinstance(price_obj, dict):
            price_id = str(price_obj.get("id") or "")
    plan_code = PRO_PLAN_CODE if status in {"active", "trialing", "past_due"} else FREE_PLAN_CODE
    user_id = _find_user_id_by_stripe_customer(conn, customer_id) or fallback_user_id
    if user_id is None:
        return None
    _upsert_subscription(
        conn,
        user_id=user_id,
        plan_code=plan_code,
        status=status,
        stripe_customer_id=customer_id or None,
        stripe_subscription_id=subscription_id or None,
        stripe_price_id=price_id or None,
        current_period_end_ts=current_period_end_ts if current_period_end_ts > 0 else None,
        cancel_at_period_end=cancel_at_period_end,
    )
    return user_id


def _handle_stripe_webhook_event(event_payload: dict) -> dict:
    _ensure_commerce_schema()
    event_id = str(event_payload.get("id") or "")
    event_type = str(event_payload.get("type") or "")
    data_object = event_payload.get("data", {}).get("object", {}) if isinstance(event_payload.get("data"), dict) else {}
    if not event_id or not event_type:
        raise ValueError("Invalid Stripe event payload.")

    with _commerce_connection() as conn:
        existing = conn.execute("SELECT id FROM stripe_events WHERE event_id = ?", (event_id,)).fetchone()
        if existing is not None:
            return {"status": "duplicate", "event_id": event_id, "event_type": event_type}

        conn.execute(
            """
            INSERT INTO stripe_events (event_id, event_type, received_at_ts, payload_json)
            VALUES (?, ?, ?, ?)
            """,
            (event_id, event_type, _now_ts(), json.dumps(_json_safe(event_payload), ensure_ascii=False)),
        )

        touched_user_id: int | None = None
        if event_type == "checkout.session.completed" and isinstance(data_object, dict):
            user_id = _safe_int(data_object.get("client_reference_id"), 0)
            metadata = data_object.get("metadata", {}) if isinstance(data_object.get("metadata"), dict) else {}
            if not user_id:
                user_id = _safe_int(metadata.get("user_id"), 0)
            customer_id = str(data_object.get("customer") or "")
            subscription_id = str(data_object.get("subscription") or "")
            if user_id:
                _upsert_subscription(
                    conn,
                    user_id=user_id,
                    plan_code=PRO_PLAN_CODE,
                    status="active",
                    stripe_customer_id=customer_id or None,
                    stripe_subscription_id=subscription_id or None,
                    current_period_end_ts=None,
                    cancel_at_period_end=False,
                )
                touched_user_id = user_id
        elif event_type in {
            "customer.subscription.created",
            "customer.subscription.updated",
            "customer.subscription.deleted",
        }:
            touched_user_id = _apply_stripe_subscription_object(conn, data_object)
        elif event_type == "invoice.paid" and isinstance(data_object, dict):
            subscription_obj = data_object.get("subscription_details", {}) if isinstance(data_object.get("subscription_details"), dict) else {}
            if isinstance(subscription_obj, dict) and subscription_obj.get("subscription"):
                subscription_id = str(subscription_obj.get("subscription"))
                sub_row = conn.execute(
                    "SELECT user_id, stripe_customer_id FROM subscriptions WHERE stripe_subscription_id = ? LIMIT 1",
                    (subscription_id,),
                ).fetchone()
                if sub_row is not None:
                    touched_user_id = _safe_int(sub_row["user_id"], 0)
        conn.commit()

    return {
        "status": "processed",
        "event_id": event_id,
        "event_type": event_type,
        "user_id": touched_user_id,
    }


class PredictionAppHandler(BaseHTTPRequestHandler):
    def do_OPTIONS(self) -> None:
        self.send_response(HTTPStatus.NO_CONTENT)
        self._send_cors_headers()
        self.end_headers()

    def _read_cookie_value(self, key: str) -> str:
        raw_cookie = str(self.headers.get("Cookie", "") or "")
        if not raw_cookie:
            return ""
        for chunk in raw_cookie.split(";"):
            if "=" not in chunk:
                continue
            name, value = chunk.split("=", 1)
            if name.strip() == key:
                return value.strip()
        return ""

    def _session_cookie_header(self, token: str) -> str:
        secure_flag = "; Secure" if str(self.headers.get("X-Forwarded-Proto", "")).lower() == "https" else ""
        max_age = max(1, int(SESSION_TTL_SECONDS))
        return (
            f"{SESSION_COOKIE_NAME}={token}; Path=/; HttpOnly; SameSite=Lax; Max-Age={max_age}{secure_flag}"
        )

    def _expired_session_cookie_header(self) -> str:
        return f"{SESSION_COOKIE_NAME}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0"

    def _current_account(self) -> dict | None:
        cached = getattr(self, "_cached_account_payload", None)
        if cached is not None:
            return cached
        token = self._read_cookie_value(SESSION_COOKIE_NAME)
        account = _account_from_session_token(token)
        self._cached_account_payload = account
        return account

    def _require_authenticated_account(self) -> dict:
        account = self._current_account()
        if account is None:
            raise PermissionError("Authentication required. Please log in.")
        return account

    def _require_pro_if_enforced(self, account: dict) -> None:
        if not PAYWALL_ENFORCEMENT:
            return
        plan_code = str(account.get("subscription", {}).get("plan_code", FREE_PLAN_CODE))
        if plan_code != PRO_PLAN_CODE:
            raise PermissionError("Pro subscription required for this action.")

    def _meter_usage(self, account: dict | None, event_key: str, quantity: int = 1, metadata: dict | None = None) -> None:
        if account is None:
            return
        user_id = _safe_int(account.get("id"), 0)
        if user_id <= 0:
            return
        _ensure_commerce_schema()
        plan_code = str(account.get("subscription", {}).get("plan_code", FREE_PLAN_CODE)).lower()
        with _commerce_connection() as conn:
            _enforce_usage_limit(conn, user_id=user_id, plan_code=plan_code, event_key=event_key, quantity=quantity)
            _record_usage_event(conn, user_id=user_id, event_key=event_key, quantity=quantity, metadata=metadata)
            conn.commit()
        # refresh cached account usage snapshot on next request
        self._cached_account_payload = None

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        if path in {"/", "/index.html"}:
            self._serve_file(UI_DIR / "index.html", "text/html; charset=utf-8")
            return
        if path.startswith("/ui/"):
            relative = path.removeprefix("/ui/").lstrip("/")
            target = (UI_DIR / relative).resolve()
            try:
                target.relative_to(UI_DIR.resolve())
            except Exception:
                self.send_error(HTTPStatus.NOT_FOUND, "Not found.")
                return
            if not target.exists() or not target.is_file():
                self.send_error(HTTPStatus.NOT_FOUND, "Not found.")
                return
            mime_type = mimetypes.guess_type(target.name)[0] or "application/octet-stream"
            if mime_type.startswith("text/") or mime_type in {"application/javascript", "application/json"}:
                mime_type = f"{mime_type}; charset=utf-8"
            self._serve_file(target, mime_type)
            return
        if path.startswith("/snapshot/") or path.startswith("/ui/snapshot/"):
            relative = (
                path.removeprefix("/snapshot/").lstrip("/")
                if path.startswith("/snapshot/")
                else path.removeprefix("/ui/snapshot/").lstrip("/")
            )
            target = (UI_DIR / "snapshot" / relative).resolve()
            snapshot_root = (UI_DIR / "snapshot").resolve()
            try:
                target.relative_to(snapshot_root)
            except Exception:
                self.send_error(HTTPStatus.NOT_FOUND, "Not found.")
                return
            if not target.exists() or not target.is_file():
                self.send_error(HTTPStatus.NOT_FOUND, "Not found.")
                return
            mime_type = mimetypes.guess_type(target.name)[0] or "application/octet-stream"
            if mime_type.startswith("text/") or mime_type in {"application/javascript", "application/json"}:
                mime_type = f"{mime_type}; charset=utf-8"
            self._serve_file(target, mime_type)
            return
        if path == "/styles.css":
            self._serve_file(UI_DIR / "styles.css", "text/css; charset=utf-8")
            return
        if path == "/app.js":
            self._serve_file(UI_DIR / "app.js", "application/javascript; charset=utf-8")
            return
        if path == "/api/schema":
            schema_payload = dict(SCHEMA_GUIDE)
            schema_payload["data_contracts"] = describe_data_contracts()
            self._send_json(schema_payload)
            return
        if path == "/api/data/contracts":
            self._send_json(describe_data_contracts())
            return
        if path == "/api/data/pipeline":
            include_drift = str(query.get("include_drift", ["0"])[0] or "").strip().lower() in {"1", "true", "yes"}
            self._send_json(pipeline_status(limit_events=100, include_drift=include_drift))
            return
        if path == "/api/data/drift-audit":
            self._send_json(run_contract_drift_audit())
            return
        if path == "/api/status":
            lite = str(query.get("lite", ["1"])[0] or "").strip().lower() in {"1", "true", "yes"}
            refresh = str(query.get("refresh", ["0"])[0] or "").strip().lower() in {"1", "true", "yes"}
            payload = _combined_app_status_cached(include_previews=not lite, force_refresh=refresh)
            payload["account"] = self._current_account()
            self._send_json(payload)
            return
        if path == "/api/rubric/v05":
            refresh = str(query.get("refresh", ["0"])[0] or "").strip().lower() in {"1", "true", "yes"}
            payload = _combined_app_status_cached(include_previews=False, force_refresh=refresh)
            self._send_json(
                {
                    "rubric": payload.get("v05_rubric"),
                    "action_plan": payload.get("v05_action_plan"),
                    "runtime_reliability": payload.get("runtime_reliability"),
                }
            )
            return
        if path == "/api/account/status":
            account = self._current_account()
            monetization = {
                "paywall_enforcement": PAYWALL_ENFORCEMENT,
                "plans": {code: {"name": details.get("name"), "limits": details.get("limits", {})} for code, details in PLAN_FEATURES.items()},
                "stripe_enabled": bool(STRIPE_SECRET_KEY and (STRIPE_PRICE_ID_PRO_MONTHLY or STRIPE_PRICE_ID_PRO_YEARLY)),
                "public_url": APP_PUBLIC_URL,
            }
            self._send_json(
                {
                    "authenticated": bool(account),
                    "account": account,
                    "monetization": monetization,
                }
            )
            return
        if path == "/api/live/status":
            self._send_json(LIVE_SYNC_MANAGER.ensure_running())
            return
        if path == "/api/live/sources":
            self._send_json(live_source_catalog())
            return
        if path == "/api/jobs":
            self._send_json({"jobs": _recent_background_jobs()})
            return
        if path == "/api/jobs/status":
            job_id = str(query.get("job_id", [""])[0] or "").strip()
            if not job_id:
                self._send_json({"error": "job_id query parameter is required."}, status=HTTPStatus.BAD_REQUEST)
                return
            job = get_background_job(job_id)
            if not job:
                self._send_json({"error": f"Job '{job_id}' was not found."}, status=HTTPStatus.NOT_FOUND)
                return
            self._send_json({"job": job})
            return
        if path == "/api/player-board":
            board_payload = build_player_board(query.get("date", [None])[0])
            account = self._current_account()
            try:
                self._meter_usage(
                    account,
                    "live_sync_actions",
                    quantity=1,
                    metadata={"endpoint": "/api/player-board", "board_date": board_payload.get("board_date")},
                )
            except PermissionError as exc:
                board_payload.setdefault("warnings", []).append(str(exc))
            self._send_json(board_payload)
            return
        if path == "/api/assistant/status":
            board_date = query.get("date", [None])[0]
            assistant = _assistant_config()
            context = _assistant_context_snapshot(board_date, max_cards=5)
            self._send_json(
                {
                    "assistant": assistant,
                    "context_summary": context.get("summary", {}),
                    "board_date": context.get("board_date"),
                    "available_dates": context.get("available_dates"),
                }
            )
            return
        if path == "/api/prizepicks/status":
            status = _combined_app_status_cached(include_previews=False, force_refresh=False)
            self._send_json(
                {
                    "prizepicks_lines_dataset": status.get("prizepicks_lines_dataset"),
                    "prizepicks_edges": status.get("prizepicks_edges"),
                }
            )
            return
        if path == "/api/benchmark/rotowire":
            report = load_rotowire_benchmark_report()
            if not report:
                self._send_json(
                    {
                        "report": None,
                        "note": (
                            "No benchmark report exists yet. "
                            "Run POST /api/benchmark/rotowire after snapshot capture and completed games."
                        ),
                    }
                )
                return
            self._send_json({"report": report})
            return
        if path.startswith("/downloads/"):
            filename = Path(path.removeprefix("/downloads/")).name
            candidates = {
                DEFAULT_BUNDLE_PATH.name: DEFAULT_BUNDLE_PATH,
                DEFAULT_METRICS_PATH.name: DEFAULT_METRICS_PATH,
                DEFAULT_PREDICTIONS_PATH.name: DEFAULT_PREDICTIONS_PATH,
                DEFAULT_SEASON_PRIORS_PATH.name: DEFAULT_SEASON_PRIORS_PATH,
                DEFAULT_PRIZEPICKS_LINES_PATH.name: DEFAULT_PRIZEPICKS_LINES_PATH,
                DEFAULT_PRIZEPICKS_EDGES_PATH.name: DEFAULT_PRIZEPICKS_EDGES_PATH,
                DEFAULT_TRAINING_UPLOAD_PATH.name: DEFAULT_TRAINING_UPLOAD_PATH,
                DEFAULT_DATA_PATH.name: DEFAULT_DATA_PATH,
                DEFAULT_UPCOMING_PATH.name: DEFAULT_UPCOMING_PATH,
                DEFAULT_CONTEXT_UPDATES_PATH.name: DEFAULT_CONTEXT_UPDATES_PATH,
                DEFAULT_PROVIDER_CONTEXT_PATH.name: DEFAULT_PROVIDER_CONTEXT_PATH,
                DEFAULT_LIVE_GAME_ACTIONS_PATH.name: DEFAULT_LIVE_GAME_ACTIONS_PATH,
                DEFAULT_POSTGAME_REVIEWS_PATH.name: DEFAULT_POSTGAME_REVIEWS_PATH,
                DEFAULT_GAME_NOTES_DAILY_PATH.name: DEFAULT_GAME_NOTES_DAILY_PATH,
                DEFAULT_RECHECK_PATH.name: DEFAULT_RECHECK_PATH,
                DEFAULT_ROTOWIRE_BENCHMARK_SNAPSHOT_PATH.name: DEFAULT_ROTOWIRE_BENCHMARK_SNAPSHOT_PATH,
                DEFAULT_ROTOWIRE_BENCHMARK_REPORT_PATH.name: DEFAULT_ROTOWIRE_BENCHMARK_REPORT_PATH,
                DEFAULT_ROTOWIRE_BENCHMARK_JOIN_PATH.name: DEFAULT_ROTOWIRE_BENCHMARK_JOIN_PATH,
                DEFAULT_LIVE_CONFIG_PATH.name: DEFAULT_LIVE_CONFIG_PATH,
                DEFAULT_LIVE_STATE_PATH.name: DEFAULT_LIVE_STATE_PATH,
                APP_ARCHIVE_PATH.name: APP_ARCHIVE_PATH,
            }
            if filename not in candidates or not candidates[filename].exists():
                self.send_error(HTTPStatus.NOT_FOUND, "Download not found.")
                return
            mime_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
            self._serve_file(candidates[filename], mime_type)
            return
        self.send_error(HTTPStatus.NOT_FOUND, "Not found.")

    def do_POST(self) -> None:
        if self.path == "/api/account/register":
            try:
                payload = self._read_json_payload()
                account = _register_account(payload.get("email", ""), payload.get("password", ""))
                token = _create_session_for_user(
                    user_id=_safe_int(account.get("id"), 0),
                    ip_address=str(self.client_address[0]) if self.client_address else None,
                    user_agent=str(self.headers.get("User-Agent", "") or ""),
                )
                self._cached_account_payload = None
                self._send_json(
                    {
                        "status": "ok",
                        "authenticated": True,
                        "account": _account_from_session_token(token),
                    },
                    extra_headers={"Set-Cookie": self._session_cookie_header(token)},
                )
            except Exception as exc:  # pragma: no cover
                self._send_error(exc)
            return
        if self.path == "/api/account/login":
            try:
                payload = self._read_json_payload()
                login_result = _login_account(payload.get("email", ""), payload.get("password", ""))
                token = _create_session_for_user(
                    user_id=_safe_int(login_result.get("user_id"), 0),
                    ip_address=str(self.client_address[0]) if self.client_address else None,
                    user_agent=str(self.headers.get("User-Agent", "") or ""),
                )
                self._cached_account_payload = None
                self._send_json(
                    {
                        "status": "ok",
                        "authenticated": True,
                        "account": _account_from_session_token(token),
                    },
                    extra_headers={"Set-Cookie": self._session_cookie_header(token)},
                )
            except Exception as exc:  # pragma: no cover
                self._send_error(exc)
            return
        if self.path == "/api/account/logout":
            try:
                token = self._read_cookie_value(SESSION_COOKIE_NAME)
                _clear_session_by_token(token)
                self._cached_account_payload = None
                self._send_json(
                    {"status": "ok", "authenticated": False},
                    extra_headers={"Set-Cookie": self._expired_session_cookie_header()},
                )
            except Exception as exc:  # pragma: no cover
                self._send_error(exc)
            return
        if self.path == "/api/account/checkout-session":
            try:
                account = self._require_authenticated_account()
                interval = str(self._read_json_payload().get("interval", "monthly") or "monthly")
                checkout = _create_stripe_checkout_session(account, interval=interval)
                self._send_json({"status": "ok", **checkout})
            except Exception as exc:  # pragma: no cover
                self._send_error(exc)
            return
        if self.path == "/api/account/portal-session":
            try:
                account = self._require_authenticated_account()
                portal = _create_stripe_portal_session(account)
                self._send_json({"status": "ok", **portal})
            except Exception as exc:  # pragma: no cover
                self._send_error(exc)
            return
        if self.path == "/api/stripe/webhook":
            try:
                body = self._read_request_body()
                signature = str(self.headers.get("Stripe-Signature", "") or "")
                if not _verify_stripe_webhook_signature(body, signature):
                    self._send_json({"error": "Invalid Stripe webhook signature."}, status=HTTPStatus.BAD_REQUEST)
                    return
                event_payload = json.loads(body.decode("utf-8") or "{}")
                result = _handle_stripe_webhook_event(event_payload)
                self._send_json(result)
            except Exception as exc:  # pragma: no cover
                self._send_error(exc)
            return
        if self.path == "/api/upload":
            self._handle_upload()
            return
        if self.path == "/api/import/historical":
            self._handle_historical_import()
            return
        if self.path == "/api/import/season-priors":
            self._handle_season_priors_import()
            return
        if self.path == "/api/prizepicks/lines":
            self._handle_prizepicks_lines_import()
            return
        if self.path == "/api/prizepicks/edges":
            self._handle_prizepicks_edges()
            return
        if self.path == "/api/assistant/chat":
            try:
                payload = self._read_json_payload()
                account = self._current_account()
                if PAYWALL_ENFORCEMENT and account is None:
                    self._send_json({"error": "Authentication required. Please log in."}, status=HTTPStatus.UNAUTHORIZED)
                    return
                try:
                    self._meter_usage(
                        account,
                        "assistant_messages",
                        quantity=1,
                        metadata={"endpoint": "/api/assistant/chat", "board_date": payload.get("board_date")},
                    )
                except PermissionError as exc:
                    self._send_json({"error": str(exc)}, status=HTTPStatus.PAYMENT_REQUIRED)
                    return
                self._send_json(
                    _assistant_chat_reply(
                        message=payload.get("message", ""),
                        board_date=payload.get("board_date"),
                        conversation=payload.get("conversation") if isinstance(payload.get("conversation"), list) else [],
                        agent_mode=bool(payload.get("agent_mode", False)),
                    )
                )
            except Exception as exc:  # pragma: no cover
                self._send_error(exc)
            return
        if self.path == "/api/assistant/config":
            try:
                payload = self._read_json_payload()
                result = _assistant_update_config(
                    openai_api_key=payload.get("openai_api_key"),
                    openai_model=payload.get("openai_model"),
                    clear_api_key=bool(payload.get("clear_api_key", False)),
                    test_connection=bool(payload.get("test_connection", False)),
                )
                self._send_json(result)
            except Exception as exc:  # pragma: no cover
                self._send_error(exc)
            return
        if self.path == "/api/live/start":
            self._send_json(LIVE_SYNC_MANAGER.start())
            return
        if self.path == "/api/live/stop":
            self._send_json(LIVE_SYNC_MANAGER.stop())
            return
        if self.path == "/api/live/sync":
            try:
                account = self._current_account()
                if PAYWALL_ENFORCEMENT and account is None:
                    self._send_json({"error": "Authentication required. Please log in."}, status=HTTPStatus.UNAUTHORIZED)
                    return
                try:
                    self._meter_usage(account, "live_sync_actions", quantity=1, metadata={"endpoint": "/api/live/sync"})
                except PermissionError as exc:
                    self._send_json({"error": str(exc)}, status=HTTPStatus.PAYMENT_REQUIRED)
                    return
                self._send_json(LIVE_SYNC_MANAGER.sync_once())
            except Exception as exc:  # pragma: no cover
                self._send_error(exc)
            return
        if self.path == "/api/live/sync-async":
            account = self._current_account()
            if PAYWALL_ENFORCEMENT and account is None:
                self._send_json({"error": "Authentication required. Please log in."}, status=HTTPStatus.UNAUTHORIZED)
                return
            try:
                self._meter_usage(account, "live_sync_actions", quantity=1, metadata={"endpoint": "/api/live/sync-async"})
            except PermissionError as exc:
                self._send_json({"error": str(exc)}, status=HTTPStatus.PAYMENT_REQUIRED)
                return
            self._send_json(start_background_job("live_sync", LIVE_SYNC_MANAGER.sync_once))
            return
        if self.path == "/api/live/in-game-sync":
            account = self._current_account()
            if PAYWALL_ENFORCEMENT and account is None:
                self._send_json({"error": "Authentication required. Please log in."}, status=HTTPStatus.UNAUTHORIZED)
                return
            try:
                self._meter_usage(account, "live_sync_actions", quantity=1, metadata={"endpoint": "/api/live/in-game-sync"})
            except PermissionError as exc:
                self._send_json({"error": str(exc)}, status=HTTPStatus.PAYMENT_REQUIRED)
                return
            self._send_json(start_background_job("in_game_refresh", LIVE_SYNC_MANAGER.in_game_refresh_once))
            return
        if self.path == "/api/daily/refresh":
            try:
                account = self._require_authenticated_account() if PAYWALL_ENFORCEMENT else self._current_account()
                if account:
                    self._require_pro_if_enforced(account)
                try:
                    self._meter_usage(account, "daily_refresh_runs", quantity=1, metadata={"endpoint": "/api/daily/refresh"})
                except PermissionError as exc:
                    self._send_json({"error": str(exc)}, status=HTTPStatus.PAYMENT_REQUIRED)
                    return
                self._send_json(run_daily_refresh_pipeline())
            except Exception as exc:  # pragma: no cover
                self._send_error(exc)
            return
        if self.path == "/api/daily/refresh-async":
            try:
                account = self._require_authenticated_account() if PAYWALL_ENFORCEMENT else self._current_account()
                if account:
                    self._require_pro_if_enforced(account)
                try:
                    self._meter_usage(account, "daily_refresh_runs", quantity=1, metadata={"endpoint": "/api/daily/refresh-async"})
                except PermissionError as exc:
                    self._send_json({"error": str(exc)}, status=HTTPStatus.PAYMENT_REQUIRED)
                    return
            except Exception as exc:  # pragma: no cover
                self._send_error(exc)
                return
            self._send_json(start_background_job("daily_refresh", run_daily_refresh_pipeline))
            return
        if self.path == "/api/train":
            try:
                account = self._require_authenticated_account() if PAYWALL_ENFORCEMENT else self._current_account()
                if account:
                    self._require_pro_if_enforced(account)
                live_config = LIVE_SYNC_MANAGER.ensure_running().get("config", {})
                lookback_days_raw = pd.to_numeric(live_config.get("model_training_lookback_days"), errors="coerce")
                lookback_days = int(lookback_days_raw) if pd.notna(lookback_days_raw) and lookback_days_raw > 0 else None
                payload = train_engine(lookback_days=lookback_days)
                self._send_json(payload)
            except Exception as exc:  # pragma: no cover
                self._send_error(exc)
            return
        if self.path == "/api/train-async":
            try:
                account = self._require_authenticated_account() if PAYWALL_ENFORCEMENT else self._current_account()
                if account:
                    self._require_pro_if_enforced(account)
            except Exception as exc:  # pragma: no cover
                self._send_error(exc)
                return
            def _train_job() -> dict:
                live_config = LIVE_SYNC_MANAGER.ensure_running().get("config", {})
                lookback_days_raw = pd.to_numeric(live_config.get("model_training_lookback_days"), errors="coerce")
                lookback_days = int(lookback_days_raw) if pd.notna(lookback_days_raw) and lookback_days_raw > 0 else None
                return train_engine(lookback_days=lookback_days)

            self._send_json(start_background_job("train", _train_job))
            return
        if self.path == "/api/predict":
            try:
                account = self._current_account()
                if PAYWALL_ENFORCEMENT and account is None:
                    self._send_json({"error": "Authentication required. Please log in."}, status=HTTPStatus.UNAUTHORIZED)
                    return
                try:
                    self._meter_usage(account, "prediction_runs", quantity=1, metadata={"endpoint": "/api/predict"})
                except PermissionError as exc:
                    self._send_json({"error": str(exc)}, status=HTTPStatus.PAYMENT_REQUIRED)
                    return
                length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                result = predict_engine(predict_all=bool(payload.get("predict_all", False)))
                self._send_json(result)
            except Exception as exc:  # pragma: no cover
                self._send_error(exc)
            return
        if self.path == "/api/recheck":
            try:
                account = self._require_authenticated_account() if PAYWALL_ENFORCEMENT else self._current_account()
                if account:
                    self._require_pro_if_enforced(account)
                payload = self._read_json_payload()
                lookback_days = payload.get("lookback_days")
                sample_rows = payload.get("sample_rows")

                def _recheck_job() -> dict:
                    return recheck_past_predictions(
                        lookback_days=lookback_days if lookback_days is not None else None,
                        sample_rows=sample_rows if sample_rows is not None else None,
                    )

                self._send_json(start_background_job("recheck", _recheck_job))
            except Exception as exc:  # pragma: no cover
                self._send_error(exc)
            return
        if self.path == "/api/benchmark/capture":
            try:
                account = self._require_authenticated_account() if PAYWALL_ENFORCEMENT else self._current_account()
                if account:
                    self._require_pro_if_enforced(account)
                try:
                    self._meter_usage(account, "benchmark_runs", quantity=1, metadata={"endpoint": "/api/benchmark/capture"})
                except PermissionError as exc:
                    self._send_json({"error": str(exc)}, status=HTTPStatus.PAYMENT_REQUIRED)
                    return
                result = capture_rotowire_benchmark_snapshot()
                result["status"] = _combined_app_status_cached(include_previews=False, force_refresh=True)
                self._send_json(result)
            except Exception as exc:  # pragma: no cover
                self._send_error(exc)
            return
        if self.path == "/api/benchmark/rotowire":
            try:
                account = self._require_authenticated_account() if PAYWALL_ENFORCEMENT else self._current_account()
                if account:
                    self._require_pro_if_enforced(account)
                try:
                    self._meter_usage(account, "benchmark_runs", quantity=1, metadata={"endpoint": "/api/benchmark/rotowire"})
                except PermissionError as exc:
                    self._send_json({"error": str(exc)}, status=HTTPStatus.PAYMENT_REQUIRED)
                    return
                payload = self._read_json_payload()
                lookback_days = _safe_int(payload.get("lookback_days"), 28)
                capture_now = bool(payload.get("capture_now", False))
                if capture_now:
                    capture_rotowire_benchmark_snapshot()
                result = run_rotowire_benchmark(lookback_days=max(1, lookback_days))
                result["status"] = _combined_app_status_cached(include_previews=False, force_refresh=True)
                self._send_json(result)
            except Exception as exc:  # pragma: no cover
                self._send_error(exc)
            return
        self.send_error(HTTPStatus.NOT_FOUND, "Not found.")

    def _handle_upload(self) -> None:
        try:
            fields = self._read_multipart_fields()
            kind = (fields.get("kind", {}).get("text") or "").strip()
            file_item = fields.get("file")

            if kind not in {"training", "upcoming", "context"}:
                raise ValueError("Upload kind must be 'training', 'upcoming', or 'context'.")
            if file_item is None or not file_item.get("filename"):
                raise ValueError("No file was uploaded.")

            if kind == "training":
                destination = DEFAULT_TRAINING_UPLOAD_PATH
            elif kind == "upcoming":
                destination = DEFAULT_UPCOMING_PATH
            else:
                destination = DEFAULT_CONTEXT_UPDATES_PATH
            destination.parent.mkdir(parents=True, exist_ok=True)
            with destination.open("wb") as output:
                value = file_item.get("value")
                output.write(value if isinstance(value, (bytes, bytearray)) else b"")

            self._send_json(
                {
                    "kind": kind,
                    "path": str(destination),
                    "status": _combined_app_status_cached(include_previews=False, force_refresh=True),
                }
            )
        except Exception as exc:  # pragma: no cover
            self._send_error(exc)

    def _handle_historical_import(self) -> None:
        try:
            if self._is_multipart():
                payload = self._read_uploaded_bytes_or_text()
                result = import_historical_bytes(payload)
            else:
                result = import_historical_text(self._read_json_payload().get("text", ""))
            result["status"] = _combined_app_status_cached(include_previews=False, force_refresh=True)
            self._send_json(result)
        except Exception as exc:  # pragma: no cover
            self._send_error(exc)

    def _handle_season_priors_import(self) -> None:
        try:
            if self._is_multipart():
                payload = self._read_uploaded_bytes_or_text()
                result = import_season_priors_bytes(payload)
            else:
                result = import_season_priors_text(self._read_json_payload().get("text", ""))
            result["status"] = _combined_app_status_cached(include_previews=False, force_refresh=True)
            self._send_json(result)
        except Exception as exc:  # pragma: no cover
            self._send_error(exc)

    def _handle_prizepicks_lines_import(self) -> None:
        try:
            if self._is_multipart():
                payload = self._read_uploaded_bytes_or_text()
                result = import_prizepicks_lines_bytes(payload)
            else:
                result = import_prizepicks_lines_text(self._read_json_payload().get("text", ""))
            result["status"] = _combined_app_status_cached(include_previews=False, force_refresh=True)
            self._send_json(result)
        except Exception as exc:  # pragma: no cover
            self._send_error(exc)

    def _handle_prizepicks_edges(self) -> None:
        try:
            payload = self._read_json_payload()
            result = generate_prizepicks_edges(slate_date=payload.get("slate_date"))
            result["status"] = _combined_app_status_cached(include_previews=False, force_refresh=True)
            self._send_json(result)
        except Exception as exc:  # pragma: no cover
            self._send_error(exc)

    def _is_multipart(self) -> bool:
        return self.headers.get("Content-Type", "").lower().startswith("multipart/form-data")

    def _read_request_body(self) -> bytes:
        length = int(self.headers.get("Content-Length", "0"))
        return self.rfile.read(length) if length else b"{}"

    def _read_json_payload(self) -> dict:
        body = self._read_request_body()
        return json.loads(body.decode("utf-8") or "{}")

    def _read_multipart_fields(self) -> dict[str, dict[str, object]]:
        content_type = self.headers.get("Content-Type", "")
        body = self._read_request_body()
        if not content_type.startswith("multipart/form-data"):
            return {}

        envelope = f"Content-Type: {content_type}\r\nMIME-Version: 1.0\r\n\r\n".encode("utf-8")
        message = BytesParser(policy=policy.default).parsebytes(envelope + body)

        fields: dict[str, dict[str, object]] = {}
        for part in message.iter_parts():
            name = part.get_param("name", header="content-disposition")
            if not name:
                continue
            filename = part.get_filename()
            payload = part.get_payload(decode=True)
            if payload is None:
                payload_text = part.get_payload()
                payload = str(payload_text).encode("utf-8")
            fields[name] = {
                "filename": filename,
                "value": payload,
                "text": part.get_content(),
            }
        return fields

    def _read_uploaded_bytes_or_text(self) -> bytes:
        if self._is_multipart():
            fields = self._read_multipart_fields()
            file_field = fields.get("file")
            if file_field is None or not file_field.get("filename"):
                raise ValueError("No file was uploaded.")
            value = file_field.get("value", b"")
            return value if isinstance(value, (bytes, bytearray)) else bytes(str(value), encoding="utf-8")

        payload = self._read_json_payload()
        text = str(payload.get("text", "") or "")
        return text.encode("utf-8")

    def _serve_file(self, path: Path, content_type: str) -> None:
        if not path.exists():
            self.send_error(HTTPStatus.NOT_FOUND, "File not found.")
            return
        content = path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self._send_cors_headers()
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def _send_cors_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Stripe-Signature")

    def _send_json(
        self,
        payload: dict,
        status: HTTPStatus = HTTPStatus.OK,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        content = json.dumps(_json_safe(payload), indent=2, allow_nan=False).encode("utf-8")
        self.send_response(status)
        self._send_cors_headers()
        if extra_headers:
            for key, value in extra_headers.items():
                self.send_header(str(key), str(value))
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def _send_error(self, exc: Exception) -> None:
        self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the local NBA prediction engine UI.")
    parser.add_argument("--host", default="127.0.0.1", help="Host interface to bind.")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    _ensure_commerce_schema()
    LIVE_SYNC_MANAGER.ensure_running()
    server = ThreadingHTTPServer((args.host, args.port), PredictionAppHandler)
    print(f"NBA prediction engine running at http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        LIVE_SYNC_MANAGER.stop()
        raise


if __name__ == "__main__":
    main()
