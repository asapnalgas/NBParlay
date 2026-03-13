from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable


@dataclass(frozen=True)
class SupportModuleSpec:
    key: str
    name: str
    description: str
    dependencies: tuple[str, ...] = ()
    critical: bool = False


SUPPORT_MODULE_SPECS: tuple[SupportModuleSpec, ...] = (
    SupportModuleSpec(
        key="live_ingest",
        name="Live Ingest",
        description="Continuously ingests schedule, scoreboard, boxscore, and provider context feeds.",
        critical=True,
    ),
    SupportModuleSpec(
        key="notes_engine",
        name="Notes Engine",
        description="Builds live action notes, postgame notes, and compiled daily notes context.",
        dependencies=("live_ingest",),
    ),
    SupportModuleSpec(
        key="model_trainer",
        name="Model Trainer",
        description="Retrains bundles and refreshes projections from synced data.",
        dependencies=("live_ingest",),
        critical=True,
    ),
    SupportModuleSpec(
        key="backtester",
        name="Backtester",
        description="Runs benchmark snapshots and formal benchmark evaluations.",
        dependencies=("model_trainer",),
    ),
    SupportModuleSpec(
        key="alerts",
        name="Alerts",
        description="Produces runtime diagnostics and actionable reliability warnings.",
        dependencies=("live_ingest",),
    ),
)


def support_module_specs() -> tuple[SupportModuleSpec, ...]:
    return SUPPORT_MODULE_SPECS


def default_support_module_config() -> dict[str, dict[str, bool]]:
    return {spec.key: {"enabled": True} for spec in SUPPORT_MODULE_SPECS}


def normalize_support_module_config(value: object) -> dict[str, dict[str, bool]]:
    normalized = default_support_module_config()
    if not isinstance(value, dict):
        return normalized
    for spec in SUPPORT_MODULE_SPECS:
        raw_module = value.get(spec.key)
        if isinstance(raw_module, dict):
            normalized[spec.key]["enabled"] = bool(raw_module.get("enabled", True))
        elif raw_module is not None:
            normalized[spec.key]["enabled"] = bool(raw_module)
    return normalized


def module_enabled(modules_config: dict[str, dict[str, bool]], key: str, *, default: bool = True) -> bool:
    module = modules_config.get(key)
    if not isinstance(module, dict):
        return bool(default)
    return bool(module.get("enabled", default))


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_support_module_snapshot(
    modules_config: dict[str, dict[str, bool]],
    state: dict,
) -> dict[str, dict]:
    providers = state.get("providers")
    if not isinstance(providers, dict):
        providers = {}
    snapshot: dict[str, dict] = {}
    for spec in SUPPORT_MODULE_SPECS:
        enabled = module_enabled(modules_config, spec.key, default=True)
        health = "healthy"
        note = ""
        metrics: dict[str, int | float | str | None] = {}

        if spec.key == "live_ingest":
            rows_ingested = int(state.get("completed_rows_appended", 0) or 0)
            rows_provider = int(state.get("provider_context_rows", 0) or 0)
            upcoming_rows = int(state.get("upcoming_rows_generated", 0) or 0)
            metrics = {
                "rows_ingested": rows_ingested,
                "rows_provider": rows_provider,
                "upcoming_rows": upcoming_rows,
                "games_seen": int(state.get("games_seen", 0) or 0),
            }
            if enabled and int(state.get("games_seen", 0) or 0) <= 0:
                health = "degraded"
                note = "No games were seen in the latest cycle."
            if state.get("last_error"):
                health = "degraded"
                note = str(state.get("last_error"))

        elif spec.key == "notes_engine":
            live_rows = int(state.get("game_notes_live_rows", 0) or 0)
            postgame_rows = int(state.get("postgame_review_rows", 0) or 0)
            daily_rows = int(state.get("game_notes_daily_rows", 0) or 0)
            news_rows = int(state.get("news_rows_matched", 0) or 0)
            metrics = {
                "live_rows": live_rows,
                "postgame_rows": postgame_rows,
                "daily_rows": daily_rows,
                "news_rows": news_rows,
            }
            if enabled and (live_rows + postgame_rows + daily_rows + news_rows) <= 0:
                health = "degraded"
                note = "No note context rows were produced."

        elif spec.key == "model_trainer":
            metrics = {
                "last_train_triggered": bool(state.get("last_train_triggered", False)),
                "last_predict_triggered": bool(state.get("last_predict_triggered", False)),
                "prediction_rows_used": int(state.get("prediction_rows_used", 0) or 0),
            }
            if enabled and int(state.get("prediction_rows_used", 0) or 0) <= 0:
                health = "degraded"
                note = "Projection refresh produced no rows in the latest cycle."

        elif spec.key == "backtester":
            benchmark_rows = int(state.get("benchmark_rows_evaluated", 0) or 0)
            metrics = {
                "benchmark_rows_evaluated": benchmark_rows,
                "last_benchmark_run_at": state.get("last_benchmark_run_at"),
            }
            if enabled and benchmark_rows <= 0:
                health = "degraded"
                note = str(state.get("benchmark_last_note") or "Benchmark has not evaluated rows yet.")
            if state.get("benchmark_last_error"):
                health = "degraded"
                note = str(state.get("benchmark_last_error"))

        elif spec.key == "alerts":
            warning_count = int(state.get("contract_drift_warning_count", 0) or 0)
            loop_error = state.get("live_loop_last_error")
            metrics = {
                "warning_count": warning_count,
                "live_loop_last_error": loop_error,
            }
            if loop_error:
                health = "warning"
                note = str(loop_error)
            elif warning_count > 0:
                health = "warning"
                note = f"{warning_count} contract drift warnings are active."
            else:
                health = "healthy"
                note = "No active warnings."

        if not enabled:
            health = "disabled"
            note = "Module is disabled in live config."

        snapshot[spec.key] = {
            "key": spec.key,
            "name": spec.name,
            "description": spec.description,
            "dependencies": list(spec.dependencies),
            "critical": bool(spec.critical),
            "enabled": bool(enabled),
            "health": health,
            "note": note,
            "metrics": metrics,
            "updated_at": _iso_now(),
        }
    return snapshot


def summarize_module_alerts(
    snapshot: dict[str, dict],
    *,
    include_disabled: bool = False,
) -> list[dict]:
    alerts: list[dict] = []
    for spec in SUPPORT_MODULE_SPECS:
        module = snapshot.get(spec.key)
        if not isinstance(module, dict):
            continue
        health = str(module.get("health", "unknown"))
        enabled = bool(module.get("enabled", False))
        if not include_disabled and not enabled:
            continue
        if health in {"healthy"}:
            continue
        severity = "warning"
        if health == "degraded":
            severity = "error" if spec.critical else "warning"
        if health == "disabled":
            severity = "info"
        alerts.append(
            {
                "module": spec.key,
                "name": spec.name,
                "severity": severity,
                "health": health,
                "message": str(module.get("note") or f"{spec.name} requires attention."),
            }
        )
    return alerts


def ordered_module_keys(specs: Iterable[SupportModuleSpec] | None = None) -> list[str]:
    active_specs = tuple(specs) if specs is not None else SUPPORT_MODULE_SPECS
    return [spec.key for spec in active_specs]
