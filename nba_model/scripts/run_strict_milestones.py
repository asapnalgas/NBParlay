#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import operator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


OPS: dict[str, Callable[[Any, Any], bool]] = {
    "==": operator.eq,
    "!=": operator.ne,
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le,
}


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _json_pointer_get(payload: Any, pointer: str) -> Any:
    if pointer == "" or pointer == "/":
        return payload
    current = payload
    parts = pointer.lstrip("/").split("/")
    for raw_part in parts:
        part = raw_part.replace("~1", "/").replace("~0", "~")
        if isinstance(current, list):
            index = int(part)
            current = current[index]
            continue
        if isinstance(current, dict):
            current = current[part]
            continue
        raise KeyError(f"Cannot traverse '{part}' in non-container value.")
    return current


def _coerce_compare(left: Any, right: Any) -> tuple[Any, Any]:
    if isinstance(left, bool) or isinstance(right, bool):
        return left, right
    try:
        left_num = float(left)
        right_num = float(right)
        return left_num, right_num
    except Exception:
        return left, right


def _eval_cmp(actual: Any, op_name: str, target: Any) -> tuple[bool, str]:
    op = OPS.get(op_name)
    if op is None:
        raise ValueError(f"Unsupported operator '{op_name}'.")
    left, right = _coerce_compare(actual, target)
    try:
        passed = bool(op(left, right))
    except Exception as exc:
        return False, f"comparison error: {exc}"
    return passed, ""


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except Exception:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def _eval_gate(gate: dict[str, Any], root: Path, week_results: dict[int, bool]) -> dict[str, Any]:
    gate_id = str(gate.get("id", "gate"))
    gate_type = str(gate.get("type", "")).strip()
    result: dict[str, Any] = {"id": gate_id, "type": gate_type, "passed": False}

    try:
        if gate_type == "path_exists":
            target = root / str(gate["path"])
            result["actual"] = target.exists()
            result["target"] = True
            result["passed"] = target.exists()
            return result

        if gate_type == "dir_exists":
            target = root / str(gate["path"])
            result["actual"] = target.is_dir()
            result["target"] = True
            result["passed"] = target.is_dir()
            return result

        if gate_type == "json_value_cmp":
            target_path = root / str(gate["path"])
            payload = _read_json(target_path)
            actual = _json_pointer_get(payload, str(gate["pointer"]))
            result["actual"] = actual
            result["target"] = gate.get("target")
            result["op"] = gate.get("op")
            passed, reason = _eval_cmp(actual, str(gate["op"]), gate.get("target"))
            result["passed"] = passed
            if reason:
                result["reason"] = reason
            return result

        if gate_type == "jsonl_count_cmp":
            target_path = root / str(gate["path"])
            count = 0
            with target_path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if line.strip():
                        count += 1
            result["actual"] = count
            result["target"] = gate.get("target")
            result["op"] = gate.get("op")
            passed, reason = _eval_cmp(count, str(gate["op"]), gate.get("target"))
            result["passed"] = passed
            if reason:
                result["reason"] = reason
            return result

        if gate_type == "jsonl_field_count_cmp":
            target_path = root / str(gate["path"])
            field = str(gate["field"])
            equals = gate.get("equals")
            rows = _load_jsonl(target_path)
            count = sum(1 for row in rows if row.get(field) == equals)
            result["actual"] = count
            result["target"] = gate.get("target")
            result["op"] = gate.get("op")
            result["field"] = field
            result["equals"] = equals
            passed, reason = _eval_cmp(count, str(gate["op"]), gate.get("target"))
            result["passed"] = passed
            if reason:
                result["reason"] = reason
            return result

        if gate_type == "json_object_array_key_contains":
            target_path = root / str(gate["path"])
            payload = _read_json(target_path)
            items = _json_pointer_get(payload, str(gate["pointer"]))
            if not isinstance(items, list):
                raise TypeError("Pointer does not reference an array.")
            key = str(gate["key"])
            wanted = set(str(v) for v in gate.get("values", []))
            actual_values = {str(item.get(key)) for item in items if isinstance(item, dict) and key in item}
            missing = sorted(wanted.difference(actual_values))
            result["actual"] = sorted(actual_values)
            result["target"] = sorted(wanted)
            result["missing"] = missing
            result["passed"] = not missing
            return result

        if gate_type == "csv_latest_date_unique_count_cmp":
            target_path = root / str(gate["path"])
            date_column = str(gate["date_column"])
            group_column = str(gate["group_column"])
            by_date: dict[str, set[str]] = {}
            with target_path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    date_value = str(row.get(date_column, "") or "").strip()
                    group_value = str(row.get(group_column, "") or "").strip()
                    if not date_value or not group_value:
                        continue
                    by_date.setdefault(date_value, set()).add(group_value)
            latest_date = max(by_date.keys()) if by_date else None
            unique_count = len(by_date.get(latest_date, set())) if latest_date else 0
            result["latest_date"] = latest_date
            result["actual"] = unique_count
            result["target"] = gate.get("target")
            result["op"] = gate.get("op")
            passed, reason = _eval_cmp(unique_count, str(gate["op"]), gate.get("target"))
            result["passed"] = passed
            if reason:
                result["reason"] = reason
            return result

        if gate_type == "week_passed":
            required_week = int(gate["week"])
            passed = bool(week_results.get(required_week, False))
            result["actual"] = passed
            result["target"] = True
            result["required_week"] = required_week
            result["passed"] = passed
            return result

        raise ValueError(f"Unsupported gate type '{gate_type}'.")
    except Exception as exc:
        result["passed"] = False
        result["error"] = str(exc)
        return result


def run_plan(config_path: Path, root: Path, through_week: int | None = None) -> dict[str, Any]:
    config = _read_json(config_path)
    weeks = sorted(config.get("weeks", []), key=lambda row: int(row.get("week", 0)))
    if through_week is None:
        through_week = max((int(week.get("week", 0)) for week in weeks), default=0)
    strict_order = bool(config.get("strict_order", True))

    week_results: dict[int, bool] = {}
    blocked = False
    report_weeks: list[dict[str, Any]] = []

    for week in weeks:
        week_number = int(week.get("week", 0))
        if week_number > through_week:
            continue
        week_payload: dict[str, Any] = {
            "week": week_number,
            "name": week.get("name"),
            "goals": week.get("goals", []),
            "gates": [],
            "passed": False,
            "status": "pending",
        }
        if blocked and strict_order:
            week_payload["status"] = "blocked"
            week_payload["reason"] = "blocked_by_prior_week_failure"
            report_weeks.append(week_payload)
            week_results[week_number] = False
            continue

        gate_results = []
        for gate in week.get("gates", []):
            gate_results.append(_eval_gate(gate, root=root, week_results=week_results))

        week_payload["gates"] = gate_results
        week_passed = all(bool(gate.get("passed")) for gate in gate_results)
        week_payload["passed"] = week_passed
        week_payload["status"] = "passed" if week_passed else "failed"
        report_weeks.append(week_payload)
        week_results[week_number] = week_passed

        if strict_order and not week_passed:
            blocked = True

    all_evaluated = [week for week in report_weeks if week.get("status") in {"passed", "failed"}]
    overall_passed = all(bool(week.get("passed")) for week in all_evaluated) if all_evaluated else False
    highest_passed_week = max((int(week["week"]) for week in report_weeks if week.get("status") == "passed"), default=0)
    first_failed_week = next((int(week["week"]) for week in report_weeks if week.get("status") == "failed"), None)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "plan_id": config.get("plan_id"),
        "plan_name": config.get("name"),
        "version": config.get("version"),
        "strict_order": strict_order,
        "through_week": through_week,
        "overall_passed": overall_passed,
        "highest_passed_week": highest_passed_week,
        "first_failed_week": first_failed_week,
        "weeks": report_weeks,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run strict weekly milestone gates in order.")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/strict_milestones_v1.json"),
        help="Path to milestone config JSON.",
    )
    parser.add_argument(
        "--through-week",
        type=int,
        default=None,
        help="Evaluate through this week number (default: all weeks).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("models/strict_milestone_status.json"),
        help="Where to write milestone run report JSON.",
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    config_path = (root / args.config).resolve() if not args.config.is_absolute() else args.config
    output_path = (root / args.output).resolve() if not args.output.is_absolute() else args.output

    report = run_plan(config_path=config_path, root=root, through_week=args.through_week)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    if not bool(report.get("overall_passed")):
        raise SystemExit(2)


if __name__ == "__main__":
    main()
