from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

try:
    from .engine import DEFAULT_BUNDLE_PATH, DEFAULT_METRICS_PATH, train_engine
    from .features import DEFAULT_MPLCONFIGDIR
except ImportError:
    from engine import DEFAULT_BUNDLE_PATH, DEFAULT_METRICS_PATH, train_engine
    from features import DEFAULT_MPLCONFIGDIR


DEFAULT_MPLCONFIGDIR.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("LOKY_MAX_CPU_COUNT", "1")
os.environ.setdefault("MPLCONFIGDIR", str(DEFAULT_MPLCONFIGDIR))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train the NBA multi-target prediction engine.")
    parser.add_argument("--data", type=Path, default=None, help="Optional path to a historical training CSV.")
    parser.add_argument("--bundle-out", type=Path, default=DEFAULT_BUNDLE_PATH, help="Path to save the model bundle.")
    parser.add_argument("--metrics-out", type=Path, default=DEFAULT_METRICS_PATH, help="Path to save engine metrics.")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=None,
        help="Optional rolling lookback window for training rows (defaults to engine setting).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    metrics = train_engine(
        data_path=args.data,
        bundle_path=args.bundle_out,
        metrics_path=args.metrics_out,
        lookback_days=args.lookback_days,
    )
    print(f"Saved engine bundle to {args.bundle_out}")
    print(f"Saved metrics to {args.metrics_out}")
    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
