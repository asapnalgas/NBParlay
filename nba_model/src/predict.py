from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

try:
    from .engine import DEFAULT_BUNDLE_PATH, DEFAULT_PREDICTIONS_PATH, predict_engine
    from .features import DEFAULT_MPLCONFIGDIR
except ImportError:
    from engine import DEFAULT_BUNDLE_PATH, DEFAULT_PREDICTIONS_PATH, predict_engine
    from features import DEFAULT_MPLCONFIGDIR


DEFAULT_MPLCONFIGDIR.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("LOKY_MAX_CPU_COUNT", "1")
os.environ.setdefault("MPLCONFIGDIR", str(DEFAULT_MPLCONFIGDIR))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate NBA engine predictions from a saved model bundle.")
    parser.add_argument("--input", type=Path, default=None, help="Optional path to a historical or upcoming CSV.")
    parser.add_argument("--bundle", type=Path, default=DEFAULT_BUNDLE_PATH, help="Path to the saved model bundle.")
    parser.add_argument("--output", type=Path, default=DEFAULT_PREDICTIONS_PATH, help="Path to save predictions.")
    parser.add_argument("--predict-all", action="store_true", help="Score every row in the input file.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    payload = predict_engine(
        input_path=args.input,
        bundle_path=args.bundle,
        output_path=args.output,
        predict_all=args.predict_all,
    )
    print(f"Saved predictions to {args.output}")
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
