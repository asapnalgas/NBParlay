#!/bin/zsh
set -euo pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
APP_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)

REPO_ROOT="$(git -C "$APP_DIR" rev-parse --show-toplevel 2>/dev/null || true)"
if [ -z "$REPO_ROOT" ]; then
  echo "No git repo found. Initializing a new repo in $APP_DIR"
  git -C "$APP_DIR" init -b main
  REPO_ROOT="$APP_DIR"
fi

if [ "$REPO_ROOT" = "$APP_DIR" ]; then
  APP_PATH_SPEC="."
else
  APP_PATH_SPEC="$(basename "$APP_DIR")"
fi

REMOTE_URL="${1:-}"
REMOTE_NAME="${REMOTE_NAME:-origin}"
if [ -n "$REMOTE_URL" ]; then
  if git -C "$REPO_ROOT" remote get-url "$REMOTE_NAME" >/dev/null 2>&1; then
    git -C "$REPO_ROOT" remote set-url "$REMOTE_NAME" "$REMOTE_URL"
  else
    git -C "$REPO_ROOT" remote add "$REMOTE_NAME" "$REMOTE_URL"
  fi
fi

COMMIT_MESSAGE="${COMMIT_MESSAGE:-chore: sync nba_model app}"
git -C "$REPO_ROOT" add "$APP_PATH_SPEC"

if git -C "$REPO_ROOT" diff --cached --quiet; then
  echo "No staged changes for $APP_PATH_SPEC."
else
  git -C "$REPO_ROOT" commit -m "$COMMIT_MESSAGE"
  echo "Committed changes with message: $COMMIT_MESSAGE"
fi

if [ "${PUSH:-0}" = "1" ]; then
  current_branch="$(git -C "$REPO_ROOT" branch --show-current)"
  git -C "$REPO_ROOT" push -u "$REMOTE_NAME" "$current_branch"
  echo "Pushed $current_branch to $REMOTE_NAME."
else
  echo "Skipping push. To push now: PUSH=1 $0 \"$REMOTE_URL\""
fi
