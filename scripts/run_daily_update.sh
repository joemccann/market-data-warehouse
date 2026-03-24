#!/usr/bin/env bash
# Daily market data update wrapper for launchd/cron.
# Loads optional env files, activates the venv, then runs the retrying daily-update runner.

set -euo pipefail

WAREHOUSE="$HOME/market-warehouse"
VENV="$WAREHOUSE/.venv"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ENV="$SCRIPT_DIR/../.env"
WAREHOUSE_ENV="$WAREHOUSE/.env"

load_env_file() {
    local env_file="$1"
    if [ -f "$env_file" ]; then
        set -a
        # shellcheck disable=SC1090
        source "$env_file"
        set +a
    fi
}

load_env_file "$REPO_ENV"
load_env_file "$WAREHOUSE_ENV"
source "$VENV/bin/activate"
"$VENV/bin/python" "$SCRIPT_DIR/run_daily_update_job.py" "$@"
