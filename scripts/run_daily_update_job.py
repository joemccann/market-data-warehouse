#!/usr/bin/env python3
"""Retrying runner for the scheduled daily parquet-first sync."""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence


SCRIPT_DIR = Path(__file__).resolve().parent

ASSET_CLASSES = ["equity", "volatility", "futures"]

# CBOE-only volatility indices not available via IB
CBOE_ONLY_SYMBOLS = ["VXHYG", "VXSMH"]


@dataclass(frozen=True)
class RunnerConfig:
    warehouse_dir: Path
    log_dir: Path
    daily_update_script: Path
    alert_script: Path
    python_bin: str
    node_bin: str
    max_attempts: int
    retry_delay_seconds: int


@dataclass(frozen=True)
class AlertRequest:
    run_date: str
    log_file: Path
    attempts: int | None
    exit_code: int | None
    error_summary: str
    repo_root: Path


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _read_positive_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default

    value = int(raw)
    if value < 1:
        raise ValueError(f"{name} must be >= 1, got {value}")
    return value


def build_config() -> RunnerConfig:
    warehouse_dir = Path(
        os.getenv("MDW_WAREHOUSE_DIR", str(Path.home() / "market-warehouse"))
    ).expanduser()
    log_dir = Path(
        os.getenv("MDW_DAILY_UPDATE_LOG_DIR", str(warehouse_dir / "logs"))
    ).expanduser()
    node_bin = os.getenv("MDW_NODE_BIN") or shutil.which("node") or "/opt/homebrew/bin/node"

    return RunnerConfig(
        warehouse_dir=warehouse_dir,
        log_dir=log_dir,
        daily_update_script=Path(
            os.getenv("MDW_DAILY_UPDATE_SCRIPT", str(SCRIPT_DIR / "daily_update.py"))
        ).expanduser(),
        alert_script=Path(
            os.getenv(
                "MDW_DAILY_UPDATE_ALERT_SCRIPT",
                str(SCRIPT_DIR / "send_daily_update_failure_email.mjs"),
            )
        ).expanduser(),
        python_bin=os.getenv("MDW_DAILY_UPDATE_PYTHON_BIN", sys.executable),
        node_bin=node_bin,
        max_attempts=_read_positive_int_env("MDW_DAILY_UPDATE_MAX_ATTEMPTS", 3),
        retry_delay_seconds=_read_positive_int_env(
            "MDW_DAILY_UPDATE_RETRY_DELAY_SECONDS", 300
        ),
    )


def build_log_file(log_dir: Path, now: datetime | None = None) -> Path:
    current = now or datetime.now()
    return log_dir / f"daily_update_{current:%Y-%m-%d}.log"


def append_log(log_file: Path, message: str) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("a", encoding="utf-8") as handle:
        handle.write(message)
        if not message.endswith("\n"):
            handle.write("\n")


def build_daily_update_command(
    config: RunnerConfig, daily_update_args: Sequence[str]
) -> list[str]:
    return [config.python_bin, str(config.daily_update_script), *daily_update_args]


def build_cboe_volatility_command(
    config: RunnerConfig, symbols: Sequence[str]
) -> list[str]:
    cboe_script = SCRIPT_DIR / "fetch_cboe_volatility.py"
    return [config.python_bin, str(cboe_script), "--symbols", *symbols]


def build_alert_command(config: RunnerConfig, request: AlertRequest) -> list[str]:
    command = [
        config.node_bin,
        str(config.alert_script),
        "--run-date",
        request.run_date,
        "--log-file",
        str(request.log_file),
        "--error-summary",
        request.error_summary,
        "--repo-root",
        str(request.repo_root),
        "--job-name",
        "daily_update",
    ]
    if request.attempts is not None:
        command.extend(["--attempts", str(request.attempts)])
    if request.exit_code is not None:
        command.extend(["--exit-code", str(request.exit_code)])
    return command


def node_binary_exists(node_bin: str) -> bool:
    if Path(node_bin).is_absolute():
        return Path(node_bin).exists()
    return shutil.which(node_bin) is not None


def run_daily_update_attempt(
    command: Sequence[str],
    log_file: Path,
    env: dict[str, str] | None = None,
    runner: callable = subprocess.run,
) -> subprocess.CompletedProcess:
    with log_file.open("a", encoding="utf-8") as handle:
        return runner(
            list(command),
            stdout=handle,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
            check=False,
        )


def send_failure_alert(
    config: RunnerConfig,
    request: AlertRequest,
    log_file: Path,
    env: dict[str, str] | None = None,
    runner: callable = subprocess.run,
) -> subprocess.CompletedProcess | None:
    if not node_binary_exists(config.node_bin):
        append_log(
            log_file,
            f"WARNING: node binary not found at {config.node_bin}; skipping failure email",
        )
        return None

    if not config.alert_script.exists():
        append_log(
            log_file,
            f"WARNING: alert script not found at {config.alert_script}; skipping failure email",
        )
        return None

    alert_command = build_alert_command(config, request)
    append_log(log_file, f"Triggering failure alert via: {' '.join(alert_command)}")
    return runner(
        alert_command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
        check=False,
    )


def extract_error_summary(log_file: Path) -> str:
    try:
        lines = log_file.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return "Daily update failed, and the log file was not found."

    for line in reversed(lines):
        stripped = line.strip()
        if stripped and not stripped.startswith("==="):
            return stripped
    return "Daily update failed with no error summary captured in the log."


def log_has_completion_marker(log_file: Path) -> bool:
    try:
        for line in log_file.read_text(encoding="utf-8").splitlines():
            if line.startswith("=== Done "):
                return True
    except FileNotFoundError:
        return False
    return False


def run_with_retries(
    config: RunnerConfig,
    daily_update_args: Sequence[str],
    env: dict[str, str] | None = None,
    sleep_fn: callable = time.sleep,
    runner: callable = subprocess.run,
    now_fn: callable = _utc_now,
) -> int:
    started_at = now_fn()
    log_file = build_log_file(config.log_dir, started_at)
    command = tuple(build_daily_update_command(config, daily_update_args))

    append_log(log_file, f"=== Daily Update {started_at:%Y-%m-%dT%H:%M:%SZ} ===\n")
    append_log(log_file, f"Runner command: {' '.join(command)}")
    append_log(
        log_file,
        (
            "Runner config: "
            f"attempts={config.max_attempts} "
            f"retry_delay_seconds={config.retry_delay_seconds} "
            f"hostname={socket.gethostname()}"
        ),
    )

    final_exit_code = 1
    for attempt in range(1, config.max_attempts + 1):
        append_log(
            log_file,
            f"=== Attempt {attempt}/{config.max_attempts} {now_fn():%Y-%m-%dT%H:%M:%SZ} ===",
        )
        result = run_daily_update_attempt(command, log_file, env=env, runner=runner)
        final_exit_code = result.returncode

        if result.returncode == 0:
            append_log(
                log_file,
                (
                    "=== Done "
                    f"{now_fn():%Y-%m-%dT%H:%M:%SZ} "
                    f"(attempt {attempt}/{config.max_attempts}) ==="
                ),
            )
            return 0

        append_log(
            log_file,
            (
                "=== Attempt failed "
                f"{now_fn():%Y-%m-%dT%H:%M:%SZ} "
                f"(attempt {attempt}/{config.max_attempts}, exit_code={result.returncode}) ==="
            ),
        )

        if attempt < config.max_attempts:
            append_log(
                log_file,
                f"Retrying in {config.retry_delay_seconds} seconds...",
            )
            sleep_fn(config.retry_delay_seconds)

    append_log(
        log_file,
        (
            "=== Failed "
            f"{now_fn():%Y-%m-%dT%H:%M:%SZ} "
            f"after {config.max_attempts} attempt(s) ==="
        ),
    )

    alert_request = AlertRequest(
        run_date=log_file.stem.removeprefix("daily_update_"),
        log_file=log_file,
        attempts=config.max_attempts,
        exit_code=final_exit_code,
        error_summary=extract_error_summary(log_file),
        repo_root=SCRIPT_DIR.parent,
    )
    alert_result = send_failure_alert(
        config,
        alert_request,
        log_file,
        env=env,
        runner=runner,
    )
    if alert_result is None:
        return final_exit_code

    alert_output = (alert_result.stdout or "").strip()
    if alert_result.returncode == 0:
        append_log(log_file, f"Failure alert sent successfully. {alert_output}".strip())
    else:
        append_log(
            log_file,
            (
                "WARNING: failure alert returned non-zero exit code "
                f"{alert_result.returncode}. {alert_output}"
            ).strip(),
        )

    return final_exit_code


def run_cboe_volatility_sync(
    config: RunnerConfig,
    symbols: Sequence[str],
    env: dict[str, str] | None = None,
    runner: callable = subprocess.run,
    now_fn: callable = _utc_now,
) -> int:
    """Sync CBOE-only volatility indices directly from CBOE API."""
    started_at = now_fn()
    log_file = build_log_file(config.log_dir, started_at)
    command = build_cboe_volatility_command(config, symbols)

    append_log(
        log_file,
        f"=== CBOE Volatility Sync {started_at:%Y-%m-%dT%H:%M:%SZ} ===",
    )
    append_log(log_file, f"Command: {' '.join(command)}")

    result = run_daily_update_attempt(command, log_file, env=env, runner=runner)

    if result.returncode == 0:
        append_log(
            log_file,
            f"=== CBOE Volatility Sync Done {now_fn():%Y-%m-%dT%H:%M:%SZ} ===",
        )
    else:
        append_log(
            log_file,
            f"=== CBOE Volatility Sync Failed {now_fn():%Y-%m-%dT%H:%M:%SZ} (exit_code={result.returncode}) ===",
        )

    return result.returncode


def main(argv: Sequence[str] | None = None) -> int:
    config = build_config()
    args = list(argv or sys.argv[1:])
    env = os.environ.copy()

    # If --asset-class is explicitly specified, run just that one.
    if "--asset-class" in args:
        return run_with_retries(config, args, env=env)

    # Otherwise, run all asset classes sequentially.
    final_code = 0
    for asset_class in ASSET_CLASSES:
        code = run_with_retries(
            config, args + ["--asset-class", asset_class], env=env
        )
        if code != 0:
            final_code = code

    # Also sync CBOE-only volatility indices (VXHYG, VXSMH)
    if CBOE_ONLY_SYMBOLS:
        cboe_code = run_cboe_volatility_sync(config, CBOE_ONLY_SYMBOLS, env=env)
        if cboe_code != 0:
            final_code = cboe_code

    return final_code


if __name__ == "__main__":
    raise SystemExit(main())
