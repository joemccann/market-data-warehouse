"""Install a machine-local secure IBC service under the user's home directory."""

from __future__ import annotations

import argparse
import os
import plistlib
import shutil
import subprocess
from pathlib import Path
from typing import Any, Sequence

SERVICE_LABEL = "local.ibc-gateway"
LEGACY_SERVICE_LABELS = (
    "com.market-warehouse.ibc-gateway",
    "com.convex-scavenger.ibc-gateway",
)
DEFAULT_TWS_MAJOR_VRSN = "10.44"
DEFAULT_KEYCHAIN_ACCOUNT = "ibc"
DEFAULT_USERNAME_SERVICE = "com.market-warehouse.ibc.username"
DEFAULT_PASSWORD_SERVICE = "com.market-warehouse.ibc.password"
DEFAULT_SCHEDULE = [
    {"Hour": 0, "Minute": 0, "Weekday": weekday} for weekday in range(1, 6)
]


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse installer arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Install or update a machine-local secure IBC LaunchAgent and the "
            "wrappers it uses under ~/ibc/bin."
        )
    )
    parser.add_argument("--home", type=Path, default=Path.home())
    parser.add_argument("--ibc-dir", type=Path)
    parser.add_argument("--ibc-install-dir", type=Path)
    parser.add_argument("--launch-agents-dir", type=Path)
    parser.add_argument("--applications-dir", type=Path)
    parser.add_argument("--tws-settings-path", type=Path)
    parser.add_argument("--service-label", default=SERVICE_LABEL)
    parser.add_argument(
        "--legacy-label",
        action="append",
        dest="legacy_labels",
        help="Legacy LaunchAgent label to migrate. May be provided more than once.",
    )
    parser.add_argument("--keychain-account", default=DEFAULT_KEYCHAIN_ACCOUNT)
    parser.add_argument("--username-service", default=DEFAULT_USERNAME_SERVICE)
    parser.add_argument("--password-service", default=DEFAULT_PASSWORD_SERVICE)
    parser.add_argument("--tws-major-version")
    parser.add_argument(
        "--manual-only",
        action="store_true",
        help="Disable RunAtLoad and any launchd schedule; start only via the helper scripts.",
    )
    parser.add_argument(
        "--no-bootstrap",
        action="store_true",
        help="Write files but do not bootstrap the LaunchAgent into launchd.",
    )
    args = parser.parse_args(argv)

    args.home = args.home.expanduser()
    args.ibc_dir = (
        args.ibc_dir.expanduser() if args.ibc_dir else args.home / "ibc"
    )
    args.ibc_install_dir = (
        args.ibc_install_dir.expanduser()
        if args.ibc_install_dir
        else args.home / "ibc-install"
    )
    args.launch_agents_dir = (
        args.launch_agents_dir.expanduser()
        if args.launch_agents_dir
        else args.home / "Library" / "LaunchAgents"
    )
    args.applications_dir = (
        args.applications_dir.expanduser()
        if args.applications_dir
        else args.home / "Applications"
    )
    args.tws_settings_path = (
        args.tws_settings_path.expanduser()
        if args.tws_settings_path
        else args.home / "Jts"
    )
    args.legacy_labels = (
        args.legacy_labels[:] if args.legacy_labels else list(LEGACY_SERVICE_LABELS)
    )
    return args


def read_plist(path: Path) -> dict[str, Any]:
    """Read a plist file if it exists."""
    if not path.exists():
        return {}
    with path.open("rb") as handle:
        return plistlib.load(handle)


def read_shell_assignment(path: Path, key: str) -> str | None:
    """Read a simple KEY=value assignment from a shell file."""
    if not path.exists():
        return None
    prefix = f"{key}="
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith(prefix):
            return line.split("=", 1)[1].strip()
    return None


def agent_labels_for_lookup(args: argparse.Namespace) -> list[str]:
    """Return the current service label followed by any legacy labels."""
    labels = [args.service_label, *args.legacy_labels]
    return list(dict.fromkeys(labels))


def detect_tws_major_version(args: argparse.Namespace) -> str:
    """Detect the TWS major version from args, legacy LaunchAgent, or IBC wrapper."""
    if args.tws_major_version:
        return args.tws_major_version

    for label in agent_labels_for_lookup(args):
        agent_plist = args.launch_agents_dir / f"{label}.plist"
        env = read_plist(agent_plist).get("EnvironmentVariables", {})
        if env.get("TWS_MAJOR_VRSN"):
            return str(env["TWS_MAJOR_VRSN"])

    wrapper_path = args.ibc_install_dir / "gatewaystartmacos.sh"
    wrapper_version = read_shell_assignment(wrapper_path, "TWS_MAJOR_VRSN")
    if wrapper_version:
        return wrapper_version

    return DEFAULT_TWS_MAJOR_VRSN


def resolve_schedule(args: argparse.Namespace) -> tuple[list[dict[str, int]], bool]:
    """Use the current or legacy LaunchAgent schedule if present."""
    if args.manual_only:
        return [], False

    for label in agent_labels_for_lookup(args):
        agent_plist = args.launch_agents_dir / f"{label}.plist"
        data = read_plist(agent_plist)
        if data:
            schedule = data.get("StartCalendarInterval", DEFAULT_SCHEDULE)
            run_at_load = bool(data.get("RunAtLoad", True))
            return schedule, run_at_load

    return DEFAULT_SCHEDULE, True


def strip_credentials_from_text(text: str) -> tuple[str, str | None, str | None]:
    """Remove IbLoginId/IbPassword lines from config text."""
    username = None
    password = None
    kept_lines: list[str] = []

    for line in text.splitlines():
        stripped = line.lstrip()
        if stripped.startswith("IbLoginId="):
            username = stripped.split("=", 1)[1]
            continue
        if stripped.startswith("IbPassword="):
            password = stripped.split("=", 1)[1]
            continue
        kept_lines.append(line)

    sanitized = "\n".join(kept_lines)
    if kept_lines or text.endswith("\n"):
        sanitized += "\n"
    return sanitized, username, password


def ensure_secure_config(config_path: Path, source_path: Path) -> None:
    """Create the secure config from the stock IBC config if needed."""
    if config_path.exists():
        return
    if not source_path.exists():
        raise RuntimeError(f"IBC source config not found: {source_path}")
    config_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source_path, config_path)


def read_keychain_secret(service: str, account: str) -> str | None:
    """Return the Keychain secret if it exists."""
    result = subprocess.run(
        [
            "security",
            "find-generic-password",
            "-a",
            account,
            "-s",
            service,
            "-w",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return None
    return result.stdout.rstrip("\n")


def store_keychain_secret(service: str, account: str, secret: str) -> None:
    """Store a secret in the Keychain and trust /usr/bin/security for later reads."""
    subprocess.run(
        [
            "security",
            "add-generic-password",
            "-U",
            "-a",
            account,
            "-s",
            service,
            "-w",
            secret,
            "-T",
            "/usr/bin/security",
        ],
        check=True,
        capture_output=True,
        text=True,
    )


def ensure_keychain_and_sanitize_config(
    config_path: Path,
    account: str,
    username_service: str,
    password_service: str,
) -> str:
    """Make Keychain the source of truth and strip plaintext credentials from disk."""
    text = config_path.read_text(encoding="utf-8")
    sanitized, config_username, config_password = strip_credentials_from_text(text)
    keychain_username = read_keychain_secret(username_service, account)
    keychain_password = read_keychain_secret(password_service, account)

    if keychain_username is None or keychain_password is None:
        if not config_username or not config_password:
            raise RuntimeError(
                "IB credentials were not found in Keychain and are not fully present in "
                f"{config_path}"
            )
        store_keychain_secret(username_service, account, config_username)
        store_keychain_secret(password_service, account, config_password)
        status = "migrated credentials from config.secure.ini into Keychain"
    else:
        store_keychain_secret(username_service, account, keychain_username)
        store_keychain_secret(password_service, account, keychain_password)
        status = "refreshed Keychain trusted-app access for existing credentials"

    if config_username is not None or config_password is not None:
        config_path.write_text(sanitized, encoding="utf-8")
        if status.startswith("refreshed"):
            status += " and stripped plaintext credentials from config.secure.ini"

    return status


def render_runner_script(
    ibc_dir: Path,
    ibc_install_dir: Path,
    applications_dir: Path,
    tws_settings_path: Path,
    tws_major_version: str,
    account: str,
    username_service: str,
    password_service: str,
) -> str:
    """Render the machine-local secure runner script."""
    return f"""#!/usr/bin/env bash
set -euo pipefail

IBC_DIR="${{IBC_DIR:-{ibc_dir}}}"
IBC_INSTALL_DIR="${{IBC_INSTALL_DIR:-{ibc_install_dir}}}"
CONFIG_PATH="${{IBC_CONFIG_PATH:-{ibc_dir / 'config.secure.ini'}}}"
TWS_PATH="${{IBC_TWS_PATH:-{applications_dir}}}"
TWS_SETTINGS_PATH="${{IBC_TWS_SETTINGS_PATH:-{tws_settings_path}}}"
KEYCHAIN_ACCOUNT="${{IBC_KEYCHAIN_ACCOUNT:-{account}}}"
USERNAME_SERVICE="${{IBC_KEYCHAIN_USERNAME_SERVICE:-{username_service}}}"
PASSWORD_SERVICE="${{IBC_KEYCHAIN_PASSWORD_SERVICE:-{password_service}}}"
TWS_MAJOR_VRSN="${{IBC_TWS_MAJOR_VRSN:-{tws_major_version}}}"
TRADING_MODE="${{IBC_TRADING_MODE:-live}}"
TWOFA_TIMEOUT_ACTION="${{IBC_TWOFA_TIMEOUT_ACTION:-restart}}"
RUNTIME_DIR="${{IBC_RUNTIME_DIR:-$IBC_DIR/run}}"

need_cmd() {{
  command -v "$1" >/dev/null 2>&1
}}

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "This script is for macOS only." >&2
  exit 1
fi

if ! need_cmd security; then
  echo "'security' was not found in PATH." >&2
  exit 1
fi

if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "Secure IBC config not found: $CONFIG_PATH" >&2
  exit 1
fi

if [[ ! -x "$IBC_INSTALL_DIR/scripts/ibcstart.sh" ]]; then
  echo "IBC launcher not found or not executable: $IBC_INSTALL_DIR/scripts/ibcstart.sh" >&2
  exit 1
fi

mkdir -p "$IBC_DIR/logs" "$RUNTIME_DIR"

if ! IBC_USERNAME="$(security find-generic-password -w -a "$KEYCHAIN_ACCOUNT" -s "$USERNAME_SERVICE" 2>/dev/null)"; then
  echo "Keychain username item not found for service '$USERNAME_SERVICE'." >&2
  exit 1
fi

if ! IBC_PASSWORD="$(security find-generic-password -w -a "$KEYCHAIN_ACCOUNT" -s "$PASSWORD_SERVICE" 2>/dev/null)"; then
  echo "Keychain password item not found for service '$PASSWORD_SERVICE'." >&2
  exit 1
fi

RUNTIME_CONFIG="$(mktemp "${{RUNTIME_DIR%/}}/config.runtime.XXXXXX")"

cleanup() {{
  local exit_code=$?
  rm -f "$RUNTIME_CONFIG"
  unset IBC_USERNAME IBC_PASSWORD
  exit "$exit_code"
}}
trap cleanup EXIT INT TERM

awk -v ibc_username="$IBC_USERNAME" -v ibc_password="$IBC_PASSWORD" '
  !/^[[:space:]]*IbLoginId=/ && !/^[[:space:]]*IbPassword=/ {{ print }}
  END {{
    print "IbLoginId=" ibc_username
    print "IbPassword=" ibc_password
  }}
' "$CONFIG_PATH" > "$RUNTIME_CONFIG"

chmod 600 "$RUNTIME_CONFIG"

exec "$IBC_INSTALL_DIR/scripts/ibcstart.sh" "$TWS_MAJOR_VRSN" --gateway \
  "--tws-path=$TWS_PATH" \
  "--tws-settings-path=$TWS_SETTINGS_PATH" \
  "--ibc-path=$IBC_INSTALL_DIR" \
  "--ibc-ini=$RUNTIME_CONFIG" \
  "--mode=$TRADING_MODE" \
  "--on2fatimeout=$TWOFA_TIMEOUT_ACTION"
"""


def render_service_script(action: str, label: str) -> str:
    """Render a launchctl management wrapper for the installed service."""
    target = f'gui/$(id -u)/{label}'
    plist = f'$HOME/Library/LaunchAgents/{label}.plist'

    if action == "start":
        body = f"""if [[ ! -f "{plist}" ]]; then
  echo "LaunchAgent plist not found: {plist}" >&2
  exit 1
fi

if ! launchctl print "{target}" >/dev/null 2>&1; then
  launchctl bootstrap "gui/$(id -u)" "{plist}"
fi

launchctl enable "{target}" >/dev/null 2>&1 || true
exec launchctl kickstart -k "{target}"
"""
    elif action == "stop":
        body = f"""launchctl kill SIGTERM "{target}" >/dev/null 2>&1 || true
exit 0
"""
    elif action == "restart":
        body = f"""launchctl kill SIGTERM "{target}" >/dev/null 2>&1 || true
sleep 1
if [[ ! -f "{plist}" ]]; then
  echo "LaunchAgent plist not found: {plist}" >&2
  exit 1
fi
if ! launchctl print "{target}" >/dev/null 2>&1; then
  launchctl bootstrap "gui/$(id -u)" "{plist}"
fi
launchctl enable "{target}" >/dev/null 2>&1 || true
exec launchctl kickstart -k "{target}"
"""
    elif action == "status":
        body = f'exec launchctl print "{target}"\n'
    else:
        raise ValueError(f"Unsupported action: {action}")

    return f"""#!/usr/bin/env bash
set -euo pipefail

{body}"""


def render_launch_agent_plist(
    label: str,
    runner_path: Path,
    log_path: Path,
    working_directory: Path,
    schedule: list[dict[str, int]],
    run_at_load: bool,
) -> bytes:
    """Render the LaunchAgent plist bytes."""
    data = {
        "Label": label,
        "ProgramArguments": [str(runner_path)],
        "RunAtLoad": run_at_load,
        "KeepAlive": False,
        "ProcessType": "Background",
        "WorkingDirectory": str(working_directory),
        "StandardOutPath": str(log_path),
        "StandardErrorPath": str(log_path),
        "EnvironmentVariables": {
            "PATH": "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
        },
    }
    if schedule:
        data["StartCalendarInterval"] = schedule
    return plistlib.dumps(data)


def write_file(path: Path, content: str | bytes, mode: int) -> None:
    """Write text or bytes to a file and set permissions."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(content, bytes):
        path.write_bytes(content)
    else:
        path.write_text(content, encoding="utf-8")
    path.chmod(mode)


def backup_legacy_plist(path: Path) -> Path | None:
    """Move the legacy LaunchAgent aside so it no longer shadows the new service."""
    if not path.exists():
        return None
    backup_path = path.with_suffix(".plist.migrated")
    if backup_path.exists():
        backup_path.unlink()
    path.rename(backup_path)
    return backup_path


def launchctl_bootout(label: str, plist_path: Path) -> None:
    """Unload a LaunchAgent if launchd knows about it."""
    subprocess.run(
        ["launchctl", "bootout", f"gui/{os.getuid()}", str(plist_path)],
        check=False,
        capture_output=True,
        text=True,
    )
    subprocess.run(
        ["launchctl", "bootout", f"gui/{os.getuid()}/{label}"],
        check=False,
        capture_output=True,
        text=True,
    )


def launchctl_bootstrap(plist_path: Path) -> None:
    """Bootstrap the LaunchAgent into the user's launchd domain."""
    subprocess.run(
        ["launchctl", "bootstrap", f"gui/{os.getuid()}", str(plist_path)],
        check=True,
        capture_output=True,
        text=True,
    )


def install(args: argparse.Namespace) -> list[str]:
    """Perform the full machine-local secure-service installation."""
    tws_major_version = detect_tws_major_version(args)
    schedule, run_at_load = resolve_schedule(args)

    config_path = args.ibc_dir / "config.secure.ini"
    source_config = args.ibc_install_dir / "config.ini"
    ensure_secure_config(config_path, source_config)

    notes = [
        ensure_keychain_and_sanitize_config(
            config_path,
            args.keychain_account,
            args.username_service,
            args.password_service,
        )
    ]

    bin_dir = args.ibc_dir / "bin"
    log_dir = args.ibc_dir / "logs"
    runner_path = bin_dir / "run-secure-ibc-gateway.sh"
    start_path = bin_dir / "start-secure-ibc-service.sh"
    stop_path = bin_dir / "stop-secure-ibc-service.sh"
    restart_path = bin_dir / "restart-secure-ibc-service.sh"
    status_path = bin_dir / "status-secure-ibc-service.sh"
    plist_path = args.launch_agents_dir / f"{args.service_label}.plist"

    write_file(
        runner_path,
        render_runner_script(
            args.ibc_dir,
            args.ibc_install_dir,
            args.applications_dir,
            args.tws_settings_path,
            tws_major_version,
            args.keychain_account,
            args.username_service,
            args.password_service,
        ),
        0o755,
    )
    write_file(start_path, render_service_script("start", args.service_label), 0o755)
    write_file(stop_path, render_service_script("stop", args.service_label), 0o755)
    write_file(
        restart_path, render_service_script("restart", args.service_label), 0o755
    )
    write_file(status_path, render_service_script("status", args.service_label), 0o755)
    write_file(
        plist_path,
        render_launch_agent_plist(
            args.service_label,
            runner_path,
            log_dir / "ibc-gateway-service.log",
            args.ibc_dir,
            schedule,
            run_at_load,
        ),
        0o644,
    )

    for legacy_label in dict.fromkeys(args.legacy_labels):
        legacy_plist = args.launch_agents_dir / f"{legacy_label}.plist"
        if legacy_plist.exists():
            launchctl_bootout(legacy_label, legacy_plist)
            backup_path = backup_legacy_plist(legacy_plist)
            if backup_path is not None:
                notes.append(f"migrated legacy LaunchAgent to {backup_path.name}")

    launchctl_bootout(args.service_label, plist_path)
    if not args.no_bootstrap:
        launchctl_bootstrap(plist_path)
        notes.append(f"bootstrapped {args.service_label} into launchd")

    notes.extend(
        [
            f"installed runner at {runner_path}",
            f"installed service wrappers under {bin_dir}",
            f"installed LaunchAgent at {plist_path}",
            f"preserved schedule: {schedule}",
        ]
    )
    return notes


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint."""
    args = parse_args(argv)
    try:
        notes = install(args)
    except (OSError, RuntimeError, subprocess.CalledProcessError, ValueError) as exc:
        print(f"Error: {exc}")
        return 1

    for note in notes:
        print(note)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
