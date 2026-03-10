"""Launch IB Gateway via IBC using credentials stored in the macOS Keychain."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Sequence

DEFAULT_IBC_PATH = Path.home() / "ibc-install"
DEFAULT_IBC_TEMPLATE = Path.home() / "ibc" / "config.secure.ini"
DEFAULT_TWS_PATH = Path.home() / "Applications"
DEFAULT_TWS_SETTINGS_PATH = Path.home() / "Jts"
DEFAULT_USERNAME_SERVICE = "com.market-warehouse.ibc.username"
DEFAULT_PASSWORD_SERVICE = "com.market-warehouse.ibc.password"
DEFAULT_KEYCHAIN_ACCOUNT = "ibc"
SECURITY_CLI = "/usr/bin/security"


class KeychainLookupError(RuntimeError):
    """Raised when a secret cannot be read from the macOS Keychain."""


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments and environment-backed defaults."""
    parser = argparse.ArgumentParser(
        description=(
            "Start IB Gateway via IBC using a runtime config rendered from "
            "macOS Keychain credentials."
        )
    )
    parser.add_argument(
        "--tws-major-version",
        default=os.getenv("IBC_TWS_MAJOR_VRSN"),
        help="Gateway major version, for example 10.44. Can also be set via IBC_TWS_MAJOR_VRSN.",
    )
    parser.add_argument(
        "--ibc-path",
        type=Path,
        default=Path(os.getenv("IBC_INSTALL_PATH", str(DEFAULT_IBC_PATH))).expanduser(),
        help="Path to the IBC installation root.",
    )
    parser.add_argument(
        "--ibc-template",
        type=Path,
        default=Path(os.getenv("IBC_TEMPLATE_PATH", str(DEFAULT_IBC_TEMPLATE))).expanduser(),
        help="Path to the non-secret IBC config template.",
    )
    parser.add_argument(
        "--tws-path",
        type=Path,
        default=Path(os.getenv("IBC_TWS_PATH", str(DEFAULT_TWS_PATH))).expanduser(),
        help="Path to the IB Gateway installation root.",
    )
    parser.add_argument(
        "--tws-settings-path",
        type=Path,
        default=Path(
            os.getenv("IBC_TWS_SETTINGS_PATH", str(DEFAULT_TWS_SETTINGS_PATH))
        ).expanduser(),
        help="Path to the Gateway settings folder.",
    )
    parser.add_argument(
        "--mode",
        default=os.getenv("IBC_TRADING_MODE", "live"),
        choices=("live", "paper"),
        help="Trading mode passed to IBC.",
    )
    parser.add_argument(
        "--twofa-timeout-action",
        default=os.getenv("IBC_TWOFA_TIMEOUT_ACTION", "restart"),
        choices=("restart", "exit"),
        help="Action to take if 2FA times out.",
    )
    parser.add_argument(
        "--java-path",
        type=Path,
        default=(
            Path(os.environ["IBC_JAVA_PATH"]).expanduser()
            if "IBC_JAVA_PATH" in os.environ
            else None
        ),
        help="Optional path to a Java installation to use for IBC.",
    )
    parser.add_argument(
        "--username-service",
        default=os.getenv("IBC_KEYCHAIN_USERNAME_SERVICE", DEFAULT_USERNAME_SERVICE),
        help="Keychain service name used for the IB username item.",
    )
    parser.add_argument(
        "--password-service",
        default=os.getenv("IBC_KEYCHAIN_PASSWORD_SERVICE", DEFAULT_PASSWORD_SERVICE),
        help="Keychain service name used for the IB password item.",
    )
    parser.add_argument(
        "--keychain-account",
        default=os.getenv("IBC_KEYCHAIN_ACCOUNT", DEFAULT_KEYCHAIN_ACCOUNT),
        help="Keychain account name shared by the username and password items.",
    )

    args = parser.parse_args(argv)
    if not args.tws_major_version:
        parser.error("--tws-major-version or IBC_TWS_MAJOR_VRSN is required")
    return args


def read_keychain_secret(service: str, account: str) -> str:
    """Read a generic-password secret from the macOS Keychain."""
    result = subprocess.run(
        [
            SECURITY_CLI,
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
        detail = result.stderr.strip() or "Keychain lookup failed"
        raise KeychainLookupError(f"{service}: {detail}")

    secret = result.stdout.rstrip("\n")
    if not secret:
        raise KeychainLookupError(f"{service}: empty secret")
    return secret


def render_runtime_config(template_text: str, username: str, password: str) -> str:
    """Render a runtime IBC config with fresh credentials injected."""
    rendered_lines = []
    for line in template_text.splitlines():
        stripped = line.lstrip()
        if stripped.startswith("IbLoginId=") or stripped.startswith("IbPassword="):
            continue
        rendered_lines.append(line)

    if rendered_lines and rendered_lines[-1] != "":
        rendered_lines.append("")

    rendered_lines.append(f"IbLoginId={username}")
    rendered_lines.append(f"IbPassword={password}")
    return "\n".join(rendered_lines) + "\n"


@contextmanager
def runtime_config(template_path: Path, username: str, password: str) -> Iterator[Path]:
    """Write a temporary config file and remove it after IBC exits."""
    rendered = render_runtime_config(
        template_path.read_text(encoding="utf-8"), username, password
    )
    fd, temp_path = tempfile.mkstemp(prefix="ibc-runtime-", suffix=".ini")
    try:
        os.chmod(temp_path, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(rendered)
        yield Path(temp_path)
    finally:
        Path(temp_path).unlink(missing_ok=True)


def build_ibc_command(args: argparse.Namespace, config_path: Path) -> list[str]:
    """Build the direct IBC service invocation."""
    command = [
        str(args.ibc_path / "scripts" / "ibcstart.sh"),
        args.tws_major_version,
        "--gateway",
        f"--ibc-path={args.ibc_path}",
        f"--ibc-ini={config_path}",
        f"--tws-path={args.tws_path}",
        f"--tws-settings-path={args.tws_settings_path}",
        f"--mode={args.mode}",
        f"--on2fatimeout={args.twofa_timeout_action}",
    ]
    if args.java_path is not None:
        command.append(f"--java-path={args.java_path}")
    return command


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint."""
    args = parse_args(argv)

    if sys.platform != "darwin":
        print(
            "This launcher requires macOS because it uses the Keychain security tool.",
            file=sys.stderr,
        )
        return 1

    ibcstart_path = args.ibc_path / "scripts" / "ibcstart.sh"
    if not ibcstart_path.exists():
        print(f"IBC start script not found: {ibcstart_path}", file=sys.stderr)
        return 1
    if not args.ibc_template.exists():
        print(f"IBC template config not found: {args.ibc_template}", file=sys.stderr)
        return 1

    try:
        username = read_keychain_secret(args.username_service, args.keychain_account)
        password = read_keychain_secret(args.password_service, args.keychain_account)
    except (FileNotFoundError, KeychainLookupError) as exc:
        print(f"Unable to read IB credentials from Keychain: {exc}", file=sys.stderr)
        return 1

    with runtime_config(args.ibc_template, username, password) as config_path:
        command = build_ibc_command(args, config_path)
        completed = subprocess.run(command, check=False)
        return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
