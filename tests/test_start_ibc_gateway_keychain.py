"""Tests for scripts/start_ibc_gateway_keychain.py."""

from __future__ import annotations

import argparse
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from scripts.start_ibc_gateway_keychain import (
    KeychainLookupError,
    build_ibc_command,
    main,
    parse_args,
    read_keychain_secret,
    render_runtime_config,
    runtime_config,
)


class TestParseArgs:
    def test_requires_tws_major_version(self):
        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(SystemExit):
                parse_args([])

    def test_uses_environment_defaults(self):
        env = {
            "IBC_TWS_MAJOR_VRSN": "10.44",
            "IBC_INSTALL_PATH": "/tmp/ibc-install",
            "IBC_TEMPLATE_PATH": "/tmp/config.secure.ini",
            "IBC_TWS_PATH": "/Applications/IB Gateway",
            "IBC_TWS_SETTINGS_PATH": "/tmp/Jts",
            "IBC_TRADING_MODE": "paper",
            "IBC_TWOFA_TIMEOUT_ACTION": "exit",
            "IBC_JAVA_PATH": "/tmp/java",
            "IBC_KEYCHAIN_USERNAME_SERVICE": "svc.user",
            "IBC_KEYCHAIN_PASSWORD_SERVICE": "svc.pass",
            "IBC_KEYCHAIN_ACCOUNT": "acct",
        }
        with patch.dict("os.environ", env, clear=True):
            args = parse_args([])

        assert args.tws_major_version == "10.44"
        assert args.ibc_path == Path("/tmp/ibc-install")
        assert args.ibc_template == Path("/tmp/config.secure.ini")
        assert args.tws_path == Path("/Applications/IB Gateway")
        assert args.tws_settings_path == Path("/tmp/Jts")
        assert args.mode == "paper"
        assert args.twofa_timeout_action == "exit"
        assert args.java_path == Path("/tmp/java")
        assert args.username_service == "svc.user"
        assert args.password_service == "svc.pass"
        assert args.keychain_account == "acct"


class TestReadKeychainSecret:
    def test_returns_secret(self):
        completed = SimpleNamespace(returncode=0, stdout="secret\n", stderr="")
        with patch("scripts.start_ibc_gateway_keychain.subprocess.run", return_value=completed) as run_mock:
            secret = read_keychain_secret("svc", "acct")

        assert secret == "secret"
        run_mock.assert_called_once()
        assert run_mock.call_args.args[0][0] == "/usr/bin/security"

    def test_raises_on_lookup_failure(self):
        completed = SimpleNamespace(returncode=44, stdout="", stderr="item not found")
        with patch("scripts.start_ibc_gateway_keychain.subprocess.run", return_value=completed):
            with pytest.raises(KeychainLookupError, match="svc: item not found"):
                read_keychain_secret("svc", "acct")

    def test_raises_on_empty_secret(self):
        completed = SimpleNamespace(returncode=0, stdout="\n", stderr="")
        with patch("scripts.start_ibc_gateway_keychain.subprocess.run", return_value=completed):
            with pytest.raises(KeychainLookupError, match="svc: empty secret"):
                read_keychain_secret("svc", "acct")


class TestRenderRuntimeConfig:
    def test_replaces_existing_credentials(self):
        template = "TradingMode=live\nIbLoginId=old\nIbPassword=oldpass\nAutoRestartTime=11:58 PM\n"
        rendered = render_runtime_config(template, "newuser", "newpass")

        assert rendered == (
            "TradingMode=live\n"
            "AutoRestartTime=11:58 PM\n"
            "\n"
            "IbLoginId=newuser\n"
            "IbPassword=newpass\n"
        )

    def test_appends_missing_credentials(self):
        rendered = render_runtime_config("TradingMode=paper\n", "paperuser", "paperpass")
        assert rendered.endswith("IbLoginId=paperuser\nIbPassword=paperpass\n")


class TestRuntimeConfig:
    def test_writes_and_removes_temp_file(self, tmp_path):
        template = tmp_path / "config.secure.ini"
        template.write_text("TradingMode=live\n", encoding="utf-8")

        with runtime_config(template, "alice", "secret") as config_path:
            assert config_path.exists()
            assert config_path.read_text(encoding="utf-8").endswith(
                "IbLoginId=alice\nIbPassword=secret\n"
            )
            assert oct(config_path.stat().st_mode & 0o777) == "0o600"

        assert not config_path.exists()


class TestBuildIbcCommand:
    def test_includes_required_flags(self, tmp_path):
        args = argparse.Namespace(
            tws_major_version="10.44",
            ibc_path=Path("/tmp/ibc-install"),
            tws_path=Path("/Applications"),
            tws_settings_path=Path("/tmp/Jts"),
            mode="live",
            twofa_timeout_action="restart",
            java_path=None,
        )

        command = build_ibc_command(args, tmp_path / "runtime.ini")

        assert command == [
            "/tmp/ibc-install/scripts/ibcstart.sh",
            "10.44",
            "--gateway",
            "--ibc-path=/tmp/ibc-install",
            f"--ibc-ini={tmp_path / 'runtime.ini'}",
            "--tws-path=/Applications",
            "--tws-settings-path=/tmp/Jts",
            "--mode=live",
            "--on2fatimeout=restart",
        ]

    def test_includes_java_path_when_present(self, tmp_path):
        args = argparse.Namespace(
            tws_major_version="10.44",
            ibc_path=Path("/tmp/ibc-install"),
            tws_path=Path("/Applications"),
            tws_settings_path=Path("/tmp/Jts"),
            mode="live",
            twofa_timeout_action="restart",
            java_path=Path("/tmp/java"),
        )

        command = build_ibc_command(args, tmp_path / "runtime.ini")

        assert command[-1] == "--java-path=/tmp/java"


class TestMain:
    def test_returns_error_off_macos(self):
        with patch("scripts.start_ibc_gateway_keychain.sys.platform", "linux"):
            assert main(["--tws-major-version", "10.44"]) == 1

    def test_returns_error_when_ibcstart_missing(self, tmp_path):
        template = tmp_path / "config.secure.ini"
        template.write_text("TradingMode=live\n", encoding="utf-8")

        with patch("scripts.start_ibc_gateway_keychain.sys.platform", "darwin"):
            result = main(
                [
                    "--tws-major-version",
                    "10.44",
                    "--ibc-path",
                    str(tmp_path / "ibc"),
                    "--ibc-template",
                    str(template),
                ]
            )

        assert result == 1

    def test_returns_error_when_template_missing(self, tmp_path):
        ibcstart = tmp_path / "ibc-install" / "scripts"
        ibcstart.mkdir(parents=True)
        (ibcstart / "ibcstart.sh").write_text("#!/bin/sh\n", encoding="utf-8")

        with patch("scripts.start_ibc_gateway_keychain.sys.platform", "darwin"):
            result = main(
                [
                    "--tws-major-version",
                    "10.44",
                    "--ibc-path",
                    str(tmp_path / "ibc-install"),
                    "--ibc-template",
                    str(tmp_path / "missing.ini"),
                ]
            )

        assert result == 1

    def test_returns_error_when_keychain_lookup_fails(self, tmp_path):
        ibcstart_dir = tmp_path / "ibc-install" / "scripts"
        ibcstart_dir.mkdir(parents=True)
        (ibcstart_dir / "ibcstart.sh").write_text("#!/bin/sh\n", encoding="utf-8")
        template = tmp_path / "config.secure.ini"
        template.write_text("TradingMode=live\n", encoding="utf-8")

        with patch("scripts.start_ibc_gateway_keychain.sys.platform", "darwin"):
            with patch(
                "scripts.start_ibc_gateway_keychain.read_keychain_secret",
                side_effect=KeychainLookupError("missing"),
            ):
                result = main(
                    [
                        "--tws-major-version",
                        "10.44",
                        "--ibc-path",
                        str(tmp_path / "ibc-install"),
                        "--ibc-template",
                        str(template),
                    ]
                )

        assert result == 1

    def test_launches_ibc_and_cleans_up_runtime_config(self, tmp_path):
        ibcstart_dir = tmp_path / "ibc-install" / "scripts"
        ibcstart_dir.mkdir(parents=True)
        (ibcstart_dir / "ibcstart.sh").write_text("#!/bin/sh\n", encoding="utf-8")
        template = tmp_path / "config.secure.ini"
        template.write_text("TradingMode=live\n", encoding="utf-8")
        launched_config = {}

        def fake_run(command, check=False, capture_output=False, text=False):
            if command[0] == "/usr/bin/security":
                secret = "ib-user\n" if command[5].endswith(".username") else "ib-pass\n"
                return SimpleNamespace(returncode=0, stdout=secret, stderr="")
            launched_config["path"] = Path(command[4].split("=", 1)[1])
            launched_config["exists_during_run"] = launched_config["path"].exists()
            launched_config["contents"] = launched_config["path"].read_text(encoding="utf-8")
            return SimpleNamespace(returncode=0)

        with patch("scripts.start_ibc_gateway_keychain.sys.platform", "darwin"):
            with patch("scripts.start_ibc_gateway_keychain.subprocess.run", side_effect=fake_run):
                result = main(
                    [
                        "--tws-major-version",
                        "10.44",
                        "--ibc-path",
                        str(tmp_path / "ibc-install"),
                        "--ibc-template",
                        str(template),
                    ]
                )

        assert result == 0
        assert launched_config["exists_during_run"] is True
        assert "IbLoginId=ib-user\nIbPassword=ib-pass\n" in launched_config["contents"]
        assert not launched_config["path"].exists()

    def test_returns_child_exit_code(self, tmp_path):
        ibcstart_dir = tmp_path / "ibc-install" / "scripts"
        ibcstart_dir.mkdir(parents=True)
        (ibcstart_dir / "ibcstart.sh").write_text("#!/bin/sh\n", encoding="utf-8")
        template = tmp_path / "config.secure.ini"
        template.write_text("TradingMode=live\n", encoding="utf-8")
        child = MagicMock(side_effect=[
            SimpleNamespace(returncode=0, stdout="user\n", stderr=""),
            SimpleNamespace(returncode=0, stdout="pass\n", stderr=""),
            SimpleNamespace(returncode=7),
        ])

        with patch("scripts.start_ibc_gateway_keychain.sys.platform", "darwin"):
            with patch("scripts.start_ibc_gateway_keychain.subprocess.run", child):
                result = main(
                    [
                        "--tws-major-version",
                        "10.44",
                        "--ibc-path",
                        str(tmp_path / "ibc-install"),
                        "--ibc-template",
                        str(template),
                    ]
                )

        assert result == 7
