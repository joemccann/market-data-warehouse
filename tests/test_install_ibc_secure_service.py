"""Tests for scripts/install_ibc_secure_service.py."""

from __future__ import annotations

import argparse
import plistlib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import call, patch

import pytest

from scripts.install_ibc_secure_service import (
    DEFAULT_SCHEDULE,
    agent_labels_for_lookup,
    backup_legacy_plist,
    detect_tws_major_version,
    ensure_keychain_and_sanitize_config,
    ensure_secure_config,
    install,
    launchctl_bootout,
    launchctl_bootstrap,
    main,
    parse_args,
    read_keychain_secret,
    read_plist,
    read_shell_assignment,
    render_launch_agent_plist,
    render_runner_script,
    render_service_script,
    resolve_schedule,
    store_keychain_secret,
    strip_credentials_from_text,
    write_file,
)


class TestParseArgs:
    def test_defaults(self):
        with patch("scripts.install_ibc_secure_service.Path.home", return_value=Path("/tmp/home")):
            args = parse_args([])

        assert args.home == Path("/tmp/home")
        assert args.ibc_dir == Path("/tmp/home/ibc")
        assert args.ibc_install_dir == Path("/tmp/home/ibc-install")
        assert args.launch_agents_dir == Path("/tmp/home/Library/LaunchAgents")
        assert args.applications_dir == Path("/tmp/home/Applications")
        assert args.tws_settings_path == Path("/tmp/home/Jts")
        assert args.service_label == "local.ibc-gateway"
        assert args.legacy_labels == [
            "com.market-warehouse.ibc-gateway",
            "com.convex-scavenger.ibc-gateway",
        ]
        assert args.no_bootstrap is False

    def test_explicit_overrides(self, tmp_path):
        args = parse_args(
            [
                "--home",
                str(tmp_path / "home"),
                "--ibc-dir",
                str(tmp_path / "ibc"),
                "--ibc-install-dir",
                str(tmp_path / "ibc-install"),
                "--launch-agents-dir",
                str(tmp_path / "agents"),
                "--applications-dir",
                str(tmp_path / "Applications"),
                "--tws-settings-path",
                str(tmp_path / "Jts"),
                "--tws-major-version",
                "10.99",
                "--legacy-label",
                "legacy.one",
                "--legacy-label",
                "legacy.two",
                "--no-bootstrap",
            ]
        )

        assert args.ibc_dir == tmp_path / "ibc"
        assert args.ibc_install_dir == tmp_path / "ibc-install"
        assert args.launch_agents_dir == tmp_path / "agents"
        assert args.applications_dir == tmp_path / "Applications"
        assert args.tws_settings_path == tmp_path / "Jts"
        assert args.tws_major_version == "10.99"
        assert args.legacy_labels == ["legacy.one", "legacy.two"]
        assert args.no_bootstrap is True


class TestReadHelpers:
    def test_read_plist_missing(self, tmp_path):
        assert read_plist(tmp_path / "missing.plist") == {}

    def test_read_plist_present(self, tmp_path):
        path = tmp_path / "test.plist"
        path.write_bytes(plistlib.dumps({"Label": "x"}))
        assert read_plist(path) == {"Label": "x"}

    def test_read_shell_assignment(self, tmp_path):
        path = tmp_path / "gatewaystartmacos.sh"
        path.write_text("TWS_MAJOR_VRSN=10.44\nIBC_INI=~/ibc/config.ini\n", encoding="utf-8")

        assert read_shell_assignment(path, "TWS_MAJOR_VRSN") == "10.44"
        assert read_shell_assignment(path, "MISSING") is None
        assert read_shell_assignment(tmp_path / "missing.sh", "TWS_MAJOR_VRSN") is None


class TestDetectTwsVersion:
    def test_prefers_cli_arg(self, tmp_path):
        args = argparse.Namespace(
            tws_major_version="10.88",
            service_label="current",
            launch_agents_dir=tmp_path,
            legacy_labels=["legacy"],
            ibc_install_dir=tmp_path,
        )
        assert detect_tws_major_version(args) == "10.88"

    def test_uses_legacy_plist_env(self, tmp_path):
        legacy = tmp_path / "legacy.plist"
        legacy.write_bytes(
            plistlib.dumps({"EnvironmentVariables": {"TWS_MAJOR_VRSN": "10.77"}})
        )
        args = argparse.Namespace(
            tws_major_version=None,
            service_label="current",
            launch_agents_dir=tmp_path,
            legacy_labels=["legacy"],
            ibc_install_dir=tmp_path,
        )
        assert detect_tws_major_version(args) == "10.77"

    def test_uses_current_service_plist_env(self, tmp_path):
        current = tmp_path / "current.plist"
        current.write_bytes(
            plistlib.dumps({"EnvironmentVariables": {"TWS_MAJOR_VRSN": "10.78"}})
        )
        args = argparse.Namespace(
            tws_major_version=None,
            service_label="current",
            launch_agents_dir=tmp_path,
            legacy_labels=["legacy"],
            ibc_install_dir=tmp_path,
        )
        assert detect_tws_major_version(args) == "10.78"

    def test_uses_wrapper_assignment(self, tmp_path):
        wrapper = tmp_path / "gatewaystartmacos.sh"
        wrapper.write_text("TWS_MAJOR_VRSN=10.66\n", encoding="utf-8")
        args = argparse.Namespace(
            tws_major_version=None,
            service_label="current",
            launch_agents_dir=tmp_path,
            legacy_labels=["legacy"],
            ibc_install_dir=tmp_path,
        )
        assert detect_tws_major_version(args) == "10.66"

    def test_falls_back_to_default(self, tmp_path):
        args = argparse.Namespace(
            tws_major_version=None,
            service_label="current",
            launch_agents_dir=tmp_path,
            legacy_labels=["legacy"],
            ibc_install_dir=tmp_path,
        )
        assert detect_tws_major_version(args) == "10.44"


class TestResolveSchedule:
    def test_uses_legacy_schedule(self, tmp_path):
        legacy = tmp_path / "legacy.plist"
        legacy.write_bytes(
            plistlib.dumps(
                {
                    "RunAtLoad": False,
                    "StartCalendarInterval": [{"Hour": 9, "Minute": 30, "Weekday": 2}],
                }
            )
        )
        args = argparse.Namespace(
            service_label="current", launch_agents_dir=tmp_path, legacy_labels=["legacy"],
            manual_only=False,
        )
        schedule, run_at_load = resolve_schedule(args)

        assert schedule == [{"Hour": 9, "Minute": 30, "Weekday": 2}]
        assert run_at_load is False

    def test_prefers_current_schedule(self, tmp_path):
        current = tmp_path / "current.plist"
        current.write_bytes(
            plistlib.dumps(
                {
                    "RunAtLoad": True,
                    "StartCalendarInterval": [{"Hour": 8, "Minute": 5, "Weekday": 4}],
                }
            )
        )
        args = argparse.Namespace(
            service_label="current", launch_agents_dir=tmp_path, legacy_labels=["legacy"],
            manual_only=False,
        )
        schedule, run_at_load = resolve_schedule(args)
        assert schedule == [{"Hour": 8, "Minute": 5, "Weekday": 4}]
        assert run_at_load is True

    def test_falls_back_to_defaults(self, tmp_path):
        args = argparse.Namespace(
            service_label="current", launch_agents_dir=tmp_path, legacy_labels=["legacy"],
            manual_only=False,
        )
        schedule, run_at_load = resolve_schedule(args)
        assert schedule == DEFAULT_SCHEDULE
        assert run_at_load is True

    def test_manual_only_returns_empty(self, tmp_path):
        args = argparse.Namespace(
            service_label="current", launch_agents_dir=tmp_path, legacy_labels=["legacy"],
            manual_only=True,
        )
        schedule, run_at_load = resolve_schedule(args)
        assert schedule == []
        assert run_at_load is False


class TestAgentLabelsForLookup:
    def test_dedupes_labels(self):
        args = argparse.Namespace(
            service_label="local.ibc-gateway",
            legacy_labels=["local.ibc-gateway", "legacy.one", "legacy.one", "legacy.two"],
        )
        assert agent_labels_for_lookup(args) == [
            "local.ibc-gateway",
            "legacy.one",
            "legacy.two",
        ]


class TestCredentialHelpers:
    def test_strip_credentials(self):
        text = "TradingMode=live\nIbLoginId=user\nIbPassword=pass\nAutoRestartTime=11:58 PM\n"
        sanitized, username, password = strip_credentials_from_text(text)

        assert sanitized == "TradingMode=live\nAutoRestartTime=11:58 PM\n"
        assert username == "user"
        assert password == "pass"

    def test_strip_credentials_without_matches(self):
        sanitized, username, password = strip_credentials_from_text("TradingMode=live")
        assert sanitized == "TradingMode=live\n"
        assert username is None
        assert password is None

    def test_ensure_secure_config_existing(self, tmp_path):
        secure = tmp_path / "config.secure.ini"
        secure.write_text("x\n", encoding="utf-8")
        ensure_secure_config(secure, tmp_path / "config.ini")
        assert secure.read_text(encoding="utf-8") == "x\n"

    def test_ensure_secure_config_copies_source(self, tmp_path):
        secure = tmp_path / "config.secure.ini"
        source = tmp_path / "config.ini"
        source.write_text("y\n", encoding="utf-8")
        ensure_secure_config(secure, source)
        assert secure.read_text(encoding="utf-8") == "y\n"

    def test_ensure_secure_config_raises_without_source(self, tmp_path):
        with pytest.raises(RuntimeError, match="IBC source config not found"):
            ensure_secure_config(tmp_path / "config.secure.ini", tmp_path / "config.ini")

    def test_read_keychain_secret_found(self):
        completed = SimpleNamespace(returncode=0, stdout="secret\n", stderr="")
        with patch("scripts.install_ibc_secure_service.subprocess.run", return_value=completed):
            assert read_keychain_secret("svc", "acct") == "secret"

    def test_read_keychain_secret_missing(self):
        completed = SimpleNamespace(returncode=44, stdout="", stderr="missing")
        with patch("scripts.install_ibc_secure_service.subprocess.run", return_value=completed):
            assert read_keychain_secret("svc", "acct") is None

    def test_store_keychain_secret(self):
        with patch("scripts.install_ibc_secure_service.subprocess.run") as run_mock:
            store_keychain_secret("svc", "acct", "secret")

        run_mock.assert_called_once_with(
            [
                "security",
                "add-generic-password",
                "-U",
                "-a",
                "acct",
                "-s",
                "svc",
                "-w",
                "secret",
                "-T",
                "/usr/bin/security",
            ],
            check=True,
            capture_output=True,
            text=True,
        )

    def test_ensure_keychain_and_sanitize_migrates_from_config(self, tmp_path):
        config = tmp_path / "config.secure.ini"
        config.write_text("IbLoginId=user\nIbPassword=pass\nTradingMode=live\n", encoding="utf-8")

        with patch(
            "scripts.install_ibc_secure_service.read_keychain_secret",
            side_effect=[None, None],
        ):
            with patch("scripts.install_ibc_secure_service.store_keychain_secret") as store_mock:
                status = ensure_keychain_and_sanitize_config(config, "acct", "svc.user", "svc.pass")

        assert status == "migrated credentials from config.secure.ini into Keychain"
        assert config.read_text(encoding="utf-8") == "TradingMode=live\n"
        store_mock.assert_has_calls(
            [call("svc.user", "acct", "user"), call("svc.pass", "acct", "pass")]
        )

    def test_ensure_keychain_and_sanitize_refreshes_existing_keychain(self, tmp_path):
        config = tmp_path / "config.secure.ini"
        config.write_text("TradingMode=live\n", encoding="utf-8")

        with patch(
            "scripts.install_ibc_secure_service.read_keychain_secret",
            side_effect=["user", "pass"],
        ):
            with patch("scripts.install_ibc_secure_service.store_keychain_secret") as store_mock:
                status = ensure_keychain_and_sanitize_config(config, "acct", "svc.user", "svc.pass")

        assert status == "refreshed Keychain trusted-app access for existing credentials"
        store_mock.assert_has_calls(
            [call("svc.user", "acct", "user"), call("svc.pass", "acct", "pass")]
        )

    def test_ensure_keychain_and_sanitize_refreshes_and_strips(self, tmp_path):
        config = tmp_path / "config.secure.ini"
        config.write_text("TradingMode=live\nIbLoginId=file-user\nIbPassword=file-pass\n", encoding="utf-8")

        with patch(
            "scripts.install_ibc_secure_service.read_keychain_secret",
            side_effect=["key-user", "key-pass"],
        ):
            with patch("scripts.install_ibc_secure_service.store_keychain_secret"):
                status = ensure_keychain_and_sanitize_config(config, "acct", "svc.user", "svc.pass")

        assert (
            status
            == "refreshed Keychain trusted-app access for existing credentials and stripped plaintext credentials from config.secure.ini"
        )
        assert config.read_text(encoding="utf-8") == "TradingMode=live\n"

    def test_ensure_keychain_and_sanitize_raises_when_missing_everywhere(self, tmp_path):
        config = tmp_path / "config.secure.ini"
        config.write_text("TradingMode=live\n", encoding="utf-8")

        with patch(
            "scripts.install_ibc_secure_service.read_keychain_secret",
            side_effect=[None, None],
        ):
            with pytest.raises(RuntimeError, match="IB credentials were not found in Keychain"):
                ensure_keychain_and_sanitize_config(config, "acct", "svc.user", "svc.pass")


class TestRenderers:
    def test_render_runner_script(self):
        rendered = render_runner_script(
            Path("/Users/test/ibc"),
            Path("/Users/test/ibc-install"),
            Path("/Users/test/Applications"),
            Path("/Users/test/Jts"),
            "10.44",
            "acct",
            "svc.user",
            "svc.pass",
        )

        assert 'CONFIG_PATH="${IBC_CONFIG_PATH:-/Users/test/ibc/config.secure.ini}"' in rendered
        assert '"$IBC_INSTALL_DIR/scripts/ibcstart.sh" "$TWS_MAJOR_VRSN" --gateway' in rendered
        assert 'security find-generic-password -w -a "$KEYCHAIN_ACCOUNT" -s "$USERNAME_SERVICE"' in rendered
        assert 'mktemp "${RUNTIME_DIR%/}/config.runtime.XXXXXX"' in rendered

    def test_render_service_scripts(self):
        start_script = render_service_script("start", "com.example.ibc")
        stop_script = render_service_script("stop", "com.example.ibc")
        restart_script = render_service_script("restart", "com.example.ibc")
        status_script = render_service_script("status", "com.example.ibc")

        assert 'launchctl kickstart -k "gui/$(id -u)/com.example.ibc"' in start_script
        assert 'launchctl kill SIGTERM "gui/$(id -u)/com.example.ibc"' in stop_script
        assert 'sleep 1' in restart_script
        assert 'launchctl print "gui/$(id -u)/com.example.ibc"' in status_script

        with pytest.raises(ValueError, match="Unsupported action"):
            render_service_script("bogus", "com.example.ibc")

    def test_render_launch_agent_plist(self, tmp_path):
        plist_bytes = render_launch_agent_plist(
            "com.example.ibc",
            tmp_path / "run.sh",
            tmp_path / "ibc.log",
            tmp_path,
            [{"Hour": 1, "Minute": 2, "Weekday": 3}],
            False,
        )
        data = plistlib.loads(plist_bytes)

        assert data["Label"] == "com.example.ibc"
        assert data["ProgramArguments"] == [str(tmp_path / "run.sh")]
        assert data["StandardOutPath"] == str(tmp_path / "ibc.log")
        assert data["StandardErrorPath"] == str(tmp_path / "ibc.log")
        assert data["StartCalendarInterval"] == [{"Hour": 1, "Minute": 2, "Weekday": 3}]
        assert data["RunAtLoad"] is False


class TestFileAndLaunchctlHelpers:
    def test_write_file_text(self, tmp_path):
        path = tmp_path / "a" / "file.txt"
        write_file(path, "hello\n", 0o600)
        assert path.read_text(encoding="utf-8") == "hello\n"
        assert oct(path.stat().st_mode & 0o777) == "0o600"

    def test_write_file_bytes(self, tmp_path):
        path = tmp_path / "a" / "file.bin"
        write_file(path, b"x", 0o644)
        assert path.read_bytes() == b"x"

    def test_backup_legacy_plist(self, tmp_path):
        legacy = tmp_path / "legacy.plist"
        legacy.write_text("x", encoding="utf-8")
        backup = backup_legacy_plist(legacy)
        assert backup == tmp_path / "legacy.plist.migrated"
        assert backup.read_text(encoding="utf-8") == "x"
        assert not legacy.exists()

    def test_backup_legacy_plist_replaces_existing_backup(self, tmp_path):
        legacy = tmp_path / "legacy.plist"
        backup = tmp_path / "legacy.plist.migrated"
        legacy.write_text("new", encoding="utf-8")
        backup.write_text("old", encoding="utf-8")
        result = backup_legacy_plist(legacy)
        assert result == backup
        assert backup.read_text(encoding="utf-8") == "new"

    def test_backup_legacy_plist_missing(self, tmp_path):
        assert backup_legacy_plist(tmp_path / "missing.plist") is None

    def test_launchctl_bootout(self, tmp_path):
        with patch("scripts.install_ibc_secure_service.subprocess.run") as run_mock:
            launchctl_bootout("com.example.ibc", tmp_path / "agent.plist")

        assert run_mock.call_count == 2
        first, second = run_mock.call_args_list
        assert first.args[0][:2] == ["launchctl", "bootout"]
        assert second.args[0] == ["launchctl", "bootout", "gui/501/com.example.ibc"]

    def test_launchctl_bootstrap(self, tmp_path):
        with patch("scripts.install_ibc_secure_service.subprocess.run") as run_mock:
            launchctl_bootstrap(tmp_path / "agent.plist")

        run_mock.assert_called_once_with(
            ["launchctl", "bootstrap", "gui/501", str(tmp_path / "agent.plist")],
            check=True,
            capture_output=True,
            text=True,
        )


class TestInstall:
    def test_install_writes_files_and_bootstraps(self, tmp_path):
        home = tmp_path / "home"
        ibc_dir = home / "ibc"
        install_dir = home / "ibc-install"
        agents_dir = home / "Library" / "LaunchAgents"
        install_dir.mkdir(parents=True)
        agents_dir.mkdir(parents=True)
        (install_dir / "config.ini").write_text("TradingMode=live\n", encoding="utf-8")
        (install_dir / "gatewaystartmacos.sh").write_text("TWS_MAJOR_VRSN=10.55\n", encoding="utf-8")
        legacy = agents_dir / "com.convex-scavenger.ibc-gateway.plist"
        legacy.write_bytes(
            plistlib.dumps(
                {
                    "RunAtLoad": True,
                    "StartCalendarInterval": [{"Hour": 7, "Minute": 15, "Weekday": 1}],
                }
            )
        )
        args = argparse.Namespace(
            home=home,
            ibc_dir=ibc_dir,
            ibc_install_dir=install_dir,
            launch_agents_dir=agents_dir,
            applications_dir=home / "Applications",
            tws_settings_path=home / "Jts",
            service_label="local.ibc-gateway",
            legacy_labels=[
                "com.market-warehouse.ibc-gateway",
                "com.convex-scavenger.ibc-gateway",
            ],
            keychain_account="ibc",
            username_service="svc.user",
            password_service="svc.pass",
            tws_major_version=None,
            no_bootstrap=False,
            manual_only=False,
        )

        with patch(
            "scripts.install_ibc_secure_service.ensure_keychain_and_sanitize_config",
            return_value="sanitized config",
        ) as sanitize_mock:
            with patch("scripts.install_ibc_secure_service.launchctl_bootout") as bootout_mock:
                with patch("scripts.install_ibc_secure_service.launchctl_bootstrap") as bootstrap_mock:
                    notes = install(args)

        sanitize_mock.assert_called_once_with(
            ibc_dir / "config.secure.ini", "ibc", "svc.user", "svc.pass"
        )
        bootout_mock.assert_has_calls(
            [
                call("com.convex-scavenger.ibc-gateway", legacy),
                call(
                    "local.ibc-gateway",
                    agents_dir / "local.ibc-gateway.plist",
                ),
            ]
        )
        bootstrap_mock.assert_called_once_with(
            agents_dir / "local.ibc-gateway.plist"
        )
        assert (ibc_dir / "bin" / "run-secure-ibc-gateway.sh").exists()
        assert (ibc_dir / "bin" / "start-secure-ibc-service.sh").exists()
        assert (agents_dir / "local.ibc-gateway.plist").exists()
        assert (agents_dir / "com.convex-scavenger.ibc-gateway.plist.migrated").exists()
        assert "sanitized config" in notes
        assert "bootstrapped local.ibc-gateway into launchd" in notes
        assert "preserved schedule: [{'Hour': 7, 'Minute': 15, 'Weekday': 1}]" in notes

    def test_install_without_bootstrap(self, tmp_path):
        home = tmp_path / "home"
        ibc_dir = home / "ibc"
        install_dir = home / "ibc-install"
        agents_dir = home / "Library" / "LaunchAgents"
        install_dir.mkdir(parents=True)
        agents_dir.mkdir(parents=True)
        (install_dir / "config.ini").write_text("TradingMode=live\n", encoding="utf-8")
        args = argparse.Namespace(
            home=home,
            ibc_dir=ibc_dir,
            ibc_install_dir=install_dir,
            launch_agents_dir=agents_dir,
            applications_dir=home / "Applications",
            tws_settings_path=home / "Jts",
            service_label="local.ibc-gateway",
            legacy_labels=["missing"],
            keychain_account="ibc",
            username_service="svc.user",
            password_service="svc.pass",
            tws_major_version="10.44",
            no_bootstrap=True,
            manual_only=False,
        )

        with patch(
            "scripts.install_ibc_secure_service.ensure_keychain_and_sanitize_config",
            return_value="sanitized config",
        ):
            with patch("scripts.install_ibc_secure_service.launchctl_bootout") as bootout_mock:
                with patch("scripts.install_ibc_secure_service.launchctl_bootstrap") as bootstrap_mock:
                    notes = install(args)

        bootout_mock.assert_called_once()
        bootstrap_mock.assert_not_called()
        assert all("bootstrapped" not in note for note in notes)


class TestMain:
    def test_main_success(self):
        with patch("scripts.install_ibc_secure_service.install", return_value=["a", "b"]):
            assert main([]) == 0

    def test_main_failure(self):
        with patch("scripts.install_ibc_secure_service.install", side_effect=RuntimeError("boom")):
            assert main([]) == 1
