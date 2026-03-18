"""Tests for scripts/run_daily_update_job.py."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from scripts.run_daily_update_job import (
    ASSET_CLASSES,
    _utc_now,
    AlertRequest,
    RunnerConfig,
    append_log,
    build_alert_command,
    build_cboe_volatility_command,
    build_config,
    build_daily_update_command,
    build_log_file,
    extract_error_summary,
    log_has_completion_marker,
    main,
    node_binary_exists,
    run_cboe_volatility_sync,
    run_daily_update_attempt,
    run_with_retries,
    send_failure_alert,
)


def _config(tmp_path: Path, *, node_bin: str = "/opt/homebrew/bin/node") -> RunnerConfig:
    repo_root = tmp_path / "repo"
    script_dir = repo_root / "scripts"
    return RunnerConfig(
        warehouse_dir=tmp_path / "warehouse",
        log_dir=tmp_path / "warehouse" / "logs",
        daily_update_script=script_dir / "daily_update.py",
        alert_script=script_dir / "send_daily_update_failure_email.mjs",
        python_bin="/usr/bin/python3",
        node_bin=node_bin,
        max_attempts=3,
        retry_delay_seconds=300,
    )


class TestBuildConfig:
    def test_defaults(self, monkeypatch, tmp_path):
        monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path / "warehouse"))
        monkeypatch.delenv("MDW_DAILY_UPDATE_LOG_DIR", raising=False)
        monkeypatch.delenv("MDW_DAILY_UPDATE_SCRIPT", raising=False)
        monkeypatch.delenv("MDW_DAILY_UPDATE_ALERT_SCRIPT", raising=False)
        monkeypatch.delenv("MDW_DAILY_UPDATE_PYTHON_BIN", raising=False)
        monkeypatch.delenv("MDW_NODE_BIN", raising=False)
        monkeypatch.delenv("MDW_DAILY_UPDATE_MAX_ATTEMPTS", raising=False)
        monkeypatch.delenv("MDW_DAILY_UPDATE_RETRY_DELAY_SECONDS", raising=False)

        with patch(
            "scripts.run_daily_update_job.shutil.which",
            return_value="/usr/local/bin/node",
        ):
            config = build_config()

        assert config.warehouse_dir == tmp_path / "warehouse"
        assert config.log_dir == config.warehouse_dir / "logs"
        assert config.node_bin == "/usr/local/bin/node"
        assert config.max_attempts == 3
        assert config.retry_delay_seconds == 300
        assert config.python_bin == os.sys.executable

    def test_env_overrides(self, monkeypatch, tmp_path):
        monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path / "warehouse"))
        monkeypatch.setenv("MDW_DAILY_UPDATE_LOG_DIR", str(tmp_path / "custom-logs"))
        monkeypatch.setenv("MDW_DAILY_UPDATE_SCRIPT", str(tmp_path / "daily.py"))
        monkeypatch.setenv("MDW_DAILY_UPDATE_ALERT_SCRIPT", str(tmp_path / "alert.mjs"))
        monkeypatch.setenv("MDW_DAILY_UPDATE_PYTHON_BIN", "/venv/bin/python")
        monkeypatch.setenv("MDW_NODE_BIN", "/custom/node")
        monkeypatch.setenv("MDW_DAILY_UPDATE_MAX_ATTEMPTS", "4")
        monkeypatch.setenv("MDW_DAILY_UPDATE_RETRY_DELAY_SECONDS", "9")

        config = build_config()

        assert config.warehouse_dir == tmp_path / "warehouse"
        assert config.log_dir == tmp_path / "custom-logs"
        assert config.daily_update_script == tmp_path / "daily.py"
        assert config.alert_script == tmp_path / "alert.mjs"
        assert config.python_bin == "/venv/bin/python"
        assert config.node_bin == "/custom/node"
        assert config.max_attempts == 4
        assert config.retry_delay_seconds == 9

    def test_invalid_positive_int_env(self, monkeypatch):
        monkeypatch.setenv("MDW_DAILY_UPDATE_MAX_ATTEMPTS", "0")

        with pytest.raises(ValueError, match="must be >= 1"):
            build_config()


class TestHelpers:
    def test_utc_now_returns_utc_datetime(self):
        assert _utc_now().tzinfo == timezone.utc

    def test_build_log_file(self, tmp_path):
        current = datetime(2026, 3, 11, 13, 5, tzinfo=timezone.utc)
        assert (
            build_log_file(tmp_path, current)
            == tmp_path / "daily_update_2026-03-11.log"
        )

    def test_append_log_adds_newline(self, tmp_path):
        log_file = tmp_path / "logs" / "daily.log"
        append_log(log_file, "line one")
        append_log(log_file, "line two\n")

        assert log_file.read_text(encoding="utf-8") == "line one\nline two\n"

    def test_build_commands_with_and_without_optional_alert_fields(self, tmp_path):
        config = _config(tmp_path)

        assert build_daily_update_command(config, ["--force"]) == [
            "/usr/bin/python3",
            str(config.daily_update_script),
            "--force",
        ]

        full_request = AlertRequest(
            run_date="2026-03-11",
            log_file=tmp_path / "daily.log",
            attempts=3,
            exit_code=9,
            error_summary="boom",
            repo_root=tmp_path / "repo",
        )
        full_command = build_alert_command(config, full_request)
        assert full_command[:2] == [
            "/opt/homebrew/bin/node",
            str(config.alert_script),
        ]
        assert "--attempts" in full_command
        assert "--exit-code" in full_command

        watchdog_request = AlertRequest(
            run_date="2026-03-11",
            log_file=tmp_path / "daily.log",
            attempts=None,
            exit_code=None,
            error_summary="missing log",
            repo_root=tmp_path / "repo",
        )
        watchdog_command = build_alert_command(config, watchdog_request)
        assert "--attempts" not in watchdog_command
        assert "--exit-code" not in watchdog_command

    def test_extract_error_summary_handles_missing_and_empty_logs(self, tmp_path):
        missing_log = tmp_path / "missing.log"
        assert (
            extract_error_summary(missing_log)
            == "Daily update failed, and the log file was not found."
        )

        empty_log = tmp_path / "empty.log"
        empty_log.write_text(
            "=== Daily Update 2026-03-11T20:05:07Z ===\n=== Failed 2026-03-11T20:05:10Z ===\n",
            encoding="utf-8",
        )
        assert (
            extract_error_summary(empty_log)
            == "Daily update failed with no error summary captured in the log."
        )

    def test_extract_error_summary_and_completion_marker(self, tmp_path):
        log_file = tmp_path / "daily.log"
        log_file.write_text(
            "\n".join(
                [
                    "=== Daily Update 2026-03-11T20:05:07Z ===",
                    "Traceback: boom",
                    "=== Done 2026-03-11T20:05:08Z (attempt 1/3) ===",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        assert extract_error_summary(log_file) == "Traceback: boom"
        assert log_has_completion_marker(log_file) is True
        assert log_has_completion_marker(tmp_path / "nope.log") is False

    def test_node_binary_exists(self):
        with patch("scripts.run_daily_update_job.Path.exists", return_value=True):
            assert node_binary_exists("/opt/homebrew/bin/node") is True

        with patch("scripts.run_daily_update_job.Path.exists", return_value=False):
            assert node_binary_exists("/opt/homebrew/bin/node") is False

        with patch(
            "scripts.run_daily_update_job.shutil.which",
            return_value="/usr/local/bin/node",
        ):
            assert node_binary_exists("node") is True

        with patch("scripts.run_daily_update_job.shutil.which", return_value=None):
            assert node_binary_exists("node") is False


class TestSubprocessPaths:
    def test_run_daily_update_attempt(self, tmp_path):
        log_file = tmp_path / "daily.log"

        def _runner(command, stdout, stderr, text, env, check):
            assert command[-1] == "--dry-run"
            stdout.write("hello from sync\n")
            return SimpleNamespace(returncode=0)

        result = run_daily_update_attempt(
            ["/usr/bin/python3", "/repo/scripts/daily_update.py", "--dry-run"],
            log_file,
            env={"X": "1"},
            runner=_runner,
        )

        assert result.returncode == 0
        assert "hello from sync" in log_file.read_text(encoding="utf-8")

    def test_send_failure_alert_skips_when_node_missing(self, tmp_path):
        config = _config(tmp_path, node_bin="/missing/node")
        request = AlertRequest(
            run_date="2026-03-11",
            log_file=tmp_path / "daily.log",
            attempts=3,
            exit_code=5,
            error_summary="sync failed",
            repo_root=tmp_path / "repo",
        )
        config.alert_script.parent.mkdir(parents=True, exist_ok=True)
        config.alert_script.write_text("console.log('x')\n", encoding="utf-8")

        with patch("scripts.run_daily_update_job.node_binary_exists", return_value=False):
            result = send_failure_alert(config, request, request.log_file, env={})

        assert result is None
        assert "node binary not found" in request.log_file.read_text(encoding="utf-8")

    def test_send_failure_alert_skips_when_script_missing(self, tmp_path):
        config = _config(tmp_path)
        request = AlertRequest(
            run_date="2026-03-11",
            log_file=tmp_path / "daily.log",
            attempts=3,
            exit_code=5,
            error_summary="sync failed",
            repo_root=tmp_path / "repo",
        )

        with patch("scripts.run_daily_update_job.node_binary_exists", return_value=True):
            result = send_failure_alert(config, request, request.log_file, env={})

        assert result is None
        assert "alert script not found" in request.log_file.read_text(encoding="utf-8")

    def test_send_failure_alert_invokes_runner(self, tmp_path):
        config = _config(tmp_path)
        request = AlertRequest(
            run_date="2026-03-11",
            log_file=tmp_path / "daily.log",
            attempts=None,
            exit_code=None,
            error_summary="sync failed",
            repo_root=tmp_path / "repo",
        )
        config.alert_script.parent.mkdir(parents=True, exist_ok=True)
        config.alert_script.write_text("console.log('x')\n", encoding="utf-8")

        def _runner(command, stdout, stderr, text, env, check):
            assert command[0] == "/opt/homebrew/bin/node"
            assert "--error-summary" in command
            assert "--attempts" not in command
            return SimpleNamespace(returncode=0, stdout="sent")

        with patch("scripts.run_daily_update_job.node_binary_exists", return_value=True):
            result = send_failure_alert(
                config,
                request,
                request.log_file,
                env={"A": "1"},
                runner=_runner,
            )

        assert result.returncode == 0
        assert "Triggering failure alert via:" in request.log_file.read_text(
            encoding="utf-8"
        )


class TestRunWithRetries:
    def test_success_first_attempt(self, tmp_path):
        config = _config(tmp_path)
        timestamps = iter(
            [
                datetime(2026, 3, 11, 20, 5, 7, tzinfo=timezone.utc),
                datetime(2026, 3, 11, 20, 5, 8, tzinfo=timezone.utc),
                datetime(2026, 3, 11, 20, 5, 9, tzinfo=timezone.utc),
            ]
        )

        def _runner(command, stdout, stderr, text, env, check):
            stdout.write("sync ok\n")
            return SimpleNamespace(returncode=0, stdout="")

        with patch("scripts.run_daily_update_job.socket.gethostname", return_value="warehouse.local"):
            rc = run_with_retries(
                config,
                ["--dry-run"],
                env={"A": "1"},
                runner=_runner,
                now_fn=lambda: next(timestamps),
            )

        assert rc == 0
        log_text = (config.log_dir / "daily_update_2026-03-11.log").read_text(
            encoding="utf-8"
        )
        assert "Runner config: attempts=3 retry_delay_seconds=300 hostname=warehouse.local" in log_text
        assert "=== Done 2026-03-11T20:05:09Z (attempt 1/3) ===" in log_text

    def test_retry_then_success(self, tmp_path):
        config = RunnerConfig(
            **(_config(tmp_path).__dict__ | {"max_attempts": 2, "retry_delay_seconds": 7})
        )
        timestamps = iter(
            [
                datetime(2026, 3, 11, 20, 5, 7, tzinfo=timezone.utc),
                datetime(2026, 3, 11, 20, 5, 8, tzinfo=timezone.utc),
                datetime(2026, 3, 11, 20, 5, 9, tzinfo=timezone.utc),
                datetime(2026, 3, 11, 20, 5, 10, tzinfo=timezone.utc),
                datetime(2026, 3, 11, 20, 5, 11, tzinfo=timezone.utc),
            ]
        )
        results = iter(
            [
                SimpleNamespace(returncode=9, stdout=""),
                SimpleNamespace(returncode=0, stdout=""),
            ]
        )

        def _runner(command, stdout, stderr, text, env, check):
            stdout.write("attempt output\n")
            return next(results)

        sleep_calls: list[int] = []

        with patch("scripts.run_daily_update_job.socket.gethostname", return_value="warehouse.local"):
            rc = run_with_retries(
                config,
                [],
                env={},
                runner=_runner,
                sleep_fn=sleep_calls.append,
                now_fn=lambda: next(timestamps),
            )

        assert rc == 0
        assert sleep_calls == [7]
        log_text = (config.log_dir / "daily_update_2026-03-11.log").read_text(
            encoding="utf-8"
        )
        assert "Retrying in 7 seconds..." in log_text
        assert "attempt 2/2" in log_text

    def test_terminal_failure_sends_alert(self, tmp_path):
        config = RunnerConfig(
            **(_config(tmp_path).__dict__ | {"max_attempts": 2, "retry_delay_seconds": 5})
        )
        timestamps = iter(
            [
                datetime(2026, 3, 11, 20, 5, 7, tzinfo=timezone.utc),
                datetime(2026, 3, 11, 20, 5, 8, tzinfo=timezone.utc),
                datetime(2026, 3, 11, 20, 5, 9, tzinfo=timezone.utc),
                datetime(2026, 3, 11, 20, 5, 10, tzinfo=timezone.utc),
                datetime(2026, 3, 11, 20, 5, 11, tzinfo=timezone.utc),
                datetime(2026, 3, 11, 20, 5, 12, tzinfo=timezone.utc),
            ]
        )
        results = iter(
            [
                SimpleNamespace(returncode=4, stdout=""),
                SimpleNamespace(returncode=4, stdout=""),
                SimpleNamespace(returncode=0, stdout="alert sent"),
            ]
        )

        def _runner(command, stdout=None, stderr=None, text=None, env=None, check=None):
            if hasattr(stdout, "write"):
                stdout.write("sync failed\n")
            return next(results)

        config.alert_script.parent.mkdir(parents=True, exist_ok=True)
        config.alert_script.write_text("console.log('send');\n", encoding="utf-8")
        sleep_calls: list[int] = []

        with patch("scripts.run_daily_update_job.node_binary_exists", return_value=True):
            rc = run_with_retries(
                config,
                [],
                env={},
                runner=_runner,
                sleep_fn=sleep_calls.append,
                now_fn=lambda: next(timestamps),
            )

        assert rc == 4
        assert sleep_calls == [5]
        log_text = (config.log_dir / "daily_update_2026-03-11.log").read_text(
            encoding="utf-8"
        )
        assert "Failure alert sent successfully. alert sent" in log_text
        assert "=== Failed 2026-03-11T20:05:12Z after 2 attempt(s) ===" in log_text

    def test_terminal_failure_without_alert_result(self, tmp_path):
        config = RunnerConfig(**(_config(tmp_path, node_bin="/missing/node").__dict__ | {"max_attempts": 1}))
        timestamps = iter(
            [
                datetime(2026, 3, 11, 20, 5, 7, tzinfo=timezone.utc),
                datetime(2026, 3, 11, 20, 5, 8, tzinfo=timezone.utc),
                datetime(2026, 3, 11, 20, 5, 9, tzinfo=timezone.utc),
                datetime(2026, 3, 11, 20, 5, 10, tzinfo=timezone.utc),
            ]
        )

        def _runner(command, stdout=None, stderr=None, text=None, env=None, check=None):
            stdout.write("sync failed\n")
            return SimpleNamespace(returncode=6, stdout="")

        with patch("scripts.run_daily_update_job.node_binary_exists", return_value=False):
            rc = run_with_retries(
                config,
                [],
                env={},
                runner=_runner,
                now_fn=lambda: next(timestamps),
            )

        assert rc == 6
        log_text = (config.log_dir / "daily_update_2026-03-11.log").read_text(
            encoding="utf-8"
        )
        assert "skipping failure email" in log_text

    def test_terminal_failure_alert_non_zero(self, tmp_path):
        config = RunnerConfig(**(_config(tmp_path).__dict__ | {"max_attempts": 1}))
        timestamps = iter(
            [
                datetime(2026, 3, 11, 20, 5, 7, tzinfo=timezone.utc),
                datetime(2026, 3, 11, 20, 5, 8, tzinfo=timezone.utc),
                datetime(2026, 3, 11, 20, 5, 9, tzinfo=timezone.utc),
                datetime(2026, 3, 11, 20, 5, 10, tzinfo=timezone.utc),
            ]
        )
        results = iter(
            [
                SimpleNamespace(returncode=3, stdout=""),
                SimpleNamespace(returncode=2, stdout="smtp down"),
            ]
        )

        def _runner(command, stdout=None, stderr=None, text=None, env=None, check=None):
            if hasattr(stdout, "write"):
                stdout.write("sync failed\n")
            return next(results)

        config.alert_script.parent.mkdir(parents=True, exist_ok=True)
        config.alert_script.write_text("console.log('send');\n", encoding="utf-8")

        with patch("scripts.run_daily_update_job.node_binary_exists", return_value=True):
            rc = run_with_retries(
                config,
                [],
                env={},
                runner=_runner,
                now_fn=lambda: next(timestamps),
            )

        assert rc == 3
        log_text = (config.log_dir / "daily_update_2026-03-11.log").read_text(
            encoding="utf-8"
        )
        assert (
            "WARNING: failure alert returned non-zero exit code 2. smtp down"
            in log_text
        )


class TestCboeVolatilitySync:
    def test_build_cboe_volatility_command(self, tmp_path):
        config = _config(tmp_path)
        command = build_cboe_volatility_command(config)
        assert command[0] == "/usr/bin/python3"
        assert "fetch_cboe_volatility.py" in command[1]
        assert len(command) == 2  # No extra args, uses preset by default

    def test_run_cboe_volatility_sync_success(self, tmp_path):
        config = _config(tmp_path)
        timestamps = iter(
            [
                datetime(2026, 3, 18, 20, 5, 7, tzinfo=timezone.utc),
                datetime(2026, 3, 18, 20, 5, 8, tzinfo=timezone.utc),
            ]
        )

        def _runner(command, stdout, stderr, text, env, check):
            stdout.write("CBOE fetch ok\n")
            return SimpleNamespace(returncode=0)

        rc = run_cboe_volatility_sync(
            config,
            env={},
            runner=_runner,
            now_fn=lambda: next(timestamps),
        )

        assert rc == 0
        log_text = (config.log_dir / "daily_update_2026-03-18.log").read_text(encoding="utf-8")
        assert "CBOE Volatility Sync" in log_text
        assert "CBOE Volatility Sync Done" in log_text

    def test_run_cboe_volatility_sync_failure(self, tmp_path):
        config = _config(tmp_path)
        timestamps = iter(
            [
                datetime(2026, 3, 18, 20, 5, 7, tzinfo=timezone.utc),
                datetime(2026, 3, 18, 20, 5, 8, tzinfo=timezone.utc),
            ]
        )

        def _runner(command, stdout, stderr, text, env, check):
            stdout.write("CBOE fetch failed\n")
            return SimpleNamespace(returncode=1)

        rc = run_cboe_volatility_sync(
            config,
            env={},
            runner=_runner,
            now_fn=lambda: next(timestamps),
        )

        assert rc == 1
        log_text = (config.log_dir / "daily_update_2026-03-18.log").read_text(encoding="utf-8")
        assert "CBOE Volatility Sync Failed" in log_text


class TestMain:
    def test_main_runs_all_asset_classes_and_cboe_by_default(self):
        config = _config(Path("/tmp/test"))
        ib_calls: list[list[str]] = []
        cboe_called = []

        def _run_ib(cfg, args, env):
            ib_calls.append(args)
            return 0

        def _run_cboe(cfg, env, **kwargs):
            cboe_called.append(True)
            return 0

        with patch("scripts.run_daily_update_job.build_config", return_value=config):
            with patch("scripts.run_daily_update_job.run_with_retries", side_effect=_run_ib):
                with patch("scripts.run_daily_update_job.run_cboe_volatility_sync", side_effect=_run_cboe):
                    assert main(["--dry-run"]) == 0

        # IB syncs equity and futures; volatility via CBOE
        assert ib_calls == [
            ["--dry-run", "--asset-class", ac] for ac in ASSET_CLASSES
        ]
        assert cboe_called == [True]

    def test_main_explicit_asset_class_skips_cboe(self):
        config = _config(Path("/tmp/test"))

        with patch("scripts.run_daily_update_job.build_config", return_value=config):
            with patch("scripts.run_daily_update_job.run_with_retries", return_value=0) as run_mock:
                with patch("scripts.run_daily_update_job.run_cboe_volatility_sync") as cboe_mock:
                    assert main(["--dry-run", "--asset-class", "equity"]) == 0

        run_mock.assert_called_once_with(
            config, ["--dry-run", "--asset-class", "equity"], env=os.environ.copy()
        )
        cboe_mock.assert_not_called()

    def test_main_returns_nonzero_if_any_asset_class_fails(self):
        config = _config(Path("/tmp/test"))

        def _run(cfg, args, env):
            if "--asset-class" in args and args[args.index("--asset-class") + 1] == "futures":
                return 1
            return 0

        with patch("scripts.run_daily_update_job.build_config", return_value=config):
            with patch("scripts.run_daily_update_job.run_with_retries", side_effect=_run):
                with patch("scripts.run_daily_update_job.run_cboe_volatility_sync", return_value=0):
                    assert main([]) == 1

    def test_main_returns_nonzero_if_cboe_fails(self):
        config = _config(Path("/tmp/test"))

        with patch("scripts.run_daily_update_job.build_config", return_value=config):
            with patch("scripts.run_daily_update_job.run_with_retries", return_value=0):
                with patch("scripts.run_daily_update_job.run_cboe_volatility_sync", return_value=1):
                    assert main([]) == 1
