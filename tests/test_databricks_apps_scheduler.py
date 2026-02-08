from __future__ import annotations

import io
import importlib.util
import json
import os
import sys
import types
from contextlib import redirect_stdout
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

_SCHED_PATH = (
    Path(__file__).resolve().parents[1] / "src" / "databricks_apps_scheduler" / "cli.py"
)
_SCHED_SPEC = importlib.util.spec_from_file_location(
    "_databricks_apps_scheduler_cli", _SCHED_PATH
)
if _SCHED_SPEC is None or _SCHED_SPEC.loader is None:
    raise RuntimeError(f"Failed to load scheduler module from {_SCHED_PATH}")

sched = importlib.util.module_from_spec(_SCHED_SPEC)
_SCHED_SPEC.loader.exec_module(sched)


def test_parse_app_names_deduplicates_and_preserves_order() -> None:
    assert sched.parse_app_names(" app-a,app-b,app-a,, app-c ") == [
        "app-a",
        "app-b",
        "app-c",
    ]


def test_parse_active_days_all() -> None:
    assert sched.parse_active_days("all") == set(range(7))


def test_parse_active_days_subset() -> None:
    assert sched.parse_active_days("0,2,4") == {0, 2, 4}


def test_parse_active_days_invalid_value() -> None:
    try:
        sched.parse_active_days("7")
    except ValueError:
        pass
    else:
        raise AssertionError("Expected ValueError for invalid weekday")


def test_resolve_action_outside_active_days_stops() -> None:
    now_local = datetime(2026, 2, 8, 10, 0, tzinfo=ZoneInfo("UTC"))
    assert (
        sched.resolve_action(now_local, start_hour=8, end_hour=17, active_days={0})
        == "stop"
    )


def test_resolve_action_overnight_window() -> None:
    now_local = datetime(2026, 2, 8, 23, 0, tzinfo=ZoneInfo("UTC"))
    assert (
        sched.resolve_action(
            now_local, start_hour=22, end_hour=6, active_days=set(range(7))
        )
        == "start"
    )


def test_resolve_now_converts_timezone() -> None:
    stockholm = ZoneInfo("Europe/Stockholm")
    now_local = sched.resolve_now("2026-02-08T10:00:00+00:00", tz=stockholm)
    assert now_local.hour == 11
    assert now_local.tzinfo is not None


def test_cli_script_path_exists() -> None:
    assert os.path.exists(Path("src/databricks_apps_scheduler/cli.py"))
    assert os.path.exists(Path("src/databricks_apps_scheduler/__main__.py"))


def test_main_json_output_with_forced_action_dry_run() -> None:
    class _State:
        def __init__(self, name: str) -> None:
            self.name = name

    class _ComputeStatus:
        def __init__(self, state_name: str) -> None:
            self.state = _State(state_name)

    class _App:
        def __init__(self, name: str, state_name: str) -> None:
            self.name = name
            self.compute_status = _ComputeStatus(state_name)

    class _AppsAPI:
        def __init__(self) -> None:
            self._apps = [_App("app-a", "STOPPED")]
            self.started: list[str] = []
            self.stopped: list[str] = []

        def list(self):
            return self._apps

        def start_and_wait(self, app_name: str) -> None:
            self.started.append(app_name)

        def stop_and_wait(self, app_name: str) -> None:
            self.stopped.append(app_name)

    class _WorkspaceClient:
        def __init__(self, profile: str | None = None) -> None:
            self.profile = profile
            self.apps = _AppsAPI()

    fake_databricks = types.ModuleType("databricks")
    fake_sdk = types.ModuleType("databricks.sdk")
    setattr(fake_sdk, "WorkspaceClient", _WorkspaceClient)
    setattr(fake_databricks, "sdk", fake_sdk)

    stdout = io.StringIO()
    with patch.dict(
        sys.modules, {"databricks": fake_databricks, "databricks.sdk": fake_sdk}
    ):
        with redirect_stdout(stdout):
            exit_code = sched.main(
                [
                    "--app-names",
                    "app-a",
                    "--action",
                    "start",
                    "--dry-run",
                    "--output",
                    "json",
                    "--timezone",
                    "UTC",
                    "--now",
                    "2026-02-08T10:00:00+00:00",
                ]
            )

    assert exit_code == 0
    payload = json.loads(stdout.getvalue().strip())
    assert payload["action"] == "start"
    assert payload["action_mode"] == "start"
    assert payload["apps_changed"] == 1
    assert payload["apps_evaluated"] == 1
    assert payload["dry_run"] is True
    assert payload["exit_code"] == 0
