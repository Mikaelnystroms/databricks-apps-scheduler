"""Start or stop Databricks Apps based on rules."""

from __future__ import annotations

import argparse
import importlib
import json
import logging
from datetime import datetime
from typing import Any, Iterable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

logger = logging.getLogger("databricks_apps_scheduler")

RUNNING_STATES = {"ACTIVE", "STARTING"}
STOPPED_STATES = {"STOPPED", "STOPPING"}
INVALID_WEEKDAY_ERROR = (
    "Invalid weekday '{token}'. Expected integers in the range 0..6."
)


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def _csv_tokens(raw: str) -> Iterable[str]:
    for token in (part.strip() for part in raw.split(",")):
        if token:
            yield token


def parse_app_names(raw: str) -> list[str]:
    seen: set[str] = set()
    names: list[str] = []
    for candidate in _csv_tokens(raw):
        if candidate in seen:
            continue
        seen.add(candidate)
        names.append(candidate)
    return names


def parse_active_days(raw: str) -> set[int]:
    if raw.strip().lower() in {"", "all"}:
        return set(range(7))

    active_days: set[int] = set()
    for token in _csv_tokens(raw):
        try:
            day = int(token)
        except ValueError as exc:
            raise ValueError(INVALID_WEEKDAY_ERROR.format(token=token)) from exc
        if day < 0 or day > 6:
            raise ValueError(INVALID_WEEKDAY_ERROR.format(token=token))
        active_days.add(day)
    if not active_days:
        raise ValueError("At least one active day must be configured.")
    return active_days


def resolve_action(
    now_local: datetime, *, start_hour: int, end_hour: int, active_days: set[int]
) -> str:
    if now_local.weekday() not in active_days:
        return "stop"

    if start_hour == end_hour:
        return "start"

    if start_hour < end_hour:
        within_schedule = start_hour <= now_local.hour < end_hour
    else:
        # Supports overnight windows such as 22 -> 06.
        within_schedule = now_local.hour >= start_hour or now_local.hour < end_hour

    return "start" if within_schedule else "stop"


def resolve_now(now_raw: str | None, *, tz: ZoneInfo) -> datetime:
    if now_raw is None:
        return datetime.now(tz)

    normalized = now_raw.strip()
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"

    try:
        candidate = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(
            f"Invalid --now value '{now_raw}'. Use ISO-8601, e.g. 2026-02-08T15:30:00+01:00."
        ) from exc

    if candidate.tzinfo is None:
        return candidate.replace(tzinfo=tz)
    return candidate.astimezone(tz)


def _state_name(app: object) -> str:
    compute_status = getattr(app, "compute_status", None)
    state = getattr(compute_status, "state", None)
    if state is None:
        return "UNKNOWN"

    name = getattr(state, "name", None)
    if isinstance(name, str):
        return name.upper()
    return str(state).replace("ComputeState.", "").upper()


def _select_target_apps(
    client: Any, app_names: Iterable[str]
) -> tuple[list[object], list[str]]:
    by_name = {app.name: app for app in client.apps.list()}
    targets: list[object] = []
    missing: list[str] = []

    for app_name in app_names:
        app = by_name.get(app_name)
        if app is None:
            missing.append(app_name)
            continue
        targets.append(app)

    return targets, missing


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Manage Databricks Apps start/stop state"
    )
    parser.add_argument(
        "--app-names",
        required=True,
        help="Comma-separated Databricks App names to manage.",
    )
    parser.add_argument(
        "--timezone",
        default="UTC",
        help="IANA timezone, for example Europe/Stockholm.",
    )
    parser.add_argument(
        "--start-hour",
        type=int,
        default=8,
        help="Start hour in 24h format.",
    )
    parser.add_argument(
        "--end-hour",
        type=int,
        default=17,
        help="End hour in 24h format. End is exclusive.",
    )
    parser.add_argument(
        "--active-days",
        default="all",
        help=(
            "Comma-separated weekday numbers where Monday=0 and Sunday=6, "
            "or 'all' (default)."
        ),
    )
    parser.add_argument(
        "--working-days",
        dest="active_days",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--action",
        default="auto",
        choices=["auto", "start", "stop"],
        help="Action mode: auto (default), or force start/stop.",
    )
    parser.add_argument(
        "--now",
        default=None,
        help="Optional ISO-8601 timestamp override used instead of current time.",
    )
    parser.add_argument(
        "--output",
        default="text",
        choices=["text", "json"],
        help="Output format for run summary.",
    )
    parser.add_argument(
        "--profile",
        default=None,
        help="Optional Databricks CLI profile name to use for authentication.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log planned actions without calling start/stop operations.",
    )
    parser.add_argument(
        "--ignore-missing-apps",
        action="store_true",
        help="Continue even when configured app names are not found in the workspace.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging.",
    )
    return parser


def _validate_hour(parser: argparse.ArgumentParser, value: int, flag: str) -> None:
    if not 0 <= value <= 23:
        parser.error(f"{flag} must be in the range 0..23.")


def _summary_payload(
    *,
    action: str,
    args: argparse.Namespace,
    now_local: datetime,
    app_names: list[str],
    apps_changed: int,
    apps_evaluated: int,
    exit_code: int,
    failures: int,
    missing_apps: list[str],
    active_days: set[int] | None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "action": action,
        "action_mode": args.action,
        "apps_changed": apps_changed,
        "apps_evaluated": apps_evaluated,
        "apps_requested": app_names,
        "dry_run": args.dry_run,
        "end_hour": args.end_hour,
        "exit_code": exit_code,
        "failures": failures,
        "missing_apps": missing_apps,
        "output": args.output,
        "start_hour": args.start_hour,
        "time_local": now_local.isoformat(),
        "timezone": args.timezone,
    }
    if active_days is not None:
        payload["active_days"] = sorted(active_days)
    return payload


def emit_result(output_format: str, payload: dict[str, object]) -> None:
    if output_format == "json":
        print(json.dumps(payload, sort_keys=True))


def _emit_and_return(
    *,
    action: str,
    active_days: set[int] | None,
    app_names: list[str],
    apps_changed: int,
    apps_evaluated: int,
    args: argparse.Namespace,
    exit_code: int,
    failures: int,
    missing_apps: list[str],
    now_local: datetime,
) -> int:
    emit_result(
        args.output,
        _summary_payload(
            action=action,
            active_days=active_days,
            app_names=app_names,
            apps_changed=apps_changed,
            apps_evaluated=apps_evaluated,
            args=args,
            exit_code=exit_code,
            failures=failures,
            missing_apps=missing_apps,
            now_local=now_local,
        ),
    )
    return exit_code


def _workspace_client_class() -> Any:
    sdk_module = importlib.import_module("databricks.sdk")
    return getattr(sdk_module, "WorkspaceClient")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(args.verbose)

    _validate_hour(parser, args.start_hour, "--start-hour")
    _validate_hour(parser, args.end_hour, "--end-hour")

    app_names = parse_app_names(args.app_names)
    if not app_names:
        parser.error("--app-names must include at least one app name.")

    try:
        tz = ZoneInfo(args.timezone)
    except ZoneInfoNotFoundError:
        parser.error(f"Unknown timezone '{args.timezone}'.")

    try:
        now_local = resolve_now(args.now, tz=tz)
    except ValueError as exc:
        parser.error(str(exc))

    active_days: set[int] | None = None
    if args.action == "auto":
        try:
            active_days = parse_active_days(args.active_days)
        except ValueError as exc:
            parser.error(f"--active-days invalid: {exc}")
        action = resolve_action(
            now_local,
            start_hour=args.start_hour,
            end_hour=args.end_hour,
            active_days=active_days,
        )
    else:
        action = args.action

    logger.info("Current local time: %s", now_local.strftime("%Y-%m-%d %H:%M %Z"))
    if args.action == "auto":
        logger.info(
            "Window: %02d:00-%02d:00, active days: %s",
            args.start_hour,
            args.end_hour,
            sorted(active_days or set()),
        )
    else:
        logger.info("Window config ignored because --action=%s.", args.action)
    logger.info("Action selected: %s", action.upper())
    logger.info("Dry run: %s", args.dry_run)

    try:
        workspace_client = _workspace_client_class()
    except (ImportError, ModuleNotFoundError):
        logger.error(
            "Missing dependency 'databricks-sdk'. Install dependencies with: pip install ."
        )
        return _emit_and_return(
            action=action,
            active_days=active_days,
            app_names=app_names,
            apps_changed=0,
            apps_evaluated=0,
            args=args,
            exit_code=2,
            failures=1,
            missing_apps=[],
            now_local=now_local,
        )

    try:
        client = (
            workspace_client(profile=args.profile)
            if args.profile
            else workspace_client()
        )
    except Exception as exc:
        logger.error("Failed to initialize Databricks client: %s", exc)
        return _emit_and_return(
            action=action,
            active_days=active_days,
            app_names=app_names,
            apps_changed=0,
            apps_evaluated=0,
            args=args,
            exit_code=2,
            failures=1,
            missing_apps=[],
            now_local=now_local,
        )

    try:
        target_apps, missing_apps = _select_target_apps(client, app_names)
    except Exception as exc:
        logger.error("Failed to list Databricks apps: %s", exc)
        return _emit_and_return(
            action=action,
            active_days=active_days,
            app_names=app_names,
            apps_changed=0,
            apps_evaluated=0,
            args=args,
            exit_code=2,
            failures=1,
            missing_apps=[],
            now_local=now_local,
        )

    if missing_apps and not args.ignore_missing_apps:
        logger.error("Configured apps not found: %s", missing_apps)
        return _emit_and_return(
            action=action,
            active_days=active_days,
            app_names=app_names,
            apps_changed=0,
            apps_evaluated=len(target_apps),
            args=args,
            exit_code=1,
            failures=0,
            missing_apps=missing_apps,
            now_local=now_local,
        )

    if missing_apps and args.ignore_missing_apps:
        logger.warning("Configured apps not found: %s", missing_apps)

    if not target_apps:
        logger.warning("No matching apps were found; nothing to do.")
        return _emit_and_return(
            action=action,
            active_days=active_days,
            app_names=app_names,
            apps_changed=0,
            apps_evaluated=0,
            args=args,
            exit_code=0 if args.ignore_missing_apps else 1,
            failures=0,
            missing_apps=missing_apps,
            now_local=now_local,
        )

    failures = 0
    updated = 0

    action_fn = (
        client.apps.start_and_wait if action == "start" else client.apps.stop_and_wait
    )
    terminal_states = RUNNING_STATES if action == "start" else STOPPED_STATES
    action_verb = "Starting" if action == "start" else "Stopping"

    for app in target_apps:
        app_name = getattr(app, "name", "<unknown>")
        state = _state_name(app)

        try:
            if state in terminal_states:
                logger.info("Skipping %s, already in state %s.", app_name, state)
                continue

            logger.info("%s app %s from state %s.", action_verb, app_name, state)
            if not args.dry_run:
                action_fn(app_name)
            updated += 1
        except Exception as exc:  # pragma: no cover - network/API interaction
            failures += 1
            logger.error("Failed to %s app %s: %s", action, app_name, exc)

    logger.info("Apps evaluated: %d", len(target_apps))
    logger.info("Apps changed: %d", updated)
    logger.info("Failures: %d", failures)
    return _emit_and_return(
        action=action,
        active_days=active_days,
        app_names=app_names,
        apps_changed=updated,
        apps_evaluated=len(target_apps),
        args=args,
        exit_code=1 if failures else 0,
        failures=failures,
        missing_apps=missing_apps,
        now_local=now_local,
    )


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
