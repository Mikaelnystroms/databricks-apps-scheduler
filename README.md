# databricks-apps-scheduler

Standalone CLI to start/stop Databricks Apps based on runtime rules.

## Install

```bash
pip install .
```

For development:

```bash
pip install -e .[dev]
```

## Databricks authentication

The CLI uses the Databricks SDK authentication chain (environment variables or Databricks CLI profiles).  
If you use profiles, pass `--profile <name>`.

## Usage (auto mode)

```bash
databricks-apps-scheduler \
  --app-names app-a,app-b \
  --timezone Europe/Stockholm \
  --start-hour 8 \
  --end-hour 17 \
  --active-days all
```

## Important flags

- `--action`: `auto` (default), `start`, or `stop`.
- `--now`: optional ISO-8601 timestamp override (`2026-02-08T15:30:00+01:00`).
- `--output`: `text` (default) or `json` summary output.
- `--dry-run`: show planned actions without calling Databricks start/stop APIs.
- `--ignore-missing-apps`: continue even if one or more app names are missing.
- `--profile`: use a named Databricks CLI profile.
- `--verbose`: enable debug logging.
- `--active-days`: optional day filter (`0..6`, Monday=0) or `all` (default).

## Schedule behavior

- On days outside `--active-days`, apps are stopped.
- If `start-hour < end-hour`, the start window is same day (for example 08:00-17:00).
- If `start-hour > end-hour`, the window crosses midnight (for example 22:00-06:00).
- If `start-hour == end-hour`, apps are always started on active days.

## Running from a Databricks Job

Schedule the job in Databricks Workflows, and pass this script's arguments as task parameters.
Use `examples/job.yml` in this repo as a reference template.

Example task command:

```bash
python /Workspace/.../src/databricks_apps_scheduler/cli.py \
  --app-names app-a,app-b \
  --timezone Europe/Stockholm \
  --start-hour 8 \
  --end-hour 17 \
  --active-days all \
  --action auto \
  --output json
```

If your job should run multiple times per day, configure the job schedule accordingly.  
This CLI only decides whether to start or stop at runtime, based on current time/day and the parameters you provide.

For local module-style execution:

```bash
python -m databricks_apps_scheduler --help
```
