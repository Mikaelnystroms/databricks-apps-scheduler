"""Module entrypoint for `python -m databricks_apps_scheduler`."""

from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
