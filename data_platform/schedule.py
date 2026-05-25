"""Daily schedule that materializes every asset (and runs their checks)."""

from __future__ import annotations

from dagster import AssetSelection, ScheduleDefinition, define_asset_job

all_assets_job = define_asset_job(
    name="materialize_all_job",
    selection=AssetSelection.all(),
)

daily_schedule = ScheduleDefinition(
    name="daily_materialize_all",
    job=all_assets_job,
    cron_schedule="0 6 * * *",  # 06:00 every day
)
