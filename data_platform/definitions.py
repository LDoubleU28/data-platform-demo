"""Top-level Dagster Definitions object: `data_platform.definitions:defs`."""

from __future__ import annotations

import os

from dagster import Definitions

from . import assets as _assets
from . import checks as _checks
from .resources import TrinoResource
from .schedule import all_assets_job, daily_schedule

trino_resource = TrinoResource(
    host=os.getenv("TRINO_HOST", "localhost"),
    port=int(os.getenv("TRINO_PORT", "8080")),
    user=os.getenv("TRINO_USER", "demo"),
    catalog="memory",
    schema_="default",
)

defs = Definitions(
    assets=[
        _assets.stg_orders,
        _assets.stg_customer_segment,
        _assets.mart_orders_by_segment,
    ],
    asset_checks=[
        _checks.mart_non_empty,
        _checks.mart_complete_segment_region,
        _checks.mart_referential_integrity,
        _checks.mart_row_count,
        _checks.mart_order_count_reconciles,
        _checks.mart_revenue_consistent,
    ],
    jobs=[all_assets_job],
    schedules=[daily_schedule],
    resources={"trino": trino_resource},
)
