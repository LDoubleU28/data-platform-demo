"""Asset checks = data-quality gates on the mart asset.

Each check queries Trino (the source of truth) for the current mart rows and
the dimension's expected cell universe, then delegates to a pure function in
`transforms` so the pass/fail logic itself is unit-tested without Trino.

There is no silent fallback to the on-disk JSON: if Trino is unreachable the
check FAILS rather than passing on a stale artifact. An explicit, opt-in
offline mode (env `DPD_OFFLINE_MART_JSON=1`) exists only for demos/tests that
deliberately want to evaluate the checks against the emitted JSON; it is never
the default, and it logs that it is doing so.
"""

from __future__ import annotations

import json
import os
from typing import Any

from dagster import AssetCheckResult, asset_check

from . import sql, transforms
from .assets import MART_JSON_PATH, mart_orders_by_segment
from .resources import TrinoResource

OFFLINE_ENV = "DPD_OFFLINE_MART_JSON"


def _offline_enabled() -> bool:
    return os.getenv(OFFLINE_ENV, "").strip().lower() in ("1", "true", "yes")


def _load_mart_rows(trino: TrinoResource) -> list[dict[str, Any]]:
    """Live Trino read. No fallback: a connection error fails the check.

    Offline mode (explicit env opt-in only) reads the emitted JSON instead, for
    test/demo evaluation without a running engine. It does not mask a Trino
    outage in normal operation.
    """
    if _offline_enabled():
        if not os.path.exists(MART_JSON_PATH):
            raise FileNotFoundError(
                f"{OFFLINE_ENV} set but {MART_JSON_PATH} does not exist"
            )
        with open(MART_JSON_PATH) as f:
            return json.load(f)
    return trino.query(sql.select_mart_sql())


def _expected_keys(trino: TrinoResource) -> set[tuple[str, str]]:
    """Expected segment x region universe, read live from the staged dimension."""
    if _offline_enabled():
        # In offline mode we have no dimension to read; the JSON is the mart
        # only. Derive the expected universe from the JSON's own cells so the
        # completeness check stays well-defined (it cannot detect a drop in
        # offline mode; that is what the live path is for).
        rows = _load_mart_rows(trino)
        return transforms.mart_segment_region_keys(rows)
    dim_rows = trino.query(sql.select_distinct_segment_region_sql())
    return transforms.expected_segment_region_keys(dim_rows)


def _staged_orders_count(trino: TrinoResource) -> int:
    rows = trino.query(sql.count_stg_orders_sql())
    return int(rows[0]["n"]) if rows else 0


@asset_check(asset=mart_orders_by_segment, description="Mart must be non-empty")
def mart_non_empty(trino: TrinoResource) -> AssetCheckResult:
    outcome = transforms.check_non_empty(_load_mart_rows(trino))
    return AssetCheckResult(passed=outcome.passed, metadata=outcome.metadata)


@asset_check(
    asset=mart_orders_by_segment,
    description="Mart contains every expected segment x region cell (no dropped cells)",
)
def mart_complete_segment_region(trino: TrinoResource) -> AssetCheckResult:
    outcome = transforms.check_completeness(_load_mart_rows(trino), _expected_keys(trino))
    return AssetCheckResult(passed=outcome.passed, metadata=outcome.metadata)


@asset_check(
    asset=mart_orders_by_segment,
    description="Every mart cell exists in the dimension (no orphan cells)",
)
def mart_referential_integrity(trino: TrinoResource) -> AssetCheckResult:
    outcome = transforms.check_referential_integrity(
        _load_mart_rows(trino), _expected_keys(trino)
    )
    return AssetCheckResult(passed=outcome.passed, metadata=outcome.metadata)


@asset_check(
    asset=mart_orders_by_segment,
    description="Mart row count equals the expected number of cells",
)
def mart_row_count(trino: TrinoResource) -> AssetCheckResult:
    expected = len(_expected_keys(trino))
    outcome = transforms.check_row_count_in_range(_load_mart_rows(trino), expected)
    return AssetCheckResult(passed=outcome.passed, metadata=outcome.metadata)


@asset_check(
    asset=mart_orders_by_segment,
    description="sum(order_count) reconciles to the staged orders count",
)
def mart_order_count_reconciles(trino: TrinoResource) -> AssetCheckResult:
    if _offline_enabled():
        # No staged-orders table to read offline; reconcile against the JSON's
        # own total so the check is well-defined (a no-op identity offline).
        rows = _load_mart_rows(trino)
        staged = transforms.total_order_count(rows)
    else:
        staged = _staged_orders_count(trino)
    outcome = transforms.check_order_count_reconciles(_load_mart_rows(trino), staged)
    return AssetCheckResult(passed=outcome.passed, metadata=outcome.metadata)


@asset_check(
    asset=mart_orders_by_segment,
    description="Per-cell revenue invariants (non-negative; zero-order cells carry zero revenue)",
)
def mart_revenue_consistent(trino: TrinoResource) -> AssetCheckResult:
    outcome = transforms.check_revenue_consistent(_load_mart_rows(trino))
    return AssetCheckResult(passed=outcome.passed, metadata=outcome.metadata)
