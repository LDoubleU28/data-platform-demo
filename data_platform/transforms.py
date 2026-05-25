"""Pure transform + validation helpers.

These operate on plain Python rows (lists of dicts), so they are fully
unit-testable without Trino or Docker. The Dagster asset checks call straight
into the `check_*` functions below.

Money is handled as ``Decimal`` end to end: Trino returns ``Decimal`` for
``SUM``/``AVG`` and we keep it that way through the checks and metadata rather
than collapsing to binary float (money-as-float loses cents at scale).
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, NamedTuple


Row = dict[str, Any]

ZERO = Decimal("0")


class CheckOutcome(NamedTuple):
    passed: bool
    metadata: dict[str, Any]


def coerce_decimal(value: Any) -> Decimal:
    """Best-effort Decimal coercion; Trino may return Decimal/str/int for SUM/AVG.

    Floats are routed through ``str`` so we don't inherit binary-float noise
    (``Decimal(0.1)`` != ``Decimal("0.1")``).
    """
    if value is None:
        return ZERO
    if isinstance(value, Decimal):
        return value
    if isinstance(value, float):
        return Decimal(str(value))
    try:
        return Decimal(value)
    except (InvalidOperation, TypeError, ValueError):
        return ZERO


def coerce_int(value: Any) -> int:
    """Coerce a count-like value to int (0 for None/garbage)."""
    if value is None:
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(coerce_decimal(value))


def total_revenue(rows: list[Row]) -> Decimal:
    return sum((coerce_decimal(r.get("total_revenue")) for r in rows), ZERO)


def total_order_count(rows: list[Row]) -> int:
    return sum(coerce_int(r.get("order_count")) for r in rows)


# --------------------------------------------------------------------------
# Expected universe of segment x region cells.
#
# The dimension (customer_segment) is the source of truth for which
# segment x region combinations *should* exist in the mart. Given the staged
# dimension rows, the expected mart key set is the distinct (segment, region)
# pairs it carries. The mart is built with a LEFT JOIN from the dimension, so
# every one of these must appear (with order_count 0 where there are no orders).
# --------------------------------------------------------------------------


def expected_segment_region_keys(dim_rows: Iterable[Row]) -> set[tuple[str, str]]:
    """Distinct (segment, region) pairs present in the staged dimension."""
    return {
        (r.get("segment"), r.get("region"))
        for r in dim_rows
        if r.get("segment") not in (None, "") and r.get("region") not in (None, "")
    }


def mart_segment_region_keys(rows: Iterable[Row]) -> set[tuple[str, str]]:
    return {(r.get("segment"), r.get("region")) for r in rows}


# --------------------------------------------------------------------------
# Checks. Each can actually fail on a real failure mode (see tests).
# --------------------------------------------------------------------------


def check_non_empty(rows: list[Row]) -> CheckOutcome:
    """Mart must contain at least one row."""
    n = len(rows)
    return CheckOutcome(passed=n > 0, metadata={"row_count": n})


def check_completeness(rows: list[Row], expected_keys: set[tuple[str, str]]) -> CheckOutcome:
    """Mart must contain every expected segment x region cell.

    ``expected_keys`` is derived from the staged dimension (the source of truth
    for the cell universe). Fires when the mart is missing cells; this is the
    check that an INNER join (which silently drops order-less cells) would fail.
    """
    actual = mart_segment_region_keys(rows)
    missing = sorted(k for k in expected_keys if k not in actual)
    unexpected = sorted(k for k in actual if k not in expected_keys)
    return CheckOutcome(
        passed=len(missing) == 0 and len(unexpected) == 0,
        metadata={
            "expected_cells": len(expected_keys),
            "actual_cells": len(actual),
            "missing_cells": [f"{s}/{r}" for s, r in missing],
            "unexpected_cells": [f"{s}/{r}" for s, r in unexpected],
        },
    )


def check_referential_integrity(
    rows: list[Row], expected_keys: set[tuple[str, str]]
) -> CheckOutcome:
    """Every mart (segment, region) must exist in the dimension.

    Catches orphan cells: a segment/region in the mart that the dimension never
    defined (e.g. a bad join that fabricated or mislabeled groups).
    """
    orphans = sorted(k for k in mart_segment_region_keys(rows) if k not in expected_keys)
    return CheckOutcome(
        passed=len(orphans) == 0,
        metadata={
            "orphan_cells": [f"{s}/{r}" for s, r in orphans],
            "row_count": len(rows),
        },
    )


def check_row_count_in_range(
    rows: list[Row], expected: int, tolerance: int = 0
) -> CheckOutcome:
    """Row count must sit within ``expected +/- tolerance``.

    Catches both partial builds (too few rows) and accidental fan-out / dup
    cells (too many). With a LEFT JOIN + GROUP BY, the mart is exactly one row
    per expected cell, so the demo runs this with tolerance 0.
    """
    n = len(rows)
    lo, hi = expected - tolerance, expected + tolerance
    return CheckOutcome(
        passed=lo <= n <= hi,
        metadata={"row_count": n, "expected": expected, "low": lo, "high": hi},
    )


def check_order_count_reconciles(rows: list[Row], staged_orders_count: int) -> CheckOutcome:
    """sum(mart.order_count) must equal the staged orders row count.

    Every staged order maps to exactly one customer (custkey is unique in the
    dimension), so the per-cell counts must add back up to the staged total. A
    dropped/duplicated join leg breaks this identity.
    """
    mart_total = total_order_count(rows)
    return CheckOutcome(
        passed=mart_total == staged_orders_count,
        metadata={
            "mart_order_count": mart_total,
            "staged_orders_count": staged_orders_count,
            "delta": mart_total - staged_orders_count,
        },
    )


def check_revenue_consistent(rows: list[Row]) -> CheckOutcome:
    """Per-cell revenue invariants that a real build must hold.

    For every cell: revenue and order_count are non-negative, and a cell with
    zero orders carries exactly zero revenue (a non-zero revenue on a zero-order
    cell would mean the aggregation is wrong). This is meaningful, unlike a bare
    ``>= 0`` tautology, because it ties the two columns together.
    """
    offenders: list[dict[str, Any]] = []
    for r in rows:
        rev = coerce_decimal(r.get("total_revenue"))
        cnt = coerce_int(r.get("order_count"))
        if rev < ZERO or cnt < 0 or (cnt == 0 and rev != ZERO):
            offenders.append(
                {
                    "segment": r.get("segment"),
                    "region": r.get("region"),
                    "order_count": cnt,
                    "total_revenue": str(rev),
                }
            )
    return CheckOutcome(
        passed=len(offenders) == 0,
        metadata={
            "offending_cells": len(offenders),
            "offenders": offenders[:10],
            "total_revenue": str(total_revenue(rows)),
        },
    )


def mart_preview(rows: list[Row], limit: int = 10) -> list[Row]:
    """Small preview slice for materialization metadata."""
    return rows[:limit]
