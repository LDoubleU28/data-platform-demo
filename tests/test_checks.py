"""Unit tests for the data-quality check logic. No live Trino required.

Every check has a pair: it PASSES on good input and FAILS on a real,
representative bad input. The completeness check is additionally pinned to the
concrete regression it guards: the old INNER-join mart that dropped 3 of the 12
seeded segment x region cells.
"""

from decimal import Decimal

from data_platform import transforms


# The full, correct mart: all 12 seeded segment x region cells present.
SEGMENTS = ["enterprise", "midmarket", "startup"]
REGIONS = ["east", "north", "south", "west"]
EXPECTED_KEYS = {(s, r) for s in SEGMENTS for r in REGIONS}


def _cell(segment, region, order_count, total_revenue):
    avg = None if order_count == 0 else round(total_revenue / order_count, 2)
    return {
        "segment": segment,
        "region": region,
        "order_count": order_count,
        "total_revenue": f"{total_revenue:.2f}",
        "avg_order_value": (None if avg is None else f"{avg:.2f}"),
    }


def _good_mart():
    """A complete 12-cell mart; two cells deliberately have 0 orders / 0 revenue."""
    rows = []
    for s in SEGMENTS:
        for r in REGIONS:
            if (s, r) in {("midmarket", "north"), ("midmarket", "south")}:
                rows.append(_cell(s, r, 0, 0.0))  # order-less cell, visible with LEFT JOIN
            else:
                rows.append(_cell(s, r, 10, 1000.0))
    return rows


def _old_incomplete_mart():
    """The pre-fix INNER-join mart: the 3 midmarket non-west cells are missing."""
    dropped = {("midmarket", "east"), ("midmarket", "north"), ("midmarket", "south")}
    return [c for c in _good_mart() if (c["segment"], c["region"]) not in dropped]


# --------------------------------------------------------------------------
# non-empty
# --------------------------------------------------------------------------


def test_non_empty_passes_with_rows():
    out = transforms.check_non_empty(_good_mart())
    assert out.passed is True
    assert out.metadata["row_count"] == 12


def test_non_empty_fails_when_empty():
    out = transforms.check_non_empty([])
    assert out.passed is False


# --------------------------------------------------------------------------
# completeness — the headline check
# --------------------------------------------------------------------------


def test_completeness_passes_on_full_12_cell_mart():
    out = transforms.check_completeness(_good_mart(), EXPECTED_KEYS)
    assert out.passed is True
    assert out.metadata["expected_cells"] == 12
    assert out.metadata["actual_cells"] == 12
    assert out.metadata["missing_cells"] == []


def test_completeness_FAILS_on_old_inner_join_mart():
    """The exact regression: the old mart dropped 3 of 12 cells; the gate fires."""
    out = transforms.check_completeness(_old_incomplete_mart(), EXPECTED_KEYS)
    assert out.passed is False
    assert out.metadata["actual_cells"] == 9
    assert sorted(out.metadata["missing_cells"]) == [
        "midmarket/east",
        "midmarket/north",
        "midmarket/south",
    ]


def test_completeness_fails_on_orphan_cell():
    rows = _good_mart() + [_cell("ghost", "nowhere", 1, 5.0)]
    out = transforms.check_completeness(rows, EXPECTED_KEYS)
    assert out.passed is False
    assert "ghost/nowhere" in out.metadata["unexpected_cells"]


def test_expected_keys_derive_from_dimension_rows():
    dim = [
        {"segment": "enterprise", "region": "north"},
        {"segment": "enterprise", "region": "north"},  # dup collapses
        {"segment": "startup", "region": "west"},
        {"segment": None, "region": "x"},  # nulls excluded
    ]
    assert transforms.expected_segment_region_keys(dim) == {
        ("enterprise", "north"),
        ("startup", "west"),
    }


# --------------------------------------------------------------------------
# referential integrity
# --------------------------------------------------------------------------


def test_referential_integrity_passes_on_clean_mart():
    assert transforms.check_referential_integrity(_good_mart(), EXPECTED_KEYS).passed is True


def test_referential_integrity_fails_on_orphan_cell():
    rows = _good_mart() + [_cell("enterprise", "atlantis", 3, 99.0)]
    out = transforms.check_referential_integrity(rows, EXPECTED_KEYS)
    assert out.passed is False
    assert "enterprise/atlantis" in out.metadata["orphan_cells"]


# --------------------------------------------------------------------------
# row-count range
# --------------------------------------------------------------------------


def test_row_count_passes_at_expected():
    assert transforms.check_row_count_in_range(_good_mart(), expected=12).passed is True


def test_row_count_fails_when_too_few():
    out = transforms.check_row_count_in_range(_old_incomplete_mart(), expected=12)
    assert out.passed is False
    assert out.metadata["row_count"] == 9


def test_row_count_fails_when_too_many():
    rows = _good_mart() + [_cell("startup", "north", 1, 1.0)]  # duplicate cell -> fan-out
    assert transforms.check_row_count_in_range(rows, expected=12).passed is False


def test_row_count_tolerance_band():
    assert transforms.check_row_count_in_range(_good_mart(), expected=13, tolerance=1).passed is True


# --------------------------------------------------------------------------
# order-count reconciliation
# --------------------------------------------------------------------------


def test_order_count_reconciles_passes():
    rows = _good_mart()
    staged = transforms.total_order_count(rows)  # by construction, matches
    assert transforms.check_order_count_reconciles(rows, staged).passed is True


def test_order_count_reconciles_FAILS_when_orders_dropped():
    """Mimics the old bug: staged 120 orders but the mart only counts 100."""
    rows = _good_mart()  # sums to 100 (10 cells x 10)
    out = transforms.check_order_count_reconciles(rows, staged_orders_count=120)
    assert out.passed is False
    assert out.metadata["mart_order_count"] == 100
    assert out.metadata["staged_orders_count"] == 120
    assert out.metadata["delta"] == -20


# --------------------------------------------------------------------------
# revenue consistency
# --------------------------------------------------------------------------


def test_revenue_consistent_passes_on_good_mart():
    assert transforms.check_revenue_consistent(_good_mart()).passed is True


def test_revenue_consistent_fails_on_negative_revenue():
    rows = [_cell("startup", "east", 2, -5.0)]
    out = transforms.check_revenue_consistent(rows)
    assert out.passed is False
    assert out.metadata["offending_cells"] == 1


def test_revenue_consistent_fails_on_revenue_without_orders():
    """A zero-order cell carrying revenue means the aggregation is wrong."""
    rows = [{"segment": "x", "region": "y", "order_count": 0, "total_revenue": "42.00"}]
    out = transforms.check_revenue_consistent(rows)
    assert out.passed is False


def test_revenue_consistent_allows_zero_order_zero_revenue_cell():
    rows = [_cell("midmarket", "north", 0, 0.0)]
    assert transforms.check_revenue_consistent(rows).passed is True


# --------------------------------------------------------------------------
# numeric coercion — Decimal, exact money
# --------------------------------------------------------------------------


def test_coerce_decimal_handles_none_str_float_exactly():
    assert transforms.coerce_decimal(None) == Decimal("0")
    assert transforms.coerce_decimal("12.50") == Decimal("12.50")
    # float routed via str -> no binary noise
    assert transforms.coerce_decimal(0.1) == Decimal("0.1")


def test_coerce_int_handles_garbage():
    assert transforms.coerce_int(None) == 0
    assert transforms.coerce_int("7") == 7


def test_total_revenue_is_exact_decimal():
    rows = [_cell("a", "b", 1, 0.10), _cell("a", "c", 1, 0.20)]
    assert transforms.total_revenue(rows) == Decimal("0.30")  # exact, not 0.30000...4


def test_total_order_count_sums():
    assert transforms.total_order_count(_good_mart()) == 100


def test_mart_preview_caps_length():
    rows = [{"segment": str(i)} for i in range(50)]
    assert len(transforms.mart_preview(rows, limit=10)) == 10
