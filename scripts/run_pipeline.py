#!/usr/bin/env python3
"""Materialize the full pipeline against a live Trino, without the Dagster UI.

Runs the same SQL the Dagster assets run (raw -> staging -> federated mart),
writes the mart JSON, then evaluates the data-quality checks and prints a
summary. Useful for a one-shot smoke test after `docker compose up -d`.

    python scripts/run_pipeline.py
"""

from __future__ import annotations

import json
import os
import sys

# Allow running as a plain script (no install needed) by adding repo root.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_platform import sql, transforms  # noqa: E402
from data_platform.assets import MART_JSON_PATH, DATA_DIR, _serialize_mart_row  # noqa: E402
from data_platform.resources import TrinoResource  # noqa: E402


def main() -> int:
    trino = TrinoResource(
        host=os.getenv("TRINO_HOST", "localhost"),
        port=int(os.getenv("TRINO_PORT", "8080")),
        user=os.getenv("TRINO_USER", "demo"),
        catalog="memory",
        schema_="default",
    )

    print("[1/4] staging tpch.tiny.orders -> memory.default.stg_orders")
    trino.execute(sql.ensure_memory_schema_sql())
    trino.execute(sql.drop_table_sql(sql.STG_ORDERS))
    trino.execute(sql.build_stg_orders_sql())

    print("[2/4] staging postgresql.public.customer_segment -> memory.default.stg_customer_segment")
    trino.execute(sql.drop_table_sql(sql.STG_CUSTOMER_SEGMENT))
    trino.execute(sql.build_stg_customer_segment_sql())

    print("[3/4] federated join (dimension LEFT JOIN orders) -> memory.default.mart_orders_by_segment")
    trino.execute(sql.drop_table_sql(sql.MART_ORDERS_BY_SEGMENT))
    trino.execute(sql.build_mart_orders_by_segment_sql())
    rows = trino.query(sql.select_mart_sql())

    serializable = [_serialize_mart_row(r) for r in rows]
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(MART_JSON_PATH, "w") as f:
        json.dump(serializable, f, indent=2)

    print(f"      mart rows: {len(serializable)} (written to {MART_JSON_PATH})")
    for row in serializable:
        print("      ", row)

    # Inputs the checks reconcile against, read live from Trino.
    dim_rows = trino.query(sql.select_distinct_segment_region_sql())
    expected_keys = transforms.expected_segment_region_keys(dim_rows)
    staged_orders = trino.query(sql.count_stg_orders_sql())
    staged_orders_count = int(staged_orders[0]["n"]) if staged_orders else 0

    print("[4/4] data-quality checks (querying Trino, no stale-JSON fallback)")
    checks = {
        "non_empty": transforms.check_non_empty(serializable),
        "complete_segment_region": transforms.check_completeness(serializable, expected_keys),
        "referential_integrity": transforms.check_referential_integrity(serializable, expected_keys),
        "row_count": transforms.check_row_count_in_range(serializable, len(expected_keys)),
        "order_count_reconciles": transforms.check_order_count_reconciles(
            serializable, staged_orders_count
        ),
        "revenue_consistent": transforms.check_revenue_consistent(serializable),
    }
    all_passed = True
    for name, outcome in checks.items():
        status = "PASS" if outcome.passed else "FAIL"
        all_passed = all_passed and outcome.passed
        print(f"      [{status}] {name}: {outcome.metadata}")

    print("done." if all_passed else "done WITH CHECK FAILURES.")
    return 0 if all_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
