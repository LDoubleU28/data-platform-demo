"""Software-defined assets: raw -> staging -> marts, executed through Trino.

Lineage:

    tpch.tiny.orders ─────────────► stg_orders ──────┐
                                                      ├──► mart_orders_by_segment
    postgresql.public.customer_segment ► stg_customer_segment ┘

The mart is a federated join: the two staging tables originate in different
physical systems (TPCH connector and Postgres) and are joined by one engine.
"""

import json
import os

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
)

from . import sql, transforms
from .resources import TrinoResource

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
MART_JSON_PATH = os.path.join(DATA_DIR, "mart_orders_by_segment.json")


def _serialize_mart_row(r: dict) -> dict:
    """Normalize one Trino mart row for JSON, keeping money exact.

    JSON has no Decimal type, so money is emitted as a fixed-2 *string* rather
    than a lossy float. `avg_order_value` is NULL for zero-order cells (no
    orders to average), preserved as JSON null.
    """
    revenue = transforms.coerce_decimal(r.get("total_revenue"))
    avg = r.get("avg_order_value")
    return {
        "segment": r.get("segment"),
        "region": r.get("region"),
        "order_count": transforms.coerce_int(r.get("order_count")),
        "total_revenue": f"{revenue:.2f}",
        "avg_order_value": (
            None if avg is None else f"{transforms.coerce_decimal(avg):.2f}"
        ),
    }


@asset(group_name="staging", compute_kind="trino")
def stg_orders(context: AssetExecutionContext, trino: TrinoResource) -> MaterializeResult:
    """Stage synthetic TPCH orders into the memory catalog."""
    trino.execute(sql.ensure_memory_schema_sql())
    trino.execute(sql.drop_table_sql(sql.STG_ORDERS))
    trino.execute(sql.build_stg_orders_sql())
    rows = trino.query(f"SELECT COUNT(*) AS n FROM {sql.STG_ORDERS}")
    n = rows[0]["n"] if rows else 0
    context.log.info("staged %s orders", n)
    return MaterializeResult(metadata={"row_count": MetadataValue.int(int(n))})


@asset(group_name="staging", compute_kind="trino")
def stg_customer_segment(
    context: AssetExecutionContext, trino: TrinoResource
) -> MaterializeResult:
    """Stage the Postgres dimension table into the memory catalog (federated read)."""
    trino.execute(sql.ensure_memory_schema_sql())
    trino.execute(sql.drop_table_sql(sql.STG_CUSTOMER_SEGMENT))
    trino.execute(sql.build_stg_customer_segment_sql())
    rows = trino.query(f"SELECT COUNT(*) AS n FROM {sql.STG_CUSTOMER_SEGMENT}")
    n = rows[0]["n"] if rows else 0
    context.log.info("staged %s segment rows", n)
    return MaterializeResult(metadata={"row_count": MetadataValue.int(int(n))})


@asset(
    group_name="marts",
    compute_kind="trino",
    deps=[stg_orders, stg_customer_segment],
)
def mart_orders_by_segment(
    context: AssetExecutionContext, trino: TrinoResource
) -> MaterializeResult:
    """Federated join + aggregation; also emits result rows to data/*.json."""
    trino.execute(sql.drop_table_sql(sql.MART_ORDERS_BY_SEGMENT))
    trino.execute(sql.build_mart_orders_by_segment_sql())

    rows = trino.query(sql.select_mart_sql())

    serializable = [_serialize_mart_row(r) for r in rows]

    os.makedirs(DATA_DIR, exist_ok=True)
    with open(MART_JSON_PATH, "w") as f:
        json.dump(serializable, f, indent=2)

    preview = transforms.mart_preview(serializable)
    total_rev = transforms.total_revenue(serializable)
    return MaterializeResult(
        metadata={
            "row_count": MetadataValue.int(len(serializable)),
            # Money kept exact: format the Decimal to a fixed-2 string rather
            # than collapsing to a binary float.
            "total_revenue": MetadataValue.text(f"{total_rev:.2f}"),
            "json_path": MetadataValue.path(MART_JSON_PATH),
            "preview": MetadataValue.json(preview),
        }
    )
