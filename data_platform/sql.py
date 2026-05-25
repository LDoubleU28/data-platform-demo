"""Pure SQL builders.

Every function here returns a SQL string and has no side effects and no
dependency on a live Trino connection, so they are unit-testable in isolation.

Naming convention for the in-flight ("memory") catalog tables:
    memory.default.stg_orders
    memory.default.stg_customer_segment
    memory.default.mart_orders_by_segment
"""

MEMORY_SCHEMA = "memory.default"

# Source locations.
TPCH_ORDERS = "tpch.tiny.orders"
PG_CUSTOMER_SEGMENT = "postgresql.public.customer_segment"

# Staging / mart targets.
STG_ORDERS = f"{MEMORY_SCHEMA}.stg_orders"
STG_CUSTOMER_SEGMENT = f"{MEMORY_SCHEMA}.stg_customer_segment"
MART_ORDERS_BY_SEGMENT = f"{MEMORY_SCHEMA}.mart_orders_by_segment"


def ensure_memory_schema_sql() -> str:
    """Create the memory schema if it does not yet exist."""
    return "CREATE SCHEMA IF NOT EXISTS memory.default"


def drop_table_sql(fqtn: str) -> str:
    return f"DROP TABLE IF EXISTS {fqtn}"


def build_stg_orders_sql() -> str:
    """Stage TPCH orders (built-in synthetic data) into the memory catalog.

    Restricts to custkey 1..50 so the rows align with the seeded Postgres
    dimension, keeping the demo dataset small and the federated join populated.
    """
    return f"""CREATE TABLE {STG_ORDERS} AS
SELECT
    orderkey,
    custkey,
    orderstatus,
    totalprice,
    orderdate
FROM {TPCH_ORDERS}
WHERE custkey BETWEEN 1 AND 50""".strip()


def build_stg_customer_segment_sql() -> str:
    """Stage the Postgres dimension table into the memory catalog.

    Reading through the `postgresql` catalog is the federated read; the row
    physically lives in Postgres and is pulled in by Trino at query time.
    """
    return f"""CREATE TABLE {STG_CUSTOMER_SEGMENT} AS
SELECT
    custkey,
    segment,
    region
FROM {PG_CUSTOMER_SEGMENT}""".strip()


def build_mart_orders_by_segment_sql() -> str:
    """Federated join: Postgres-sourced segments LEFT JOIN TPCH-sourced orders.

    Both inputs are already staged into `memory`, but the lineage they carry is
    cross-source: one engine (Trino), two physical systems (TPCH connector +
    Postgres). Aggregates order economics by segment and region.

    The join is LEFT from the segment dimension to orders so that *every*
    seeded segment x region cell appears in the mart, with `order_count` 0 and
    `total_revenue` 0 where a cell has no orders. An INNER join would silently
    drop order-less cells, leaving the mart incomplete while the old (weaker)
    checks still passed; the completeness check now gates exactly that.

    COALESCE pins the no-order cells to 0 revenue (SUM over no rows is NULL);
    avg_order_value stays NULL for those cells (no orders => no average).
    """
    return f"""CREATE TABLE {MART_ORDERS_BY_SEGMENT} AS
SELECT
    s.segment,
    s.region,
    COUNT(o.orderkey)                       AS order_count,
    COALESCE(SUM(o.totalprice), DECIMAL '0') AS total_revenue,
    AVG(o.totalprice)                       AS avg_order_value
FROM {STG_CUSTOMER_SEGMENT} s
LEFT JOIN {STG_ORDERS} o
    ON s.custkey = o.custkey
GROUP BY s.segment, s.region
ORDER BY s.segment, s.region""".strip()


def select_distinct_segment_region_sql() -> str:
    """Distinct (segment, region) cells the dimension defines: the expected universe."""
    return f"""SELECT DISTINCT segment, region
FROM {STG_CUSTOMER_SEGMENT}
ORDER BY segment, region""".strip()


def count_stg_orders_sql() -> str:
    """Row count of the staged orders fact; mart order_count must reconcile to this."""
    return f"SELECT COUNT(*) AS n FROM {STG_ORDERS}"


def select_mart_sql() -> str:
    """Read the materialized mart back out for metadata + downstream checks."""
    return f"""SELECT
    segment,
    region,
    order_count,
    total_revenue,
    avg_order_value
FROM {MART_ORDERS_BY_SEGMENT}
ORDER BY segment, region""".strip()
