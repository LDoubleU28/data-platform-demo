"""Unit tests for the pure SQL builders. No live Trino required."""

from data_platform import sql


def test_stg_orders_reads_tpch_and_writes_memory():
    q = sql.build_stg_orders_sql()
    assert sql.TPCH_ORDERS in q
    assert q.startswith(f"CREATE TABLE {sql.STG_ORDERS} AS")
    assert "custkey BETWEEN 1 AND 50" in q


def test_stg_customer_segment_reads_postgres():
    q = sql.build_stg_customer_segment_sql()
    assert sql.PG_CUSTOMER_SEGMENT in q
    assert q.startswith(f"CREATE TABLE {sql.STG_CUSTOMER_SEGMENT} AS")


def test_mart_is_a_left_join_from_dimension_to_orders():
    q = sql.build_mart_orders_by_segment_sql()
    # The federated lineage: both staging tables appear in one join.
    assert sql.STG_ORDERS in q
    assert sql.STG_CUSTOMER_SEGMENT in q
    # LEFT JOIN *from the dimension* so every seeded cell survives.
    assert "LEFT JOIN" in q
    # Dimension is the left (driving) side; orders are the optional right side.
    dim_pos = q.index(sql.STG_CUSTOMER_SEGMENT)
    orders_pos = q.index(sql.STG_ORDERS)
    assert dim_pos < orders_pos, "dimension must be the left/driving table"
    assert "ON s.custkey = o.custkey" in q
    assert "GROUP BY s.segment, s.region" in q
    # No-order cells must report 0 revenue, not NULL.
    assert "COALESCE(SUM(o.totalprice)" in q


def test_select_distinct_segment_region_targets_the_dimension():
    q = sql.select_distinct_segment_region_sql()
    assert sql.STG_CUSTOMER_SEGMENT in q
    assert "DISTINCT" in q.upper()


def test_count_stg_orders_targets_staged_orders():
    q = sql.count_stg_orders_sql()
    assert sql.STG_ORDERS in q
    assert "COUNT(*)" in q.upper()


def test_select_mart_targets_the_mart_table():
    q = sql.select_mart_sql()
    assert sql.MART_ORDERS_BY_SEGMENT in q
    assert q.upper().startswith("SELECT")


def test_drop_and_schema_helpers():
    assert sql.drop_table_sql("a.b.c") == "DROP TABLE IF EXISTS a.b.c"
    assert "CREATE SCHEMA IF NOT EXISTS" in sql.ensure_memory_schema_sql()


def test_memory_table_names_share_memory_schema():
    for fqtn in (sql.STG_ORDERS, sql.STG_CUSTOMER_SEGMENT, sql.MART_ORDERS_BY_SEGMENT):
        assert fqtn.startswith("memory.default.")
