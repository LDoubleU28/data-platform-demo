"""Trino connection resource.

Wraps the `trino` python DBAPI client in a Dagster ConfigurableResource so the
host/port/user/catalog are surfaced in the Dagster UI and overridable per run.
"""

from __future__ import annotations

import time
from typing import Any

from dagster import ConfigurableResource


class TrinoResource(ConfigurableResource):
    host: str = "localhost"
    port: int = 8080
    user: str = "demo"
    catalog: str = "memory"
    schema_: str = "default"
    # Retry the connect/first-query with backoff so running the pipeline a few
    # seconds before Trino finishes booting doesn't surface a raw socket error.
    connect_max_attempts: int = 10
    connect_backoff_seconds: float = 2.0

    def _connect(self):
        # Imported lazily so the module imports cleanly without the driver
        # present (e.g. during pure-logic test collection).
        from trino.dbapi import connect

        last_err: Exception | None = None
        for attempt in range(1, self.connect_max_attempts + 1):
            try:
                conn = connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    catalog=self.catalog,
                    schema=self.schema_,
                )
                # connect() is lazy; force a real round-trip so a not-yet-ready
                # server is caught here (and retried) rather than mid-statement.
                cur = conn.cursor()
                cur.execute("SELECT 1")
                cur.fetchall()
                return conn
            except Exception as err:  # noqa: BLE001 - retry transport errors, then re-raise
                last_err = err
                if attempt == self.connect_max_attempts:
                    break
                time.sleep(self.connect_backoff_seconds)
        raise ConnectionError(
            f"Trino not reachable at {self.host}:{self.port} after "
            f"{self.connect_max_attempts} attempts"
        ) from last_err

    def execute(self, sql: str) -> None:
        """Run a statement for its side effect (DDL / CTAS). Drains the cursor."""
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(sql)
            cur.fetchall()
        finally:
            conn.close()

    def query(self, sql: str) -> list[dict[str, Any]]:
        """Run a query and return rows as a list of dicts keyed by column name."""
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            columns = [d[0] for d in cur.description] if cur.description else []
            return [dict(zip(columns, row)) for row in rows]
        finally:
            conn.close()
