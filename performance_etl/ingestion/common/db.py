"""
PostgreSQL connection and query helpers built on :mod:`psycopg2`.

Provides a :class:`DatabaseManager` that wraps a connection pool, generic
insert/upsert helpers (using ``psycopg2.extras.execute_values``), and simple
transaction support.
"""

from __future__ import annotations

import os
import socket
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator

import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool

from ingestion.common.logging import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Default pool sizing
# ---------------------------------------------------------------------------
_MIN_CONNECTIONS = 1
_MAX_CONNECTIONS = 5
_DEFAULT_APPLICATION_NAME = "performance_etl"
_POSTGRES_APPLICATION_NAME_MAX_LENGTH = 63


def _coerce_positive_int(value: Any) -> int | None:
    """Return a positive integer, or ``None`` when disabled/missing."""
    if value in (None, ""):
        return None
    parsed = int(value)
    return parsed if parsed > 0 else None


def _resolve_pool_size(
    config: dict[str, Any],
    *,
    requested_min_conn: int | None,
    requested_max_conn: int | None,
) -> tuple[int, int]:
    """Return sane pool sizing using explicit args or config defaults."""
    min_conn = _coerce_positive_int(requested_min_conn)
    if min_conn is None:
        min_conn = _coerce_positive_int(config.get("min_connections")) or _MIN_CONNECTIONS

    max_conn = _coerce_positive_int(requested_max_conn)
    if max_conn is None:
        max_conn = _coerce_positive_int(config.get("max_connections")) or _MAX_CONNECTIONS

    if max_conn < min_conn:
        max_conn = min_conn

    return min_conn, max_conn


def _build_application_name(config: dict[str, Any]) -> str:
    """Return a compact PostgreSQL application name for this process."""
    base_name = str(config.get("application_name") or _DEFAULT_APPLICATION_NAME).strip()
    if not base_name:
        base_name = _DEFAULT_APPLICATION_NAME

    dag_id = os.environ.get("AIRFLOW_CTX_DAG_ID")
    task_id = os.environ.get("AIRFLOW_CTX_TASK_ID")
    context = f"{dag_id}.{task_id}" if dag_id and task_id else None
    if context is None:
        script_name = Path(sys.argv[0]).stem.strip()
        context = script_name or None

    hostname = socket.gethostname().split(".", 1)[0]
    suffix = f":{hostname}:{os.getpid()}"
    prefix = f"{base_name}:{context}" if context else base_name

    max_prefix_length = max(
        1,
        _POSTGRES_APPLICATION_NAME_MAX_LENGTH - len(suffix),
    )
    if len(prefix) > max_prefix_length:
        prefix = prefix[:max_prefix_length]

    return f"{prefix}{suffix}"[:_POSTGRES_APPLICATION_NAME_MAX_LENGTH]


def _build_pg_options(config: dict[str, Any]) -> str | None:
    """Return startup ``SET`` options for psycopg2/libpq."""
    option_parts: list[str] = []
    for config_key, setting_name in (
        ("lock_timeout_ms", "lock_timeout"),
        ("statement_timeout_ms", "statement_timeout"),
        (
            "idle_in_transaction_session_timeout_ms",
            "idle_in_transaction_session_timeout",
        ),
    ):
        value = _coerce_positive_int(config.get(config_key))
        if value is not None:
            option_parts.append(f"-c {setting_name}={value}")
    return " ".join(option_parts) or None


class DatabaseManager:
    """Manage a PostgreSQL connection pool and expose query helpers.

    Args:
        config: Dict with keys ``host``, ``port``, ``dbname``, ``user``,
            ``password`` (as returned by :func:`ingestion.common.config.get_db_config`).
        min_conn: Minimum number of connections kept in the pool.
        max_conn: Maximum number of connections in the pool.
    """

    def __init__(
        self,
        config: dict[str, Any],
        min_conn: int | None = None,
        max_conn: int | None = None,
    ) -> None:
        application_name = _build_application_name(config)
        connect_timeout = _coerce_positive_int(
            config.get("connect_timeout_seconds")
        )
        options = _build_pg_options(config)
        resolved_min_conn, resolved_max_conn = _resolve_pool_size(
            config,
            requested_min_conn=min_conn,
            requested_max_conn=max_conn,
        )

        connect_kwargs: dict[str, Any] = {
            "host": config["host"],
            "port": config["port"],
            "dbname": config["dbname"],
            "user": config["user"],
            "password": config["password"],
            "application_name": application_name,
        }
        if connect_timeout is not None:
            connect_kwargs["connect_timeout"] = connect_timeout
        if options is not None:
            connect_kwargs["options"] = options

        self._pool = ThreadedConnectionPool(
            minconn=resolved_min_conn,
            maxconn=resolved_max_conn,
            **connect_kwargs,
        )
        logger.info(
            (
                "Database pool created "
                "(%s@%s:%s/%s app=%s connect_timeout_seconds=%s options=%s pool=%s-%s)"
            ),
            config["user"],
            config["host"],
            config["port"],
            config["dbname"],
            application_name,
            connect_timeout if connect_timeout is not None else "default",
            options or "none",
            resolved_min_conn,
            resolved_max_conn,
        )

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    def get_connection(self) -> psycopg2.extensions.connection:
        """Borrow a connection from the pool.

        The caller is responsible for returning it via :meth:`put_connection`
        or by using :meth:`connection` as a context manager.
        """
        return self._pool.getconn()

    def put_connection(self, conn: psycopg2.extensions.connection) -> None:
        """Return a connection to the pool, discarding it if it is broken.

        psycopg2 ``conn.closed`` values:
          0 = open and usable
          1 = closed cleanly by the client
          2 = broken by the server / network
        Broken or closed connections are removed from the pool rather than
        recycled, so the next caller always gets a live connection.
        """
        self._pool.putconn(conn, close=conn.closed != 0)

    @contextmanager
    def connection(self) -> Generator[psycopg2.extensions.connection, None, None]:
        """Context manager that borrows and returns a pooled connection.

        The connection is committed on clean exit and rolled back on exception.
        """
        conn = self.get_connection()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self.put_connection(conn)

    @contextmanager
    def cursor(
        self,
        conn: psycopg2.extensions.connection | None = None,
        cursor_factory: Any = None,
    ) -> Generator[psycopg2.extensions.cursor, None, None]:
        """Context manager for a cursor.  Borrows a connection if none supplied.

        Args:
            conn: Optional existing connection.
            cursor_factory: psycopg2 cursor factory (e.g. ``RealDictCursor``).
        """
        own_conn = conn is None
        if own_conn:
            conn = self.get_connection()
        try:
            cur = conn.cursor(cursor_factory=cursor_factory)
            yield cur
            if own_conn:
                conn.commit()
        except Exception:
            if own_conn:
                conn.rollback()
            raise
        finally:
            cur.close()
            if own_conn:
                self.put_connection(conn)

    # ------------------------------------------------------------------
    # Generic DML helpers
    # ------------------------------------------------------------------

    def insert_raw(self, table: str, data: dict[str, Any]) -> int:
        """Insert a single row and return the generated primary-key value.

        Assumes the first column of the table is a serial/identity PK named
        following the convention ``<singular_table>_id`` or simply ``id``.

        Args:
            table: Fully-qualified table name (e.g. ``"raw.catapult_activities"``).
            data: Column-name -> value mapping.

        Returns:
            The value of the ``RETURNING`` first column (typically ``raw_id``).
        """
        columns = list(data.keys())
        placeholders = ", ".join(["%s"] * len(columns))
        col_str = ", ".join(columns)
        sql = f"INSERT INTO {table} ({col_str}) VALUES ({placeholders}) RETURNING *"

        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, list(data.values()))
                row = cur.fetchone()
                return row[0] if row else None  # type: ignore[return-value]

    def insert_batch_raw(
        self,
        table: str,
        records: list[dict[str, Any]],
    ) -> list[int]:
        """Batch-insert rows using ``execute_values`` and return generated PKs.

        All dicts in *records* must share the same keys.

        Args:
            table: Fully-qualified table name.
            records: List of column-name -> value mappings.

        Returns:
            List of first-column values from ``RETURNING *``.
        """
        if not records:
            return []

        columns = list(records[0].keys())
        col_str = ", ".join(columns)
        template = "(" + ", ".join([f"%({c})s" for c in columns]) + ")"
        sql = f"INSERT INTO {table} ({col_str}) VALUES %s RETURNING *"

        with self.connection() as conn:
            with conn.cursor() as cur:
                result = psycopg2.extras.execute_values(
                    cur,
                    sql,
                    records,
                    template=template,
                    fetch=True,
                )
                return [row[0] for row in result]

    def upsert_bronze(
        self,
        table: str,
        data: dict[str, Any],
        conflict_columns: list[str],
        update_columns: list[str],
        conn: psycopg2.extensions.connection | None = None,
    ) -> None:
        """Insert a single row or update on conflict.

        Args:
            table: Fully-qualified table name.
            data: Column-name -> value mapping.
            conflict_columns: Columns that form the unique constraint.
            update_columns: Columns to overwrite on conflict.
        """
        columns = list(data.keys())
        placeholders = ", ".join(["%s"] * len(columns))
        col_str = ", ".join(columns)
        conflict_str = ", ".join(conflict_columns)
        update_str = ", ".join(
            [f"{c} = EXCLUDED.{c}" for c in update_columns]
        )

        sql = (
            f"INSERT INTO {table} ({col_str}) VALUES ({placeholders}) "
            f"ON CONFLICT ({conflict_str}) DO UPDATE SET {update_str}"
        )

        if conn is None:
            with self.connection() as managed_conn:
                with managed_conn.cursor() as cur:
                    cur.execute(sql, list(data.values()))
            return

        with conn.cursor() as cur:
            cur.execute(sql, list(data.values()))

    def upsert_batch_bronze(
        self,
        table: str,
        records: list[dict[str, Any]],
        conflict_columns: list[str],
        update_columns: list[str],
        conn: psycopg2.extensions.connection | None = None,
    ) -> None:
        """Batch upsert using ``execute_values``.

        Args:
            table: Fully-qualified table name.
            records: List of column-name -> value mappings.
            conflict_columns: Columns that form the unique constraint.
            update_columns: Columns to overwrite on conflict.
        """
        if not records:
            return

        columns = list(records[0].keys())
        col_str = ", ".join(columns)
        conflict_str = ", ".join(conflict_columns)
        update_str = ", ".join(
            [f"{c} = EXCLUDED.{c}" for c in update_columns]
        )
        template = "(" + ", ".join([f"%({c})s" for c in columns]) + ")"

        sql = (
            f"INSERT INTO {table} ({col_str}) VALUES %s "
            f"ON CONFLICT ({conflict_str}) DO UPDATE SET {update_str}"
        )

        if conn is None:
            with self.connection() as managed_conn:
                with managed_conn.cursor() as cur:
                    psycopg2.extras.execute_values(
                        cur,
                        sql,
                        records,
                        template=template,
                    )
            return

        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                sql,
                records,
                template=template,
            )

    # ------------------------------------------------------------------
    # Generic query helpers
    # ------------------------------------------------------------------

    def execute(
        self,
        sql: str,
        params: tuple[Any, ...] | None = None,
    ) -> None:
        """Execute an arbitrary SQL statement (no result set).

        Args:
            sql: SQL string with ``%s`` placeholders.
            params: Bind parameters.
        """
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)

    def fetch_one(
        self,
        sql: str,
        params: tuple[Any, ...] | None = None,
    ) -> tuple[Any, ...] | None:
        """Execute SQL and return the first row, or ``None``.

        Args:
            sql: SQL string.
            params: Bind parameters.

        Returns:
            A tuple for the first row, or ``None`` if no rows.
        """
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchone()

    def fetch_all(
        self,
        sql: str,
        params: tuple[Any, ...] | None = None,
    ) -> list[tuple[Any, ...]]:
        """Execute SQL and return all rows.

        Args:
            sql: SQL string.
            params: Bind parameters.

        Returns:
            List of tuples.
        """
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall()

    def fetch_one_dict(
        self,
        sql: str,
        params: tuple[Any, ...] | None = None,
    ) -> dict[str, Any] | None:
        """Execute SQL and return the first row as a dict, or ``None``.

        Uses ``RealDictCursor`` internally.
        """
        with self.connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
                return dict(row) if row else None

    def fetch_all_dict(
        self,
        sql: str,
        params: tuple[Any, ...] | None = None,
    ) -> list[dict[str, Any]]:
        """Execute SQL and return all rows as dicts.

        Uses ``RealDictCursor`` internally.
        """
        with self.connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                return [dict(r) for r in cur.fetchall()]

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close all connections in the pool."""
        self._pool.closeall()
        logger.info("Database pool closed.")

    def __enter__(self) -> "DatabaseManager":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
