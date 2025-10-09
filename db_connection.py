import os
from typing import Any, Iterable, Optional, Tuple

import psycopg


def _get_env(key: str, default: Optional[str] = None) -> str:
    value = os.getenv(key, default)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {key}")
    return value


def get_connection() -> psycopg.Connection:  # type: ignore[name-defined]
    """
    Return a new psycopg (v3) connection using standard PG* env vars.

    Required env vars:
        - PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
    """
    host = _get_env("PGHOST")
    port = int(_get_env("PGPORT"))
    dbname = _get_env("PGDATABASE")
    user = _get_env("PGUSER")
    password = _get_env("PGPASSWORD")

    conn = psycopg.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        autocommit=True,
    )
    return conn


def execute_scalar(sql: str, params: Optional[Iterable[Any]] = None) -> Optional[Any]:
    """
    Execute a query expected to return a single value.
    Returns the first column of the first row, or None if no rows.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            row = cur.fetchone()
            return row[0] if row else None


def execute_rows(sql: str, params: Optional[Iterable[Any]] = None) -> Iterable[Tuple[Any, ...]]:
    """
    Execute a query and yield rows as tuples.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            for row in cur:
                yield row
def execute_sql_file(sql_file: str, params: Optional[Iterable[Any]] = None) -> None:
    """
    Execute sql file
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            

__all__ = [
    "get_connection",
    "execute_scalar",
    "execute_rows",
]


