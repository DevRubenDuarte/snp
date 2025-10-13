import os
from typing import Optional

import psycopg

import dotenv

dotenv.load_dotenv()

def _get_env(key: str, default: Optional[str] = None) -> str:
    value = os.getenv(key, default)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {key}")
    return value

def get_connection() -> psycopg.Connection:
    """
    Return new psycopg connection

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

def execute_sql_file(sql_file_path: str) -> None:
    """
    Execute all SQL statements contained in a .sql file using a fresh connection.
    """
    abs_path = os.path.abspath(sql_file_path)
    if not os.path.isfile(abs_path):
        raise FileNotFoundError(f"SQL file not found: {sql_file_path}")
    with open(abs_path, "r", encoding="utf-8") as f:
        sql_text = f.read()
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    # psycopg can execute multiple statements in one execute when autocommit=True
                    cur.execute(sql_text)
        except psycopg.Error as e:
            print(f"Error executing SQL: {e}")
            raise


__all__ = [
    "get_connection",
    "execute_sql_file",
]


