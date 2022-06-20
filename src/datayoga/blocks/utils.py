import sqlite3
from sqlite3 import Connection
from typing import Any


def get_connection() -> Connection:
    """Gets sqlite3 in memory connection

    Returns:
        Connection: sqlite3 connection
    """
    return sqlite3.connect(":memory")


def exec_sql(conn: Connection, fields: Any, expression: str) -> Any:
    """Executes an SQL statement

    Args:
        conn (Connection): Connection
        fields (Any): Fields
        expression (str): Expression

    Returns:
        Any: Query result
    """
    clauses = []
    for k, v in fields:
        clauses.append(f"'{v}' as '{k}'")

    from_clause = f"select {','.join(clauses)}"

    return conn.execute(f"select {expression} from ({from_clause})").fetchone()[0]
