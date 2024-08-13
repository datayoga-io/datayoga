import logging
from enum import Enum, unique
from typing import Tuple

import sqlalchemy as sa
from datayoga_core.connection import Connection
from datayoga_core.context import Context
from sqlalchemy.engine.interfaces import DBAPIConnection
from sqlalchemy.pool import ConnectionPoolEntry

logger = logging.getLogger("dy")


@unique
class DbType(str, Enum):
    DB2 = "db2"
    MYSQL = "mysql"
    ORACLE = "oracle"
    PSQL = "postgresql"
    SQLSERVER = "sqlserver"


DEFAULT_DRIVERS = {
    DbType.DB2: "ibm_db_sa",
    DbType.MYSQL: "mysql+pymysql",
    DbType.ORACLE: "oracle+oracledb",
    DbType.PSQL: "postgresql",
    DbType.SQLSERVER: "mssql+pymssql"
}


def get_engine(connection_name: str, context: Context, autocommit: bool = True) -> Tuple[sa.engine.Engine, DbType]:
    """Creates an SQLAlchemy engine based on the connection details specified in the context.

    Args:
        connection_name (str): The name of the connection to retrieve details for.
        context (Context): The context object providing necessary configurations and settings.
        autocommit (bool, optional): Indicates whether to set the engine's isolation level to autocommit.
            Defaults to True.

    Raises:
        ValueError: If the connection details are invalid or missing required fields.

    Returns:
        Tuple[sa.engine.Engine, DbType]: A tuple containing the created SQLAlchemy engine and the database type.
    """
    connection_details = Connection.get_connection_details(connection_name, context)

    db_type = DbType(connection_details.get("type", "").lower())

    query_args = connection_details.get("query_args", {})
    connect_args = connection_details.get("connect_args", {})
    extra = {}

    if autocommit:
        extra["isolation_level"] = None if db_type == DbType.DB2 else "AUTOCOMMIT"

    if db_type == DbType.ORACLE and connection_details.get("oracle_thick_mode", False):
        lib_dir = connection_details.get("oracle_thick_mode_lib_dir")
        extra["thick_mode"] = {"lib_dir": lib_dir} if lib_dir else {}

    engine = sa.create_engine(
        sa.engine.URL.create(
            drivername=connection_details.get("driver", DEFAULT_DRIVERS.get(db_type)),
            host=connection_details.get("host"),
            port=connection_details.get("port"),
            username=connection_details.get("user"),
            password=connection_details.get("password"),
            database=connection_details.get("database"),
            query=query_args),
        echo=connection_details.get("debug", False),
        connect_args=connect_args,
        pool_pre_ping=True,
        **extra)

    if db_type == DbType.ORACLE:
        # add NLS_DATE_FORMAT for Oracle to explicitly set the date format to ISO
        sa.event.listen(engine, "connect", alter_session_on_oracle_connect)

    return engine, db_type


def alter_session_on_oracle_connect(dbapi_connection: DBAPIConnection, connection_record: ConnectionPoolEntry):
    """SQLAlchemy event listener to alter the Oracle session settings upon connection.

    This callback function is intended to be used with SQLAlchemy's `connect` event.
    It alters the Oracle session settings by setting the `NLS_DATE_FORMAT` for the session
    to ensure that dates are formatted as 'YYYY-MM-DD HH24:MI:SS'.

    Args:
        dbapi_connection (DBAPIConnection): The raw DB-API connection object provided by the Oracle driver.
        connection_record (ConnectionPoolEntry): A record associated with the connection, managed by SQLAlchemy.
    """
    cursor = dbapi_connection.cursor()
    cursor.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'")
    cursor.close()


def construct_table_reference(table: sa.Table, with_brackets: bool = False) -> str:
    """Constructs a table reference string.

    Args:
        table (sa.Table): The SQLAlchemy Table object.
        with_brackets (bool, optional): Whether to include brackets around schema and table names or not.

    Returns:
        str: The formatted table reference string.
    """
    schema_prefix = ""
    if table.schema:
        schema_prefix = f"[{table.schema}]." if with_brackets else f"{table.schema}."
    table_name = f"[{table.name}]" if with_brackets else table.name
    return f"{schema_prefix}{table_name}"
