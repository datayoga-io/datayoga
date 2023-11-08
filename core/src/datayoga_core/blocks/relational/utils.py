import logging
from enum import Enum, unique
from typing import Tuple

import sqlalchemy as sa
from datayoga_core import utils
from datayoga_core.context import Context

logger = logging.getLogger("dy")


@unique
class DbType(str, Enum):
    MYSQL = "mysql"
    ORACLE = "oracle"
    PSQL = "postgresql"
    SQLSERVER = "sqlserver"


DEFAULT_DRIVERS = {
    DbType.MYSQL: "mysql+pymysql",
    DbType.ORACLE: "oracle+oracledb",
    DbType.PSQL: "postgresql",
    DbType.SQLSERVER: "mssql+pymssql"
}


def get_engine(connection_name: str, context: Context, autocommit: bool = True) -> Tuple[sa.engine.Engine, DbType]:
    connection_details = utils.get_connection_details(connection_name, context)

    db_type = DbType(connection_details.get("type", "").lower())

    query_args = connection_details.get("query_args", {})
    connect_args = connection_details.get("connect_args", {})
    extra = {}

    if autocommit:
        extra["isolation_level"] = "AUTOCOMMIT"

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
        **extra)

    return engine, db_type
