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
    connection = utils.get_connection_details(connection_name, context)

    db_type = DbType(connection.get("type", "").lower())

    extra = {}

    if autocommit:
        extra["isolation_level"] = "AUTOCOMMIT"

    if db_type == DbType.ORACLE and connection.get("oracle_thick_mode", False):
        lib_dir = connection.get("oracle_thick_mode_lib_dir")
        extra["thick_mode"] = {'lib_dir': lib_dir} if lib_dir else {}

    engine = sa.create_engine(
        sa.engine.URL.create(
            drivername=connection.get("driver", DEFAULT_DRIVERS.get(db_type)),
            host=connection.get("host"),
            port=connection.get("port"),
            username=connection.get("user"),
            password=connection.get("password"),
            database=connection.get("database")),
        echo=connection.get("debug", False), connect_args=connection.get("connect_args", {}),
        **extra)

    return engine, db_type
