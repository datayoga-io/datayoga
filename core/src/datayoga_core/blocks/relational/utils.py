import logging
from enum import Enum, unique
from typing import Tuple

import sqlalchemy as sa
from datayoga_core import utils
from datayoga_core.context import Context

logger = logging.getLogger("dy")


@unique
class DbType(str, Enum):
    MSSQL = "mssql"
    MYSQL = "mysql"
    PSQL = "postgresql"


DEFAULT_DRIVERS = {
    DbType.MYSQL: "mysql+pymysql",
    DbType.MSSQL: "mssql+pymssql",
    DbType.PSQL: "postgresql"
}


def get_engine(connection_name: str, context: Context) -> Tuple[sa.engine.Engine, DbType]:
    connection = utils.get_connection_details(connection_name, context)

    db_type = DbType(connection.get("type", "").lower())

    engine = sa.create_engine(
        sa.engine.URL.create(
            drivername=connection.get("driver", DEFAULT_DRIVERS.get(db_type)),
            host=connection.get("host"),
            port=connection.get("port"),
            username=connection.get("user"),
            password=connection.get("password"),
            database=connection.get("database")),
        echo=connection.get("debug", False), connect_args=connection.get("connect_args", {}))

    return engine, db_type
