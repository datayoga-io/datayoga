from typing import Any, Dict, Optional

import sqlalchemy
from sqlalchemy import Column, Integer, String, Table, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import declarative_base
from testcontainers.core.generic import (ADDITIONAL_TRANSIENT_ERRORS,
                                         DbContainer)
from testcontainers.core.waiting_utils import wait_container_is_ready
from testcontainers.mssql import SqlServerContainer
from testcontainers.mysql import MySqlContainer
from testcontainers.oracle import OracleDbContainer
from testcontainers.postgres import PostgresContainer


class Db2Container(DbContainer):
    def __init__(self, dbname: str, username: str, password: str, **kwargs):
        super(Db2Container, self).__init__(image="ibmcom/db2", **kwargs)
        self.with_bind_ports(50000, 50000)
        self.dbname = dbname
        self.username = username
        self.password = password

    def get_connection_url(self):
        port = self.get_exposed_port(50000)
        return f"ibm_db_sa://db2inst1:{port}/{self.dbname}"

    @wait_container_is_ready()
    def _connect(self):
        engine = sqlalchemy.create_engine(self.get_connection_url(), isolation_level="AUTOCOMMIT")
        engine.connect()

    def _configure(self):
        self.with_env("DB2INST1_PASSWORD", self.password)
        self.with_env("LICENSE", "accept")
        self.with_env("DBNAME", self.dbname)
        self.with_env("DB2INST1_USER", self.username)


def get_db2_container(db_name: str, db_user: str, db_password: str) -> Db2Container:
    return Db2Container(dbname=db_name, username=db_user, password=db_password)


def get_sqlserver_container(db_name: str, db_user: str, db_password: Optional[str] = None) -> SqlServerContainer:
    return SqlServerContainer(dbname=db_name, user=db_user, password=db_password).with_bind_ports(1433, 11433)


def get_mysql_container(mysql_root_password: str, db_name: str, db_user: str, db_password: str) -> MySqlContainer:
    """Runs MySQL as docker container.

    Args:
        mysql_root_password (str): The password of the root user.
        db_name (str): The name of the database.
        db_user (str): The user of the database.
        db_password (str): The password of the user.

    Returns:
        MySqlContainer: MySQL container
    """
    return MySqlContainer(
        MYSQL_ROOT_PASSWORD=mysql_root_password,
        MYSQL_DATABASE=db_name,
        MYSQL_USER=db_user,
        MYSQL_PASSWORD=db_password
    ).with_bind_ports(3306, 13306)


def get_postgres_container(db_name: str, db_user: str, db_password: str) -> PostgresContainer:
    return PostgresContainer(dbname=db_name, user=db_user, password=db_password).with_bind_ports(5432, 5433)


def get_oracle_container() -> OracleDbContainer:
    class FixedOracleDbContainer(OracleDbContainer):
        def get_connection_url(self):
            return super()._create_connection_url(
                dialect="oracle+oracledb", username="system", password="oracle", port=self.container_port,
                db_name="xe"
            )

        @wait_container_is_ready(*ADDITIONAL_TRANSIENT_ERRORS)
        def _connect(self):
            engine = sqlalchemy.create_engine(self.get_connection_url(), thick_mode={}, isolation_level="AUTOCOMMIT")
            engine.connect()

    return FixedOracleDbContainer().with_bind_ports(1521, 11521)


def get_engine(db_container: DbContainer) -> Engine:
    return sqlalchemy.create_engine(db_container.get_connection_url())


def create_schema(engine: Engine, schema_name: str):
    with engine.connect() as connection:
        with connection.begin():
            connection.execute(sqlalchemy.schema.CreateSchema(schema_name, if_not_exists=True))


def create_emp_table(engine: Engine, schema_name: str):
    base = declarative_base()

    columns = [
        Column("id", Integer, primary_key=True, nullable=False, autoincrement=False),
        Column("full_name", String(50)),
        Column("country", String(50)),
        Column("address", String(50)),
        Column("gender", String(1))
    ]
    Table("emp", base.metadata, *columns, schema=schema_name)

    base.metadata.create_all(engine)


def create_address_table(engine: Engine, schema_name: str):
    base = declarative_base()

    columns = [
        Column("id", Integer, primary_key=True, nullable=False, autoincrement=False),
        Column("emp_id", Integer),
        Column("country_code", String(2)),
        Column("address", String(100))
    ]
    Table("address", base.metadata, *columns, schema=schema_name)

    base.metadata.create_all(engine)


def insert_to_emp_table(engine: Engine, schema_name: str):
    with engine.connect() as connection:
        with connection.begin():
            connection.execute(text(
                f"INSERT INTO {schema_name}.emp (id, full_name, country, gender) VALUES (1, 'John Doe', '972 - ISRAEL', 'M')"))
            connection.execute(text(
                f"INSERT INTO {schema_name}.emp (id, full_name, country, gender) VALUES (10, 'john doe', '972 - ISRAEL', 'M')"))
            connection.execute(text(
                f"INSERT INTO {schema_name}.emp (id, full_name, country, gender, address) VALUES (12, 'steve steve', '972 - ISRAEL', 'M', 'main street')"))


def insert_to_address_table(engine: Engine, schema_name: str):
    with engine.connect() as connection:
        with connection.begin():
            connection.execute(text(
                f"INSERT INTO {schema_name}.address (id, emp_id, country_code, address) VALUES (1, 1, 'IL', 'my address 1')"))
            connection.execute(text(
                f"INSERT INTO {schema_name}.address (id, emp_id, country_code, address) VALUES (2, 1, 'US', 'my address 2')"))
            connection.execute(text(
                f"INSERT INTO {schema_name}.address (id, emp_id, country_code, address) VALUES (5, 12, 'US', 'my address 0')"))


def select_one_row(engine: Engine, query: str) -> Optional[Dict[str, Any]]:
    with engine.connect() as connection:
        row = connection.execute(text(query)).first()
        if row:
            return row._asdict()
