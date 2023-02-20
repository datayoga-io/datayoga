from typing import Any, Dict, Optional

import sqlalchemy
from sqlalchemy import Column, Integer, String, Table, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import declarative_base
from testcontainers.core.generic import DbContainer
from testcontainers.mssql import SqlServerContainer
from testcontainers.mysql import MySqlContainer
from testcontainers.oracle import OracleDbContainer
from testcontainers.postgres import PostgresContainer


def get_mssql_container(db_name: str, db_user: str, db_password: Optional[str] = None) -> SqlServerContainer:
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
    return OracleDbContainer().with_bind_ports(1521, 11521)


def get_engine(db_container: DbContainer) -> Engine:
    return sqlalchemy.create_engine(db_container.get_connection_url())


def create_schema(engine: Engine, schema_name: str):
    with engine.connect() as connection:
        if not engine.dialect.has_schema(connection, schema_name):
            connection.execute(sqlalchemy.schema.CreateSchema(schema_name))


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


def insert_to_emp_table(engine: Engine, schema_name: str):
    with engine.connect() as connection:
        connection.execute(text(
            f"INSERT INTO {schema_name}.emp (id, full_name, country, gender) VALUES (1, 'John Doe', '972 - ISRAEL', 'M')"))
        connection.execute(text(
            f"INSERT INTO {schema_name}.emp (id, full_name, country, gender) VALUES (10, 'john doe', '972 - ISRAEL', 'M')"))
        connection.execute(text(
            f"INSERT INTO {schema_name}.emp (id, full_name, country, gender, address) VALUES (12, 'steve steve', '972 - ISRAEL', 'M', 'main street')"))


def select_one_row(engine: Engine, query: str) -> Optional[Dict[str, Any]]:
    with engine.connect() as connection:
        row = connection.execute(text(query)).first()
        if row:
            return row._asdict()
