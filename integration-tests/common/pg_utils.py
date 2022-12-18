

from typing import Optional

import sqlalchemy
from sqlalchemy import Column, Integer, String, Table
from sqlalchemy.engine import Engine
from sqlalchemy.engine.row import Row
from sqlalchemy.orm import declarative_base
from testcontainers.postgres import PostgresContainer


def get_postgres_container() -> PostgresContainer:
    return PostgresContainer(dbname="postgres", user="postgres", password="postgres").with_bind_ports(5432, 5433)


def get_engine(postgres_container: PostgresContainer) -> Engine:
    return sqlalchemy.create_engine(postgres_container.get_connection_url())


def create_schema(engine: Engine, schema_name: str):
    if not engine.dialect.has_schema(engine, schema_name):
        engine.execute(sqlalchemy.schema.CreateSchema(schema_name))


def create_emp_table(engine: Engine, schema_name: str):
    create_schema(engine, schema_name)

    base = declarative_base()

    columns = [
        Column("id", Integer, primary_key=True, nullable=False),
        Column("full_name", String(50)),
        Column("country", String(50)),
        Column("address", String(50)),
        Column("gender", String(1))
    ]
    Table("emp", base.metadata, *columns, schema=schema_name)

    base.metadata.create_all(engine)


def insert_to_emp_table(engine: Engine, schema_name: str):
    engine.execute(
        f"INSERT INTO {schema_name}.emp (id, full_name, country, gender) VALUES (1, 'John Doe', '972 - ISRAEL', 'M')")
    engine.execute(
        f"INSERT INTO {schema_name}.emp (id, full_name, country, gender) VALUES (10, 'john doe', '972 - ISRAEL', 'M')")
    engine.execute(
        f"INSERT INTO {schema_name}.emp (id, full_name, country, gender, address) VALUES (12, 'steve steve', '972 - ISRAEL', 'M', 'main street')")


def select_one_row(engine: Engine, query: str) -> Optional[Row]:
    return engine.execute(query).fetchone()
