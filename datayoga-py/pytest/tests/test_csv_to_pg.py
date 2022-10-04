import logging

import sqlalchemy
from common.utils import run_job
from sqlalchemy import Column, Integer, String, Table
from sqlalchemy.orm import declarative_base
from testcontainers.postgres import PostgresContainer

logger = logging.getLogger("dy")


def test_csv_to_pg():
    postgres_container = PostgresContainer(
        dbname="postgres", user="postgres", password="postgres").with_bind_ports(
        5432, 5433)
    postgres_container.start()

    schema_name = "hr"
    engine = sqlalchemy.create_engine(postgres_container.get_connection_url())
    base = declarative_base()

    if not engine.dialect.has_schema(engine, schema_name):
        engine.execute(sqlalchemy.schema.CreateSchema(schema_name))

    columns = [
        Column("id", Integer, primary_key=True, nullable=False),
        Column("full_name", String(50)),
        Column("country", String(50)),
        Column("gender", String(1))
    ]
    Table("emp", base.metadata, *columns, schema=schema_name)

    base.metadata.create_all(engine)

    run_job("test_csv_to_pg.yaml")

    total_employees = engine.execute("select count(*) as total from hr.emp").fetchone()
    assert total_employees["total"] == 3

    first_employee = engine.execute("select * from hr.emp where id = 1").fetchone()

    assert first_employee["id"] == 1
    assert first_employee["full_name"] == "John Doe"
    assert first_employee["country"] == "972 - ISRAEL"
    assert first_employee["gender"] == "M"

    second_employee = engine.execute("select * from hr.emp where id = 2").fetchone()
    assert second_employee["id"] == 2
    assert second_employee["full_name"] == "Jane Doe"
    assert second_employee["country"] == "972 - ISRAEL"
    assert second_employee["gender"] == "F"

    third_employee = engine.execute("select * from hr.emp where id = 3").fetchone()
    assert third_employee["id"] == 3
    assert third_employee["full_name"] == "Bill Adams"
    assert third_employee["country"] == "1 - USA"
    assert third_employee["gender"] == "M"

    postgres_container.stop()
