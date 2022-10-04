import json
import logging

import sqlalchemy
from common.utils import (get_postgres_container, get_redis_client,
                          get_redis_oss_container, run_job)
from sqlalchemy import Column, Integer, String, Table
from sqlalchemy.orm import declarative_base

logger = logging.getLogger("dy")

REDIS_PORT = 12554


def test_redis_to_pg():
    redis_container = get_redis_oss_container(REDIS_PORT)
    redis_container.start()

    redis_client = get_redis_client("localhost", REDIS_PORT)
    redis_client.xadd(
        "emp",
        {"message":
         json.dumps({"id": 1, "fname": "john", "lname": "doe", "country_code": 972, "country_name": "israel",
                     "credit_card": "1234-1234-1234-1234", "gender": "M"})})

    redis_client.xadd(
        "emp",
        {"message":
         json.dumps({"id": 2, "fname": "jane", "lname": "doe", "country_code": 972, "country_name": "israel",
                     "credit_card": "1000-2000-3000-4000", "gender": "F"})})

    postgres_container = get_postgres_container()
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

    run_job("test_redis_to_pg.yaml")

    total_employees = engine.execute("select count(*) as total from hr.emp").fetchone()
    assert total_employees["total"] == 2

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

    redis_container.stop()
    postgres_container.stop()
