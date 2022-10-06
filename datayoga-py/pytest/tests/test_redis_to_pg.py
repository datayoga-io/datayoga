import json
import logging

import common.pg as pg
import common.redis as redis
from common.utils import run_job

logger = logging.getLogger("dy")

REDIS_PORT = 12554


def test_redis_to_pg():
    redis_container = redis.get_redis_oss_container(REDIS_PORT)
    redis_container.start()

    redis_client = redis.get_redis_client("localhost", REDIS_PORT)
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

    postgres_container = pg.get_postgres_container()
    postgres_container.start()

    engine = pg.get_engine(postgres_container)
    pg.create_emp_table(engine, "hr")

    run_job("test_redis_to_pg.yaml")

    total_employees = pg.select_one_row(engine, "select count(*) as total from hr.emp")
    assert total_employees["total"] == 2

    first_employee = pg.select_one_row(engine, "select * from hr.emp where id = 1")

    assert first_employee["id"] == 1
    assert first_employee["full_name"] == "John Doe"
    assert first_employee["country"] == "972 - ISRAEL"
    assert first_employee["gender"] == "M"

    second_employee = pg.select_one_row(engine, "select * from hr.emp where id = 2")
    assert second_employee["id"] == 2
    assert second_employee["full_name"] == "Jane Doe"
    assert second_employee["country"] == "972 - ISRAEL"
    assert second_employee["gender"] == "F"

    redis_container.stop()
    postgres_container.stop()
