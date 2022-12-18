import logging

import common.pg as pg
import common.redis as redis
from common.utils import run_job

logger = logging.getLogger("dy")

REDIS_PORT = 12554
SCHEMA = "hr"


def test_redis_to_pg():
    redis_container = redis.get_redis_oss_container(REDIS_PORT)
    redis_container.start()

    redis_client = redis.get_redis_client("localhost", REDIS_PORT)
    redis.add_to_emp_stream(redis_client)

    postgres_container = pg.get_postgres_container()
    postgres_container.start()

    engine = pg.get_engine(postgres_container)
    pg.create_emp_table(engine, SCHEMA)
    pg.insert_to_emp_table(engine, SCHEMA)

    run_job("tests.redis_to_pg")

    total_employees = pg.select_one_row(engine, "select count(*) as total from hr.emp")
    assert total_employees["total"] == 3

    first_employee = pg.select_one_row(engine, f"select * from {SCHEMA}.emp where id = 1")
    assert first_employee is None

    second_employee = pg.select_one_row(engine, f"select * from {SCHEMA}.emp where id = 2")
    assert second_employee["id"] == 2
    assert second_employee["full_name"] == "Jane Doe"
    assert second_employee["country"] == "972 - ISRAEL"
    assert second_employee["gender"] == "F"

    second_employee = pg.select_one_row(engine, f"select * from {SCHEMA}.emp where id = 12")
    assert second_employee["id"] == 12
    assert second_employee["full_name"] == "John Doe"
    assert second_employee["country"] == "972 - ISRAEL"
    assert second_employee["gender"] == "M"
    assert second_employee["address"] == "main street"

    redis_container.stop()
    postgres_container.stop()
