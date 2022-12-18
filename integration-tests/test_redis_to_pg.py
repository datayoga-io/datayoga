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
         json.dumps({"_id": 1, "fname": "john", "lname": "doe", "country_code": 972, "country_name": "israel",
                     "credit_card": "1234-1234-1234-1234", "gender": "M", "__$$opcode": "d"})})

    redis_client.xadd(
        "emp",
        {"message":
         json.dumps({"_id": 2, "fname": "jane", "lname": "doe", "country_code": 972, "country_name": "israel",
                     "credit_card": "1000-2000-3000-4000", "gender": "F", "__$$opcode": "u"})})

    redis_client.xadd(
        "emp",
        {"message":
         json.dumps({"_id": 12, "fname": "john", "lname": "doe", "country_code": 972, "country_name": "israel",
                     "credit_card": "1234-1234-1234-1234", "gender": "M", "__$$opcode": "u"})})

    postgres_container = pg.get_postgres_container()
    postgres_container.start()

    engine = pg.get_engine(postgres_container)
    pg.create_emp_table(engine, "hr")
    engine.execute("INSERT INTO hr.emp (id, full_name, country, gender) VALUES (1, 'John Doe', '972 - ISRAEL', 'M')")
    engine.execute("INSERT INTO hr.emp (id, full_name, country, gender) VALUES (10, 'john doe', '972 - ISRAEL', 'M')")
    engine.execute(
        "INSERT INTO hr.emp (id, full_name, country, gender, address) VALUES (12, 'steve steve', '972 - ISRAEL', 'M', 'main street')")

    run_job("tests.redis_to_pg")

    total_employees = pg.select_one_row(engine, "select count(*) as total from hr.emp")
    assert total_employees["total"] == 3

    first_employee = pg.select_one_row(engine, "select * from hr.emp where id = 1")
    assert first_employee is None

    second_employee = pg.select_one_row(engine, "select * from hr.emp where id = 2")
    assert second_employee["id"] == 2
    assert second_employee["full_name"] == "Jane Doe"
    assert second_employee["country"] == "972 - ISRAEL"
    assert second_employee["gender"] == "F"

    second_employee = pg.select_one_row(engine, "select * from hr.emp where id = 12")
    assert second_employee["id"] == 12
    assert second_employee["full_name"] == "John Doe"
    assert second_employee["country"] == "972 - ISRAEL"
    assert second_employee["gender"] == "M"
    assert second_employee["address"] == "main street"


    redis_container.stop()
    postgres_container.stop()
