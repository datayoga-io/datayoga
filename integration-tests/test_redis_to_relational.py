import logging

from common import db_utils, redis_utils
from common.utils import run_job
from sqlalchemy.engine import Engine

logger = logging.getLogger("dy")

REDIS_PORT = 12554
SCHEMA = "hr"


def test_redis_to_mysql():
    redis_container = redis_utils.get_redis_oss_container(REDIS_PORT)
    redis_container.start()

    redis_client = redis_utils.get_redis_client("localhost", REDIS_PORT)
    redis_utils.add_to_emp_stream(redis_client)

    mysql_container = db_utils.get_mysql_container("root", "hr", "my_user", "my_pass")
    mysql_container.start()

    engine = db_utils.get_engine(mysql_container)
    logger.warning(f"engine: {engine}")
    db_utils.create_emp_table(engine, SCHEMA)
    db_utils.insert_to_emp_table(engine, SCHEMA)

    run_job("tests.redis_to_mysql")

    check_results(engine)

    redis_container.stop()
    mysql_container.stop()


def test_redis_to_pg():
    redis_container = redis_utils.get_redis_oss_container(REDIS_PORT)
    redis_container.start()

    redis_client = redis_utils.get_redis_client("localhost", REDIS_PORT)
    redis_utils.add_to_emp_stream(redis_client)

    postgres_container = db_utils.get_postgres_container("postgres", "postgres", "postgres")
    postgres_container.start()

    engine = db_utils.get_engine(postgres_container)
    db_utils.create_schema(engine, SCHEMA)
    db_utils.create_emp_table(engine, SCHEMA)
    db_utils.insert_to_emp_table(engine, SCHEMA)

    run_job("tests.redis_to_pg")

    check_results(engine)

    redis_container.stop()
    postgres_container.stop()


def check_results(engine: Engine):
    total_employees = db_utils.select_one_row(engine, f"select count(*) as total from {SCHEMA}.emp")
    assert total_employees and total_employees["total"] == 3

    first_employee = db_utils.select_one_row(engine, f"select * from {SCHEMA}.emp where id = 1")
    assert first_employee is None

    second_employee = db_utils.select_one_row(engine, f"select * from {SCHEMA}.emp where id = 2")
    assert second_employee is not None
    assert second_employee["id"] == 2
    assert second_employee["full_name"] == "Jane Doe"
    assert second_employee["country"] == "972 - ISRAEL"
    assert second_employee["gender"] == "F"
    # address was not in the inserted record. verify that additional columns are set to null
    assert second_employee["address"] == None

    # address is not in the record. verify an upsert operation doesn't remove it
    third_employee = db_utils.select_one_row(engine, f"select * from {SCHEMA}.emp where id = 12")
    assert third_employee is not None
    assert third_employee["id"] == 12
    assert third_employee["full_name"] == "John Doe"
    assert third_employee["country"] == "972 - ISRAEL"
    assert third_employee["gender"] == "M"
    assert third_employee["address"] == "main street"
