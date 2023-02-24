import logging

from contextlib import suppress

import pytest
from common import db_utils, redis_utils
from common.utils import run_job
from datayoga_core.blocks import relational  # noqa
from sqlalchemy.engine import Engine

logger = logging.getLogger("dy")

REDIS_PORT = 12554


def test_redis_to_mysql():
    try:
        schema = "hr"

        redis_container = redis_utils.get_redis_oss_container(REDIS_PORT)
        redis_container.start()

        mysql_container = db_utils.get_mysql_container("root", "hr", "my_user", "my_pass")
        mysql_container.start()

        redis_utils.add_to_emp_stream(redis_utils.get_redis_client("localhost", REDIS_PORT))

        engine = db_utils.get_engine(mysql_container)
        db_utils.create_emp_table(engine, schema)
        db_utils.insert_to_emp_table(engine, schema)

        run_job("tests.redis_to_mysql")

        check_results(engine, schema)
    finally:
        with suppress(Exception):
            redis_container.stop()  # noqa
        with suppress(Exception):
            mysql_container.stop()  # noqa


def test_redis_to_pg():
    try:
        schema = "hr"

        redis_container = redis_utils.get_redis_oss_container(REDIS_PORT)
        redis_container.start()

        redis_utils.add_to_emp_stream(redis_utils.get_redis_client("localhost", REDIS_PORT))

        postgres_container = db_utils.get_postgres_container("postgres", "postgres", "postgres")
        postgres_container.start()

        engine = db_utils.get_engine(postgres_container)
        db_utils.create_schema(engine, schema)
        db_utils.create_emp_table(engine, schema)
        db_utils.insert_to_emp_table(engine, schema)

        run_job("tests.redis_to_pg")

        check_results(engine, schema)
    finally:
        with suppress(Exception):
            redis_container.stop()  # noqa
        with suppress(Exception):
            postgres_container.stop()  # noqa


def test_redis_to_oracle():
    try:
        schema = "hr"

        redis_container = redis_utils.get_redis_oss_container(REDIS_PORT)
        redis_container.start()

        redis_utils.add_to_emp_stream(redis_utils.get_redis_client("localhost", REDIS_PORT))

        oracle_container = db_utils.get_oracle_container()
        oracle_container.start()

        engine = db_utils.get_engine(oracle_container)
        db_utils.create_emp_table(engine, schema)
        db_utils.insert_to_emp_table(engine, schema)

        run_job("tests.redis_to_oracle")

        check_results(engine, schema)
    finally:
        with suppress(Exception):
            redis_container.stop()  # noqa
        with suppress(Exception):
            oracle_container.stop()  # noqa


@pytest.mark.xfail
# fails due https://github.com/testcontainers/testcontainers-python/issues/285
# will be changed once this [1] PR is merged:
#
# [1] https://github.com/testcontainers/testcontainers-python/pull/286
def test_redis_to_sqlserver():
    try:
        schema = "dbo"

        redis_container = redis_utils.get_redis_oss_container(REDIS_PORT)
        redis_container.start()

        redis_utils.add_to_emp_stream(redis_utils.get_redis_client("localhost", REDIS_PORT))

        sqlserver_container = db_utils.get_sqlserver_container("tempdb", "sa")
        sqlserver_container.start()

        engine = db_utils.get_engine(sqlserver_container)
        db_utils.create_emp_table(engine, schema)
        db_utils.insert_to_emp_table(engine, schema)

        run_job("tests.redis_to_sqlserver")

        check_results(engine, schema)
    finally:
        with suppress(Exception):
            redis_container.stop()  # noqa
        with suppress(Exception):
            sqlserver_container.stop()  # noqa


def check_results(engine: Engine, schema: str):
    # the first record was supposed to be delete due to opcode=="d"
    total_employees = db_utils.select_one_row(engine, f"select count(*) as total from {schema}.emp")
    assert total_employees and total_employees["total"] == 3

    first_employee = db_utils.select_one_row(engine, f"select * from {schema}.emp where id = 1")
    assert first_employee is None

    second_employee = db_utils.select_one_row(engine, f"select * from {schema}.emp where id = 2")
    assert second_employee is not None
    assert second_employee["id"] == 2
    assert second_employee["full_name"] == "Jane Doe"
    assert second_employee["country"] == "972 - ISRAEL"
    assert second_employee["gender"] == "F"
    # address was not in the inserted record. verify that additional columns are set to null
    assert second_employee["address"] == None

    # address is not in the record. verify an upsert operation doesn't remove it
    third_employee = db_utils.select_one_row(engine, f"select * from {schema}.emp where id = 12")
    assert third_employee is not None
    assert third_employee["id"] == 12
    assert third_employee["full_name"] == "John Doe"
    assert third_employee["country"] == "972 - ISRAEL"
    assert third_employee["gender"] == "M"
    assert third_employee["address"] == "main street"
