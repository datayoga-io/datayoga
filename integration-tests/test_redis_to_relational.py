import logging
from contextlib import suppress

import pytest
from common import db_utils, redis_utils
from common.utils import run_job
from sqlalchemy.engine import Engine

logger = logging.getLogger("dy")
SCHEMA_NAME = "hr"

# sqlserver test fails due https://github.com/testcontainers/testcontainers-python/issues/285
# will be changed once this [1] PR is merged:
#
# [1] https://github.com/testcontainers/testcontainers-python/pull/286


@pytest.mark.parametrize("db_type",
                         ["db2", "mysql", "pg", "oracle", pytest.param("sqlserver", marks=pytest.mark.xfail)])
def test_redis_to_relational_db(db_type: str):
    """Reads data from a Redis stream and writes it to a relational database."""
    try:
        redis_container = redis_utils.get_redis_oss_container(redis_utils.REDIS_PORT)
        redis_container.start()

        if db_type == "db2":
            db_container = db_utils.get_db2_container("hr", "my_user", "my_pass")
        elif db_type == "mysql":
            db_container = db_utils.get_mysql_container("root", "hr", "my_user", "my_pass")
        elif db_type == "pg":
            db_container = db_utils.get_postgres_container("postgres", "postgres", "postgres")
        elif db_type == "oracle":
            db_container = db_utils.get_oracle_container()
        elif db_type == "sqlserver":
            db_container = db_utils.get_sqlserver_container("tempdb", "sa")
        else:
            raise ValueError(f"Unsupported database type: {db_type}")

        db_container.start()

        engine = db_utils.get_engine(db_container)
        db_utils.create_schema(engine, SCHEMA_NAME)
        db_utils.create_emp_table(engine, SCHEMA_NAME)
        db_utils.create_address_table(engine, SCHEMA_NAME)
        db_utils.insert_to_emp_table(engine, SCHEMA_NAME)
        db_utils.insert_to_address_table(engine, SCHEMA_NAME)

        run_job(f"tests.redis_to_{db_type}")
        check_results(engine, SCHEMA_NAME)
    finally:
        with suppress(Exception):
            redis_container.stop()
        with suppress(Exception):
            database_container.stop()


def check_results(engine: Engine, schema_name: str):
    # the first record was supposed to be deleted due to opcode=="d"
    total_employees = db_utils.select_one_row(engine, f"select count(*) as total from {schema_name}.emp")
    assert total_employees and total_employees["total"] == 3

    first_employee = db_utils.select_one_row(engine, f"select * from {schema_name}.emp where id = 1")
    assert first_employee is None

    second_employee = db_utils.select_one_row(engine, f"select * from {schema_name}.emp where id = 2")
    assert second_employee is not None
    assert second_employee["full_name"] == "Jane Doe"
    assert second_employee["country"] == "972 - ISRAEL"
    assert second_employee["gender"] == "F"
    # address was not in the inserted record. verify that additional columns are set to null
    assert second_employee["address"] is None

    # address is not in the record. verify an upsert operation doesn't remove it
    third_employee = db_utils.select_one_row(engine, f"select * from {schema_name}.emp where id = 12")
    assert third_employee is not None
    assert third_employee["full_name"] == "John Doe"
    assert third_employee["country"] == "972 - ISRAEL"
    assert third_employee["gender"] == "M"
    assert third_employee["address"] == "main street"

    total_addresses = db_utils.select_one_row(engine, f"select count(*) as total from {schema_name}.address")
    assert total_addresses and total_addresses["total"] == 3

    updated_address = db_utils.select_one_row(engine, f"select * from {schema_name}.address where id = 5")
    assert updated_address is not None
    assert updated_address["emp_id"] == 12
    assert updated_address["country_code"] == "IL"
    assert updated_address["address"] == "my address 5"

    new_address_1 = db_utils.select_one_row(engine, f"select * from {schema_name}.address where id = 3")
    assert new_address_1 is not None
    assert new_address_1["emp_id"] == 2
    assert new_address_1["country_code"] == "IL"
    assert new_address_1["address"] == "my address 3"

    new_address_2 = db_utils.select_one_row(engine, f"select * from {schema_name}.address where id = 4")
    assert new_address_2 is not None
    assert new_address_2["emp_id"] == 2
    assert new_address_2["country_code"] == "US"
    assert new_address_2["address"] == "my address 4"
