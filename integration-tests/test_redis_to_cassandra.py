import logging

import pytest
from common import cassandra_utils, redis_utils
from common.utils import run_job

logger = logging.getLogger("dy")


KEYSPACE = "hr"
TABLE = f"{KEYSPACE}.emp"


@pytest.fixture(scope="module")
def prepare_db():
    # pseudo code
    redis_container = redis_utils.get_redis_oss_container(redis_utils.REDIS_PORT)
    redis_container.start()

    redis_client = redis_utils.get_redis_client("localhost", redis_utils.REDIS_PORT)
    redis_utils.add_to_emp_stream(redis_client)

    cassandra_container = cassandra_utils.get_cassandra_container()
    cassandra_container.start()
    session = cassandra_utils.get_cassandra_session(["localhost"])

    cassandra_utils.create_keyspace(session, KEYSPACE)
    cassandra_utils.create_emp_table(session, KEYSPACE)
    cassandra_utils.insert_to_emp_table(session, KEYSPACE)

    run_job("tests.redis_to_cassandra")

    yield

    # cleanup
    redis_container.stop()
    cassandra_container.stop()


def test_total_records(prepare_db):
    session = cassandra_utils.get_cassandra_session(["localhost"])
    total_employees = session.execute(f"select count(*) as total from {TABLE}").one()
    assert total_employees.total == 3


def test_filtered_record(prepare_db):
    session = cassandra_utils.get_cassandra_session(["localhost"])
    first_employee = session.execute(f"select * from {TABLE} where id = 1").one()
    assert first_employee is None


def test_records(prepare_db):
    session = cassandra_utils.get_cassandra_session(["localhost"])
    second_employee = session.execute(f"select * from {TABLE} where id = 2").one()
    assert second_employee.id == 2
    assert second_employee.full_name == "Jane Doe"
    assert second_employee.country == "972 - ISRAEL"
    assert second_employee.gender == "F"

    second_employee = session.execute(f"select * from {TABLE} where id = 12").one()
    assert second_employee.id == 12
    assert second_employee.full_name == "John Doe"
    assert second_employee.country == "972 - ISRAEL"
    assert second_employee.gender == "M"
    assert second_employee.address == "main street"
