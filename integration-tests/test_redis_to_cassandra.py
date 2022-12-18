import logging

import common.cassandra as cassandra
import common.redis as redis
from common.utils import run_job

logger = logging.getLogger("dy")

REDIS_PORT = 12554
KEYSPACE = "hr"
TABLE = f"{KEYSPACE}.emp"


def test_redis_to_cassandra():
    redis_container = redis.get_redis_oss_container(REDIS_PORT)
    redis_container.start()

    redis_client = redis.get_redis_client("localhost", REDIS_PORT)
    redis.add_to_emp_stream(redis_client)

    cassandra_container = cassandra.get_cassandra_container()
    cassandra_container.start()
    session = cassandra.get_cassandra_session(["localhost"])

    cassandra.create_keyspace(session, KEYSPACE)
    cassandra.create_emp_table(session, KEYSPACE)
    cassandra.insert_to_emp_table(session, KEYSPACE)

    run_job("tests.redis_to_cassandra")

    total_employees = session.execute(f"select count(*) as total from {TABLE}").one()
    assert total_employees.total == 3

    first_employee = session.execute(f"select * from {TABLE} where id = 1").one()
    assert first_employee is None

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

    redis_container.stop()
    cassandra_container.stop()
