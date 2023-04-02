import logging
from contextlib import suppress

from common import db_utils, redis_utils
from common.utils import run_job

logger = logging.getLogger("dy")

SCHEMA = "hr"

REDIS_PORT = 12554

def test_pg_to_redis():
    try:
        postgres_container = db_utils.get_postgres_container("postgres", "postgres", "postgres")
        postgres_container.start()

        redis_container = redis_utils.get_redis_oss_container(REDIS_PORT)
        redis_container.start()

        engine = db_utils.get_engine(postgres_container)
        db_utils.create_schema(engine, SCHEMA)
        db_utils.create_emp_table(engine, SCHEMA)
        db_utils.insert_to_emp_table(engine, SCHEMA)

        run_job("tests.pg_to_redis")

        redis_client = redis_utils.get_redis_client("localhost", REDIS_PORT)

        assert len(redis_client.keys()) == 3

        first_employee = redis_client.hgetall("1")
        assert first_employee== {"id":"1",
                                "full_name":"John Doe",
                                "country": "972 - ISRAEL",
                                "gender": "M"}

        second_employee = redis_client.hgetall("10")
        assert second_employee== {"id":"10",
                                "full_name":"john doe",
                                "country": "972 - ISRAEL",
                                "gender": "M"}

        third_employee = redis_client.hgetall("12")
        assert third_employee== {"id":"12",
                                "full_name":"steve steve",
                                "country": "972 - ISRAEL",
                                "gender": "M",
                                "address": "main street"}
    finally:
        with suppress(Exception):
            redis_container.stop()  # noqa
        with suppress(Exception):
            postgres_container.stop()  # noqa
