from common import redis_utils
from common.utils import run_job


def test_csv_to_redis():
    redis_container = redis_utils.get_redis_oss_container(redis_utils.REDIS_PORT)
    redis_container.start()
    run_job("tests.csv_to_redis")

    redis_client = redis_utils.get_redis_client("localhost", redis_utils.REDIS_PORT)

    assert len(redis_client.keys()) == 3

    first_employee = redis_client.hgetall("1")
    assert first_employee["id"] == "1"
    assert first_employee["full_name"] == "John Doe"
    assert first_employee["country"] == "972 - ISRAEL"
    assert first_employee["gender"] == "M"

    second_employee = redis_client.hgetall("2")
    assert second_employee["id"] == "2"
    assert second_employee["full_name"] == "Jane Doe"
    assert second_employee["country"] == "972 - ISRAEL"
    assert second_employee["gender"] == "F"

    third_employee = redis_client.hgetall("3")
    assert third_employee["id"] == "3"
    assert third_employee["full_name"] == "Bill Adams"
    assert third_employee["country"] == "1 - USA"
    assert third_employee["gender"] == "M"

    redis_container.stop()
