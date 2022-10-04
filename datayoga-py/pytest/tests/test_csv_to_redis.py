from common.utils import get_redis_client, get_redis_oss_container, run_job

REDIS_PORT = 12554


def test_csv_to_redis():
    redis_container = get_redis_oss_container(REDIS_PORT)
    redis_container.start()
    run_job("test_csv_to_redis.yaml")

    redis_client = get_redis_client("localhost", REDIS_PORT)

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