import pytest
from common import redis_utils
from common.utils import run_job


@pytest.mark.parametrize(
    "job_name, key, expected",
    [("tests.redis_lookup_string", "0", "None"),
     ("tests.redis_lookup_string", "1", "test_string"),
     ("tests.redis_lookup_hash", "2", "{'tf0': 'tv0', 'tf1': 'tv1'}"),
     ("tests.redis_lookup_set", "3", "{'tv0'}"),
     ("tests.redis_lookup_sorted_set", "4", "['tv0', '10', 'tv1', '20']"),
     ("tests.redis_lookup_list", "5", "['tv2', 'tv1', 'tv0']"),
     ("tests.redis_lookup_string_nested", "1", "{'a': {'b': {'c.d': 'test_string'}}}")])
def test_redis_lookup(job_name: str, key: str, expected: str):
    """Tests the functionality of Redis lookup operations.

    Args:
        job_name (str): The name of the job to run.
        key (str): The Redis key to lookup.
        expected (str): The expected value after the Redis lookup.

    Raises:
        AssertionError: If the retrieved value from Redis does not match the expected value.
    """
    redis_container = redis_utils.get_redis_oss_container(redis_utils.REDIS_PORT)
    redis_container.start()

    try:
        redis_client = redis_utils.get_redis_client("localhost", redis_utils.REDIS_PORT)

        redis_client.set("string_key", "test_string")
        redis_client.hset("hash_key", mapping={"tf0": "tv0", "tf1": "tv1"})
        redis_client.sadd("set_key", "tv0")
        redis_client.zadd("sorted_set_key", mapping={"tv0": "10", "tv1": "20"})
        redis_client.lpush("list_key", "tv0", "tv1", "tv2")

        run_job(job_name)

        val = redis_client.hgetall(key)
        assert val["obj"] == expected

        assert len(redis_client.keys()) == 7
    finally:
        redis_container.stop()
