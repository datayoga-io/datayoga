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


@pytest.mark.parametrize(
    "job_name",
    [("tests.redis_lookup_with_when_jmespath"),
     ("tests.redis_lookup_with_when_sql")])
def test_redis_lookup_with_when_condition(job_name: str):
    """Tests the functionality of Redis lookup with when condition.

    This test verifies that the when condition properly skips lookups for records
    where the condition evaluates to false (e.g., when branch_id is null).

    Args:
        job_name (str): The name of the job to run.

    Raises:
        AssertionError: If the lookup behavior doesn't match expectations.
    """
    redis_container = redis_utils.get_redis_oss_container(redis_utils.REDIS_PORT)
    redis_container.start()

    try:
        redis_client = redis_utils.get_redis_client("localhost", redis_utils.REDIS_PORT)

        # Set up test data in Redis
        redis_client.set("string_key", "test_string")
        redis_client.set("hash_key", "test_hash")
        redis_client.set("set_key", "test_set")
        redis_client.set("sorted_set_key", "test_sorted_set")
        redis_client.set("list_key", "test_list")

        run_job(job_name)

        # Verify records with branch_id (should have lookup results)
        val_1 = redis_client.hgetall("1")
        assert val_1["obj"] == "test_string"
        assert val_1["branch_id"] == "branch_1"

        val_3 = redis_client.hgetall("3")
        assert val_3["obj"] == "test_set"
        assert val_3["branch_id"] == "branch_3"

        val_4 = redis_client.hgetall("4")
        assert val_4["obj"] == "test_sorted_set"
        assert val_4["branch_id"] == "branch_4"

        # Verify records without branch_id (should have None for obj since lookup was skipped)
        val_0 = redis_client.hgetall("0")
        assert val_0["obj"] == "None"
        assert val_0["branch_id"] == ""

        val_2 = redis_client.hgetall("2")
        assert val_2["obj"] == "None"
        assert val_2["branch_id"] == ""

        val_5 = redis_client.hgetall("5")
        assert val_5["obj"] == "None"
        assert val_5["branch_id"] == ""

        # Total keys: 5 original test data + 6 result records = 11
        assert len(redis_client.keys()) == 11
    finally:
        redis_container.stop()
