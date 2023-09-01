from common import redis_utils
from common.utils import run_job

REDIS_PORT = 12554


def test_redis_lookup():
    redis_container = redis_utils.get_redis_oss_container(REDIS_PORT)
    redis_container.start()

    try:
        redis_client = redis_utils.get_redis_client("localhost", REDIS_PORT)

        redis_client.set("string_key", "test_string")
        redis_client.hset("hash_key", mapping={"tf0": "tv0", "tf1": "tv1"})
        redis_client.sadd("set_key", "tv0", "tv1", "tv2")
        redis_client.zadd("sorted_set_key", mapping={"tv0": "10", "tv1": "20"})
        redis_client.lpush("list_key", "tv0", "tv1", "tv2")

        run_job("tests.redis_lookup")

        not_exist_val = redis_client.hgetall("0")
        assert not_exist_val["obj"] == "None"

        string_val = redis_client.hgetall("1")
        assert string_val["obj"] == "test_string"

        hash_val = redis_client.hgetall("2")
        assert hash_val["obj"] == "{'tf0': 'tv0', 'tf1': 'tv1'}"

        set_val = redis_client.hgetall("3")
        assert "tv0" in set_val["obj"]
        assert "tv1" in set_val["obj"]
        assert "tv2" in set_val["obj"]

        sorted_set_val = redis_client.hgetall("4")
        assert sorted_set_val["obj"] == "['tv0', '10', 'tv1', '20']"

        list_val = redis_client.hgetall("5")
        assert list_val["obj"] == "['tv2', 'tv1', 'tv0']"

        assert len(redis_client.keys()) == 11
    finally:
        redis_container.stop()
