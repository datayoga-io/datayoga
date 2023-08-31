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

        run_job("tests.redis_lookup_string")

        string_key = redis_client.get("not_exist_key")
        assert string_key == "xxx"

        string_key = redis_client.get("string_key")
        assert string_key == "xxx"

        string_key = redis_client.get("hash_key")
        assert string_key == "xxx"

        string_key = redis_client.get("set_key")
        assert string_key == "xxx"

        string_key = redis_client.get("sorted_set_key")
        assert string_key == "xxx"

        string_key = redis_client.get("list_key")
        assert string_key == "xxx"

        assert len(redis_client.keys()) == 11
    finally:
        redis_container.stop()
