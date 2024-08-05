import json
from typing import Optional

import redis
from redis import Redis
from testcontainers.redis import RedisContainer

REDIS_PORT = 12554


def get_redis_client(host: str, port: int, password: Optional[str] = None) -> Redis:
    """Establishes a connection to a Redis server.

    Args:
        host (str): The Redis server hostname or IP address.
        port (int): The Redis server port number.
        password (Optional[str], optional): Redis password. Defaults to None.

    Returns:
        Redis: Redis client instance.

    Raises:
        ValueError: If connection to Redis fails.
    """
    try:
        client = redis.Redis(
            host=host,
            port=port,
            password=password,
            decode_responses=True,
            client_name="datayoga"
        )

        client.ping()
        return client
    except Exception as e:
        raise ValueError(f"can not connect to Redis on {host}:{port}:\n {e}")


def get_redis_oss_container(redis_port: int, redis_password: Optional[str] = None) -> RedisContainer:
    """Creates a Redis container for testing purposes.

    Args:
        redis_port (int): The port to bind the container's Redis server.
        redis_password (Optional[str], optional): Redis password. Defaults to None.

    Returns:
        RedisContainer: Redis container instance.
    """
    return RedisContainer(password=redis_password).with_bind_ports(6379, redis_port)


def add_to_emp_stream(redis_client: Redis):
    """Adds records to the 'emp' Redis stream using XADD command.

    Args:
        redis_client (Redis): Redis client instance.
    """
    records = [
        {
            "_id": 1,
            "fname": "john",
            "lname": "doe",
            "country_code": 972,
            "country_name": "israel",
            "credit_card": "1234-1234-1234-1234",
            "gender": "M",
            "addresses": [
                {"id": 1, "country_code": "IL", "address": "my address 1"},
                {"id": 2, "country_code": "US", "address": "my address 2"}
            ],
            "__$$opcode": "d"
        },
        {
            "_id": 2,
            "fname": "jane",
            "lname": "doe",
            "country_code": 972,
            "country_name": "israel",
            "credit_card": "1000-2000-3000-4000",
            "gender": "F",
            "addresses": [
                {"id": 3, "country_code": "IL", "address": "my address 3"},
                {"id": 4, "country_code": "US", "address": "my address 4"}
            ],
            "__$$opcode": "u"
        },
        # gender length is invalid
        {
            "_id": 11,
            "fname": "jane",
            "lname": "doe",
            "country_code": 972,
            "country_name": "israel",
            "credit_card": "1000-2000-3000-4000",
            "gender": "FF",
            "addresses": [
                {"id": 33, "country_code": "IL", "address": "my address 33"},
                {"id": 44, "country_code": "US", "address": "my address 44"}
            ],
            "__$$opcode": "u"
        },
        {
            "_id": 12,
            "fname": "john",
            "lname": "doe",
            "country_code": 972,
            "country_name": "israel",
            "credit_card": "1234-1234-1234-1234",
            "gender": "M",
            "addresses": [
                {"id": 5, "country_code": "IL", "address": "my address 5"}
            ],
            "__$$opcode": "u"
        },
        # unsupported opcode
        {
            "_id": 99,
            "fname": "john",
            "lname": "doe",
            "country_code": 972,
            "country_name": "israel",
            "credit_card": "1234-1234-1234-1234",
            "gender": "M",
            "addresses": [],
            "__$$opcode": "x"
        }
    ]

    for record in records:
        redis_client.xadd("emp", {"message": json.dumps(record)})
