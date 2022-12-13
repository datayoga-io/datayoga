from typing import Any, Dict, Optional

import redis
from datayoga_core.context import Context
from datayoga_core.utils import get_connection_details
from redis import Redis


def get_client(host: str, port: int, password: Optional[str] = None) -> Redis:
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


def get_redis_connection_details(connection_name: str, context: Context) -> Dict[str, Any]:
    connection_details = get_connection_details(connection_name, context)
    if connection_details.get("type") != "redis":
        raise ValueError(f"{connection_name} is not a redis connection")

    return connection_details
