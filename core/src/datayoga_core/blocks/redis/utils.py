from typing import Optional

import redis
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
