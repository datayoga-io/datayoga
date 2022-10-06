from typing import Optional, Union

from testcontainers.core.container import DockerContainer
from testcontainers.redis import RedisContainer

import redis
from redis import Redis


def get_redis_client(host: str, port: int, password: Optional[str] = None) -> Redis:
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


def get_redis_oss_container(redis_port: int, redis_password: Optional[str] = None) -> Union[DockerContainer, RedisContainer]:
    if redis_password:
        return DockerContainer(image="redis:latest").\
            with_bind_ports(6379, redis_port).\
            with_command(f"redis-server --requirepass {redis_password}")
    else:
        return RedisContainer().with_bind_ports(6379, redis_port)
