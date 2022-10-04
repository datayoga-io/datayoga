
import logging
import os
import time
from os import path
from subprocess import PIPE, Popen
from typing import Any, Dict, Optional, Union

import redis
from datayoga.utils import read_yaml
from redis import Redis
from testcontainers.core.container import DockerContainer
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer

logger = logging.getLogger("dy")


def get_job_settings_from_yaml(filename: str) -> Dict[str, Any]:
    job_settings = read_yaml(path.join(os.path.dirname(os.path.realpath(__file__)), "..", "resources", filename))
    logger.debug(f"job_settings: {job_settings}")
    return job_settings


def execute_program(command: str):
    """Executes a child program in a new process and logs its output.

    Args:
        command (str): script or command to run.

    Raises:
        ValueError: When the return code is not 0.
    """
    logger.debug(f"Running command: {command}")
    process = Popen(command, stdin=PIPE, stdout=PIPE, shell=True, universal_newlines=True)
    while process.poll() is None:
        logger.info(process.stdout.readline().rstrip())
        time.sleep(0.2)

    process.communicate()[0]
    if process.returncode != 0:
        raise ValueError(f"command {command} failed")


def run_job(job_file: str):
    execute_program(
        f'datayoga run {path.join(os.path.dirname(os.path.realpath(__file__)), "..", "resources", job_file)} --loglevel DEBUG')


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


def get_postgres_container():
    return PostgresContainer(dbname="postgres", user="postgres", password="postgres").with_bind_ports(5432, 5433)
