import ssl
from typing import Any, Dict

import redis
from redis import Redis


def get_client(connection: Dict[str, Any]) -> Redis:
    """Establishes a connection to a Redis server with optional SSL/TLS encryption and authentication.

    Args:
        connection (Dict[str, Any]): A dictionary containing connection parameters:
            - "host" (str): The Redis server hostname or IP address.
            - "port" (int): The Redis server port number.
            - "user" (Optional[str]): Redis username.
            - "password" (Optional[str]): Redis password.
            - "key" (Optional[str]): Path to the client private key file for SSL/TLS.
            - "key_password" (Optional[str]): Password for the client private key file.
            - "cert" (Optional[str]): Path to the client certificate file for SSL/TLS.
            - "cacert" (Optional[str]): Path to the CA certificate file for SSL/TLS.

    Returns:
        Redis: Redis client instance.

    Raises:
        ValueError: If connection to Redis fails.
    """
    host = connection["host"]
    port = connection["port"]
    key = connection.get("key")
    key_password = connection.get("key_password")
    cert = connection.get("cert")
    cacert = connection.get("cacert")

    try:
        client = redis.Redis(
            host=host,
            port=port,
            username=connection.get("user"),
            password=connection.get("password"),
            ssl=(key is not None and cert is not None) or cacert is not None,
            ssl_keyfile=key,
            ssl_password=key_password,
            ssl_certfile=cert,
            ssl_cert_reqs=ssl.CERT_REQUIRED if cacert else ssl.CERT_NONE,
            ssl_ca_certs=cacert,
            decode_responses=True,
            client_name="datayoga"
        )

        client.ping()
        return client
    except Exception as e:
        raise ValueError(f"can not connect to Redis on {host}:{port}:\n {e}")
