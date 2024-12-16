from typing import Any, Dict

from redis.client import Redis

SSL_CIPHERS = "AES256-SHA:DHE-RSA-AES256-SHA:AES128-SHA:DHE-RSA-AES128-SHA"


def get_client(connection_details: Dict[str, Any]) -> Redis:
    """Establishes a connection to a Redis server with optional SSL/TLS encryption and authentication.

    Args:
        connection_details (Dict[str, Any]): A dictionary containing connection parameters:
            - "type" (str): Must be "redis" for Redis connections.
            - "host" (str): The Redis DB hostname or IP address.
            - "port" (int): The Redis DB port number.
            - "user" (Optional[str]): Redis username.
            - "password" (Optional[str]): Redis password.
            - "key" (Optional[str]): Path to the client private key file for SSL/TLS.
            - "key_password" (Optional[str]): Password for the client private key file.
            - "cert" (Optional[str]): Path to the client certificate file for SSL/TLS.
            - "cacert" (Optional[str]): Path to the CA certificate file for SSL/TLS.
            - "socket_timeout" (Optional[float]): Socket timeout in seconds. Default: 10.0.
            - "socket_connect_timeout" (Optional[float]): Socket connection timeout in seconds. Default: 2.0.
            - "socket_keepalive" (Optional[bool]): Enable/disable TCP keepalive. Default: True.
            - "health_check_interval" (Optional[float]): Interval for health checks in seconds. Default: 60.

    Returns:
        Redis: Redis client instance.

    Raises:
        ValueError: If type is not "redis" or if connection to Redis fails.
    """
    if connection_details["type"] != "redis":
        raise ValueError("not a Redis connection")

    host = connection_details["host"]
    port = connection_details["port"]
    key = connection_details.get("key")
    key_password = connection_details.get("key_password")
    cert = connection_details.get("cert")
    cacert = connection_details.get("cacert")

    try:
        redis_client = Redis(
            host=host,
            port=port,
            username=connection_details.get("user"),
            password=connection_details.get("password"),
            ssl=cacert is not None or cert is not None or key is not None,
            ssl_keyfile=key,
            ssl_password=key_password,
            ssl_certfile=cert,
            ssl_ca_certs=cacert,
            ssl_ciphers=SSL_CIPHERS,  # Customize TLS ciphers (Python 3.10+)
            decode_responses=True,
            client_name="datayoga",
            socket_timeout=connection_details.get("socket_timeout", 10.0),
            socket_connect_timeout=connection_details.get("socket_connect_timeout", 2.0),
            socket_keepalive=connection_details.get("socket_keepalive", True),
            health_check_interval=connection_details.get("health_check_interval", 60)
        )

        redis_client.ping()
        return redis_client
    except Exception as e:
        raise ValueError(f"can not connect to Redis on {host}:{port}:\n {e}")
