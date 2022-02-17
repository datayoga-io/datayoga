import logging
logger = logging.getLogger("dy_runner")


def get_full_table_name(table_name: str, table_schema: str, connection: str, logger) -> str:
    """
    Prepare fully qualified table name
    """
    # SQLServer needs the table name to be enclosed in square brackets in case of reserved works (like 'User' table)
    if connection['subtype'] == "sqlserver":
        full_table_name = f"[{table_name}]"
    else:
        full_table_name = table_name

    if table_schema is not None and table_schema != "":
        full_table_name = table_schema+"."+full_table_name
    # try to take the schema from the connection definitions
    elif connection.get("schema"):
        full_table_name = connection.get("schema")+"."+full_table_name
    else:
        logger.warning(f"no schema set for table {table_name}. Using default database schema")

    return full_table_name


def get_jdbc_url(connection) -> str:
    """given a connection dictionary, retrieve the fully qualified jdbc url

    Parameters:
        connection (dict): contains keys for type, subtype, host, port

    Returns:
        str: jdbc url

    """
    if connection['subtype'] == 'sqlite':
        return f"{connection['type']}:{connection['subtype']}:{connection.get('database')}"
    else:
        return f"{connection['type']}:{connection['subtype']}://{connection.get('host')}:{connection.get('port')}"
