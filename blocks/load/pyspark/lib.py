import yaml
import string
import random
import jaydebeapi
import os
import pyspark.sql
import pyspark.sql.functions as F
import pyspark.sql.types as T
import logging
import common.utils
import common.db_utils
from typing import Any, List
logger = logging.getLogger("dy_runner")


class LoadStrategy:
    APPEND: str = "append"
    UPSERT: str = "upsert"
    REPLACE: str = "replace"
    UPDATE: str = "update"
    TYPE2: str = "type2"


def load(
    df,
    spark,
    connection_name: str,
    table_name: str,
    table_schema: str,
    business_keys: List[str],
    df_schema,
    load_strategy: LoadStrategy = LoadStrategy.APPEND,
    inactive_record_mapping: List[any] = [],
    active_record_indicator: str = None
) -> pyspark.sql.DataFrame:
    # logging
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"loading {df.count()} rows into table: {table_name} using load_strategy: {load_strategy}")
    else:
        logger.info(f"loading into table: {table_name} using load_strategy: {load_strategy}")

    # NOTE: databases like Snowflake require all columns in uppercase otherwise it default to having columns as case sensitive
    # and must be quoted
    # TODO: add business keys if not explicitly stated in the schema
    if df_schema and df_schema != {} and df_schema != []:
        cols = [
            F.expr(str(mapping.get("source"))).alias(mapping.get("target", mapping.get("source")).upper())
            for mapping in df_schema]
        df_target = df.select(*cols)
    else:
        # take all columns
        df_target = df

    load_jdbc(
        df_target,
        spark,
        connection_name,
        table_name,
        table_schema,
        business_keys,
        df_schema,
        load_strategy,
        inactive_record_mapping,
        active_record_indicator
    )
    logger.info(f"loading into table: {table_name} using load_strategy: {load_strategy}: done")
    # TODO: handle writing to files


def load_jdbc(
    df,
    spark,
    connection_name,
    table_name,
    table_schema,
    business_keys,
    df_schema,
    load_strategy,
    inactive_record_mapping,
    active_record_indicator
):
    # search for special _load_strategy column. This is an indication of special handling
    with open(os.path.join(pyspark.files.SparkFiles.getRootDirectory(), "env.yaml")) as envfile:
        env = yaml.safe_load(envfile)
    connection = common.utils.get_connection(env, connection_name)
    if "_load_strategy" in df.columns or load_strategy.lower() == LoadStrategy.UPDATE or load_strategy.lower() == LoadStrategy.UPSERT:
        upsert(
            df,
            spark,
            connection,
            table_name,
            table_schema,
            business_keys,
            df_schema,
            load_strategy
        )
    elif load_strategy.lower() == LoadStrategy.TYPE2:
        update_type2(
            df,
            spark,
            connection,
            table_name,
            table_schema,
            business_keys,
            df_schema,
            inactive_record_mapping,
            active_record_indicator
        )
    else:

        # this is an APPEND operation. load using spark
        jdbcUrl = common.db_utils.get_jdbc_url(connection)

        full_table_name = common.db_utils.get_full_table_name(
            table_name,
            table_schema,
            connection,
            logger
        )

        df.write \
            .format("jdbc") \
            .option("driver", connection['driver']) \
            .option("dbtable", full_table_name) \
            .option("batchsize", 10000) \
            .option("user", connection.get('user')) \
            .option("password", connection.get('password')) \
            .option("database", connection.get('database')) \
            .mode("APPEND") \
            .option("url", jdbcUrl) \
            .save()


def upsert(
    df,
    spark,
    connection: str,
    table_name: str,
    table_schema: str,
    business_keys: List[str],
    df_schema,
    load_strategy: str
):
    """
    simulate an upsert by splitting into an insert/update/delete statement
    This does not use the MERGE statement for better flexibility
    """
    # some databases do not support upsert. we create multiple statements
    jdbcUrl = common.db_utils.get_jdbc_url(connection)
    jdbcDriver = connection['driver']

    # spark can only insert rows. we use a library to connect directly to JDBC sources
    # using the Jar files of Spark
    conn = _get_jaydebe_connection(spark, connection)
    db = conn.cursor()

    full_table_name = common.db_utils.get_full_table_name(
        table_name,
        table_schema,
        connection,
        logger
    )

    #
    # update statement
    #
    temp_table_name = load_to_temp_table(
        spark=spark,
        df=df,
        table_name=table_name,
        table_schema=table_schema,
        connection=connection,
    )

    # load data into temp table for bulk performance
    # TODO: filter _LOAD_STRATEGY
    if load_strategy.lower() == LoadStrategy.UPDATE or load_strategy.lower() == LoadStrategy.UPSERT:
        logger.debug("loading data to temp table")

        if df_schema and df_schema != {} and df_schema != []:
            columns_to_update = [col.get('target', col.get('source')) for col in df_schema]
        else:
            # update all columns except business keys
            columns_to_update = list(set(df.columns)-set(business_keys))

        set_clause = ", ".join(
            [f"{colname}=incoming.{colname}" for colname in columns_to_update])

        join_clause = " and ".join([
            f"({full_table_name}.{key}=incoming.{key} or ({full_table_name}.{key} is null and incoming.{key} is null))"
            for key in business_keys])
        update_stmt = f"""
            update {full_table_name} set {set_clause} 
            from {temp_table_name} incoming 
            where {join_clause}
        """
        logger.debug(update_stmt)
        db.execute(update_stmt)
        logger.debug(f"{db.rowcount} rows updated")

    if load_strategy.lower() == LoadStrategy.UPSERT:
        # also perform an insert statement of all that do not exist
        # TODO: handle _load_strategy

        if df_schema and df_schema != {} and df_schema != []:
            columns_to_update = [col.get('target', col.get('source')) for col in df_schema]
        else:
            # update all columns except business keys
            columns_to_update = df.columns

        columns_clause = ",".join(columns_to_update)

        # join by business keys
        join_clause = " and ".join([
            f"(existing.{key}=incoming.{key} or (existing.{key} is null and incoming.{key} is null))"
            for key in business_keys])

        insert_stmt = f"""
            insert into {full_table_name} ({columns_clause})
            select {columns_clause} from {temp_table_name} incoming
            where not exists (
                select {columns_clause} from {full_table_name} existing
                where {join_clause}
            )
        """
        logger.debug(insert_stmt)
        db.execute(insert_stmt)
        logger.debug(f"{db.rowcount} rows inserted")

        # TODO: add delete
    drop_stmt = f"""
    drop table {temp_table_name}
    """
    db.execute(drop_stmt)
    logger.debug(f"dropping temp table")
    # close connections
    db.close()
    conn.close()
    logger.debug("done")


def update_type2(
    df,
    spark,
    connection: str,
    table_name: str,
    table_schema: str,
    business_keys: List[str],
    df_schema,
    inactive_record_mapping: List[Any],
    active_record_indicator: str
):
    # bulk load to temp table
    temp_table_name = load_to_temp_table(
        spark=spark,
        df=df,
        table_name=table_name,
        table_schema=table_schema,
        connection=connection
    )

    # close out any active records
    full_table_name = common.db_utils.get_full_table_name(
        table_name,
        table_schema,
        connection,
        logger
    )

    conn = _get_jaydebe_connection(spark, connection)
    db = conn.cursor()

    join_clause = " and ".join([
        f"({full_table_name}.{key}=incoming.{key} or ({full_table_name}.{key} is null and incoming.{key} is null))"
        for key in business_keys])

    set_clause = ", ".join(
        [f"{col.get('target', col.get('source'))}=incoming.{col.get('source')}"
         for col in inactive_record_mapping])

    update_stmt = f"""
        update {full_table_name} set {set_clause} 
        from {temp_table_name} incoming 
        where {join_clause} and {active_record_indicator}
    """

    logger.debug(update_stmt)
    db.execute(update_stmt)
    logger.debug(f"{db.rowcount} rows updated")

    # add all new records
    columns_clause = ", ".join(df.columns)
    insert_stmt = f"""
        insert into {full_table_name} ({columns_clause})
        select {columns_clause} from {temp_table_name} incoming
    """
    logger.debug(insert_stmt)
    db.execute(insert_stmt)

    drop_stmt = f"""
    drop table {temp_table_name}
    """
    db.execute(drop_stmt)
    logger.debug(f"dropping temp table")

    db.close()
    conn.close()


def load_to_temp_table(spark, df, table_name, table_schema, connection) -> str:
    """Load a dataframe into a temp table

    Parameters:
        df (dataframe): dataframe to load
        table_name (str): original table name. used to create schema
        table_schema (str): schema to create the temp table
        connection: connection object

    Returns:
        str: name of temp table

    """

    jdbcUrl = common.db_utils.get_jdbc_url(connection)
    jdbcDriver = connection['driver']
    # we add a random suffix. in the future can change into UUID
    random_suffix = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(6))

    temp_table_name = common.db_utils.get_full_table_name(
        f"temp_{table_name}_{random_suffix}",
        table_schema,
        connection,
        logger
    )
    full_table_name = common.db_utils.get_full_table_name(
        table_name,
        table_schema,
        connection,
        logger
    )

    conn = _get_jaydebe_connection(spark, connection)
    db = conn.cursor()

    if connection['subtype'] == 'sqlserver':
        # use advanced spark connector for better performance
        # TODO: split all DB-specific behavior to a class inheriting from a generic class
        # create the table explicitly so the collation is preserved

        # explicitly create temp table for retaining same collation defintions as source table
        # for SQLServer, we use a hack of adding 'group by' to avoid identity column definitions and constraints to be copied over to the target
        # in the future, this can be changed to explicitly create the schema based on the database data dictionary
        drop_stmt = f"""
            IF OBJECT_ID('{temp_table_name}', 'U') IS NOT NULL 
            drop table {temp_table_name}
        """
        logger.debug(drop_stmt)
        db.execute(drop_stmt)
        create_stmt = f"""
            select Top 0 {','.join(df.columns)} into {temp_table_name} from {full_table_name} group by {','.join(df.columns)}
        """
        db.execute(create_stmt)

        writer = df.write.format("com.microsoft.sqlserver.jdbc.spark") \
            .option("tableLock", "true") \
            .option("schemaCheckEnabled", "false") \
            .option("truncate", "true")
    else:
        writer = df.write.format("jdbc")
    df.show()

    if connection.get('user'):
        writer = writer.option("user", connection.get('user'))
    if connection.get('password'):
        writer = writer.option("password", connection.get('password'))
    if connection.get('database'):
        writer = writer.option("database", connection.get('database'))

    writer \
        .option("driver", jdbcDriver) \
        .option("batchSize", 10000) \
        .option("dbtable", temp_table_name.upper()) \
        .mode("OVERWRITE") \
        .option("url", jdbcUrl) \
        .save()

    # TODO: add try catch finally
    db.close()
    conn.close()
    return temp_table_name


def _get_jaydebe_connection(spark, connection):
    # spark can only insert rows. we use a library to connect directly to JDBC sources
    # using the Jar files of Spark
    jdbcUrl = common.db_utils.get_jdbc_url(connection)
    jdbcDriver = connection['driver']

    driver_args = {}
    if connection.get('user'):
        driver_args['user'] = connection.get('user')
    if connection.get('password'):
        driver_args['password'] = connection.get('password')
    if connection.get('database'):
        driver_args['database'] = connection.get('database')
    conn = jaydebeapi.connect(
        jdbcDriver,
        jdbcUrl,
        driver_args=driver_args,
        jars=spark.sparkContext.getConf().get('spark.jars').split(",")  # spark receives a comma delimited string
    )
    return conn
