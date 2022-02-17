import logging
import pyspark.sql.types as T
import pyspark.sql.functions as F
import common.utils
import common.db_utils
import pyspark.sql
import os
from functools import reduce

logger = logging.getLogger("dy_runner")


def flatfile(spark, source, filename_column: str = None):
    properties = common.utils.get_catalog(source)
    if properties["filetype"] == "delimited":
        options = {
            "quote": "\"",
            "escape": "\"",
            # "mode": "DROPMALFORMED", # TODO: add drop_malformed option to extract
            "ignoreTrailingWhiteSpace": True,
            "ignoreLeadingWhiteSpace": True,
        }
        options["delimiter"] = properties.get("delimiter", ",")
        options["header"] = properties.get("header", True)
        options["multiLine"] = properties.get("multi_line", False)

        # prepare the schema based on the catalog definition, if exists
        if not options["header"]:
            schema = T.StructType.fromJson({"fields": [
                {
                    "name": field.get('name'),
                    "type": field.get('datatype'),
                    "nullable": True,
                    "metadata": {}
                }
                for field in properties["columns"]
            ]})
        else:
            # schema will be taken from header
            schema = None

        # locate all files
        file_limit = properties.get("limit", 1)
        file_locations = common.utils.get_file_locations(
            common.utils.get_datafolder({}, "raw"),
            properties["filename"],
            limit=file_limit,
            sort=properties.get("sort", 'last_modified'),
            ascending=properties.get("ascending", True)
        )

        if file_limit is None or file_limit == -1:
            # reading without a limit may be slow. we use spark's build-in bulk reader. this will not support skipping header or footers
            df = spark.read.options(**options).csv(file_locations)
        else:
            df_input_segments = []
            for file_location in file_locations:
                # read the csv without inferring schema
                reader = spark.read.options(**options)
                df_input_segment = read_csv_remove_header_footer(
                    spark, file_location, reader, properties.get("skip_header_rows", 0), schema=schema)
                if len(properties.get("columns", [])) > 0:
                    df_input_segment = df_input_segment.toDF(
                        *map(lambda col: col["name"], properties.get("columns", [])))

                df_input_segments.append(df_input_segment)

            # reduce the input segments of multiple files to a single dataframe
            df = reduce(pyspark.sql.DataFrame.unionAll, df_input_segments)

        # optionally add the filename as a column
        if filename_column and filename_column != "":
            df = df.withColumn(
                filename_column,
                F.reverse(F.split(F.input_file_name(), os.path.sep))[0]
            )

        # multiLine produces one big partition. repartition for performance purposes
        # if options["multiLine"]:
        #     df = common.utils.repartition_by_size(df)

    elif properties["filetype"] == "parquet":
        file_location = os.path.join(common.utils.get_datafolder({}, "raw"), properties["filename"])
        df = spark.read.parquet(file_location)

    # logging
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"extracted {df.count()} rows from source: {source}")

    return df


def read_csv_remove_header_footer(spark, file_location, dataframe_reader, header_lines, footer_lines=0,
                                  schema: T.StructType = None) -> pyspark.sql.DataFrame:
    # returns dataframe while skipping header or footer rows

    if header_lines > 0 or footer_lines > 0:
        # WARNING: loading of multiline with skip_header_rows does not work due to spark implementation of read.csv!
        # read as text
        # then filter out the n lines to skip
        rdd = spark.read.text(file_location).rdd.zipWithIndex()
        if header_lines > 0:
            rdd = rdd.filter(lambda line_index: line_index[1] >= header_lines)
        # if we need to filter footer, we also need to figure out line count. then filter again
        if (footer_lines > 0):
            line_count = rdd.count()
            rdd = rdd.filter(
                lambda line_index: line_index[1] <= line_count-footer_lines)
        rdd = rdd.map(lambda row: row[0].value)
        # parse as csv
        # the following line can be used if in the future we want to limit the amount of records used for inferSchema. Otherwise all rows are used
        # which can be slow for large files
        # df_schema = dataframe_reader.csv(rdd.map(lambda x: (x, )).toDF().limit(500).rdd.map(lambda x: x[0])).schema
        df = dataframe_reader.csv(rdd, schema)
    else:
        # use straight load from file.
        df = dataframe_reader.csv(file_location, schema)
    # return casted dataframe
    # note: we use backticks in column names to avoid a bug in spark with columns names that contain special characters
    return df
    # TODO: cast to schema types
    # return df.select([F.col(f"`{c.name}`").cast(T.DecimalType(38,18)) if c.dataType.typeName() in ["double","float","long"] else F.col(f"`{c.name}`") for c in df.schema])


def jdbc(spark,
         connection_name,
         table_name=None,
         table_schema=None,
         query=None,
         columns=[],
         column_mapping="order"
         ) -> pyspark.sql.DataFrame:
    """
    column_mapping - 'order' means map the columns by order they appear in the SQL query. 'name' match by name in the columns list
    """
    connection = common.utils.get_connection({}, connection_name)
    # this is an APPEND operation. load using spark
    jdbcUrl = common.db_utils.get_jdbc_url(connection)
    logger.debug(f"connecting to {jdbcUrl}")

    full_table_name = common.db_utils.get_full_table_name(
        table_name,
        table_schema,
        connection,
        logger
    )

    # read data into table
    logger.debug(f"reading from {full_table_name if table_name else query}")
    df = spark.read \
        .format("jdbc") \
        .option("url", jdbcUrl) \
        .option("user", connection.get('user')) \
        .option("password", connection.get('password')) \
        .option("driver", connection.get('driver')) \
        .option("database", connection.get('database')) \
        .option("fetchsize", 100000)
    if table_name:
        df = df.option("dbtable", full_table_name)
    elif query:
        df = df.option("query", query)
        # rename the columns
    else:
        raise ValueError(
            "extract from database must have either table_name or query")

    return df.load().cache()
