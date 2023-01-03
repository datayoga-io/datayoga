import logging
import os
from csv import DictReader
from typing import Generator, Optional

import sqlalchemy as sa
from datayoga_core import utils
from datayoga_core.blocks.relational import utils as relational_utils
from datayoga_core.context import Context
from datayoga_core.producer import Message
from datayoga_core.producer import Producer as DyProducer

logger = logging.getLogger("dy")


class Block(DyProducer):

    def init(self, context: Optional[Context] = None):
        connection = utils.get_connection_details(self.properties.get("connection"), context)

        self.db_type = connection.get("type").lower()
        if not relational_utils.DbType.has_value(self.db_type):
            raise ValueError(f"{self.db_type} is not supported yet")

        self.schema = self.properties.get("schema")
        self.table = self.properties.get("table")
        self.opcode_field = self.properties.get("opcode_field")
        self.load_strategy = self.properties.get("load_strategy")
        self.keys = self.properties.get("keys")
        self.mapping = self.properties.get("mapping")

        engine = sa.create_engine(
            sa.engine.URL.create(
                drivername=connection.get("driver", relational_utils.DEFAULT_DRIVERS.get(self.db_type)),
                host=connection.get("host"),
                port=connection.get("port"),
                username=connection.get("user"),
                password=connection.get("password"),
                database=connection.get("database")),
            echo=connection.get("debug", False), connect_args=connection.get("connect_args", {}))
        self.tbl = sa.Table(self.table, sa.MetaData(schema=self.schema), autoload_with=engine)

        logger.debug(f"Connecting to {self.db_type}")
        self.connection = engine.connect()

    def produce(self) -> Generator[Message, None, None]:
        result = self.connection.execution_options(stream_results=True).execute(
            self.tbl.select()
        )

        while True:
            chunk = result.fetchmany(10000)
            if not chunk:
                break
            for row in chunk:
                yield utils.add_uid(dict(row))
