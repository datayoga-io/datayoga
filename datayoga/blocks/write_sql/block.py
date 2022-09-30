import logging
from typing import Any, Dict, List, Optional

import sqlalchemy as sa
from datayoga.block import Block as DyBlock
from datayoga.context import Context
from datayoga.utils import get_connection_details

logger = logging.getLogger("dy")


class Block(DyBlock):
    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")

        connection = get_connection_details(self.properties.get("connection"), context)

        engine_url = sa.engine.URL.create(
            drivername=connection.get("type"),
            host=connection.get("host"),
            port=connection.get("port"),
            username=connection.get("user"),
            password=connection.get("password"),
            database=connection.get("database")
        )

        self.schema = self.properties.get("schema")
        self.table = self.properties.get("table")
        self.engine = sa.create_engine(engine_url, echo=False)
        self.conn = self.engine.connect()

    def run(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        logger.debug(f"Running {self.get_block_name()}")

        logger.info(f"Inserting {len(data)} record(s) to {self.table} table")
        tbl = sa.Table(self.table, sa.MetaData(schema=self.schema), autoload_with=self.engine)

        self.conn.execute(tbl.insert(), data)
