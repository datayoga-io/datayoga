import logging
from typing import Any, Dict, List, Optional, Tuple

import sqlalchemy as sa
from datayoga_core.block import Block as DyBlock
from datayoga_core.block import Result
from datayoga_core.context import Context
from datayoga_core.utils import get_connection_details

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
        logger.debug(f"Connecting to {connection.get('type')}")

        self.conn = self.engine.connect()

    async def run(self, data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Result]]:
        logger.debug(f"Running {self.get_block_name()}")

        logger.info(f"Inserting {len(data)} record(s) to {self.table} table")
        tbl = sa.Table(self.table, sa.MetaData(schema=self.schema), autoload_with=self.engine)

        self.conn.execute(tbl.insert(), data)
        # if we made it here, it is a success. return the data and the success result
        return data, [Result.SUCCESS]*len(data)
