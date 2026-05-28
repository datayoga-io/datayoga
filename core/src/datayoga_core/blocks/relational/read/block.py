import logging
from typing import Any, AsyncGenerator, Dict, List, Optional

import sqlalchemy as sa
from datayoga_core import utils
from datayoga_core.blocks.relational import utils as relational_utils
from datayoga_core.context import Context
from datayoga_core.producer import Producer as DyProducer

logger = logging.getLogger("dy")


class Block(DyProducer):
    DEFAULT_FETCH_SIZE = 10000

    def init(self, context: Optional[Context] = None):
        self.engine, self.db_type = relational_utils.get_engine(
            self.properties["connection"],
            context,
            autocommit=False,
        )

        self.schema = self.properties.get("schema")
        self.table = self.properties.get("table")
        self.opcode_field = self.properties.get("opcode_field")
        self.load_strategy = self.properties.get("load_strategy")
        self.keys = self.properties.get("keys")
        self.mapping = self.properties.get("mapping")

        self.tbl = sa.Table(self.table, sa.MetaData(schema=self.schema), autoload_with=self.engine)

        logger.debug(f"Connecting to {self.db_type}")
        self.connection = self.engine.connect()

    async def produce_chunks(self) -> AsyncGenerator[List[Dict[str, Any]], None]:
        fetch_size = int(self.properties.get("fetch_size", self.DEFAULT_FETCH_SIZE))
        result = self.connection.execution_options(stream_results=True).execute(self.tbl.select())
        while True:
            rows = result.fetchmany(fetch_size)
            if not rows:
                return
            yield [utils.add_uid(dict(row._asdict())) for row in rows]

    def stop(self):
        self.connection.close()
        self.engine.dispose()
