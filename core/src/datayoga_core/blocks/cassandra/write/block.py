

import logging
from typing import Any, Dict, List, Optional, Tuple

import cassandra.auth
import cassandra.cluster
import sqlalchemy as sa
from datayoga_core import utils
from datayoga_core.block import Block as DyBlock
from datayoga_core.context import Context
from datayoga_core.result import Result

logger = logging.getLogger("dy")


class Block(DyBlock):

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")

        connection_name = self.properties.get("connection")
        connection_details = utils.get_connection_details(connection_name, context)
        if connection_details.get("type") != "cassandra":
            raise ValueError(f"{connection_name} is not a cassandra connection")

        auth_provider = cassandra.auth.PlainTextAuthProvider(
            username=connection_details.get("user"),
            password=connection_details.get("password"))

        cluster = cassandra.cluster.Cluster(
            connection_details.get("hosts"),
            port=connection_details.get("port", 9042),
            auth_provider=auth_provider)

        logger.debug(f"Connecting to Cassandra")
        self.session = cluster.connect(connection_details.get("keyspace"))

        self.opcode_field = self.properties.get("opcode_field")
        self.keys = self.properties.get("keys")
        self.mapping = self.properties.get("mapping")

        primary_keys = utils.get_fields(self.keys)
        mapping_fields = utils.get_fields(self.mapping)

        # self.delete_stmt = self.tbl.delete(
        #     sa.and_(*[sa.text(f"{self.tbl.columns.get(field['column'])} = {sa.bindparam(field['key'])}")
        #               for field in primary_keys]))

        # self.upsert_stmt = generate_upsert_stmt(self.tbl.fullname, primary_keys, mapping_fields, db_type)

    async def run(self, data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Result]]:
        logger.debug(f"Running {self.get_block_name()}")

        return data
