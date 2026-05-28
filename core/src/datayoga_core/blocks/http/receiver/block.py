import logging
from abc import ABCMeta
from asyncio import Queue
from contextlib import suppress
from itertools import count
from typing import Any, AsyncGenerator, Dict, List, Optional

import orjson
from aiohttp.web import (BaseRequest, HTTPInternalServerError, HTTPOk,
                         Response, Server, ServerRunner, TCPSite)
from datayoga_core.context import Context
from datayoga_core.producer import Producer as DyProducer

logger = logging.getLogger("dy")


class Block(DyProducer, metaclass=ABCMeta):
    """Producer block that exposes an HTTP endpoint and emits POSTed JSON bodies."""

    port: int
    host: str
    DEFAULT_FLUSH_MS = 1000

    def init(self, context: Optional[Context] = None):
        """Reads host/port from properties; the HTTP server is started in produce_chunks."""
        logger.debug(f"Initializing {self.get_block_name()}")
        self.port = int(self.properties.get("port", 8080))
        self.host = self.properties.get("host", "0.0.0.0")

    async def produce_chunks(self) -> AsyncGenerator[List[Dict[str, Any]], None]:
        """Starts the HTTP server, then yields one chunk per drained queue snapshot."""
        queue: Queue = Queue(maxsize=1000)

        async def handler(request: BaseRequest) -> Response:
            """Parses the incoming HTTP body as JSON and enqueues it for delivery."""
            try:
                queue.put_nowait(orjson.loads(await request.read()))
                return HTTPOk()
            except Exception:
                logger.exception("Got exception while parsing request:")
                return HTTPInternalServerError()

        runner = ServerRunner(Server(handler))
        await runner.setup()
        srv = TCPSite(runner, self.host, self.port)
        await srv.start()
        logger.info(f"Listening on {self.host}:{self.port}...")

        try:
            counter = iter(count())
            while True:
                first = await queue.get()
                chunk = [{self.MSG_ID_FIELD: f"{next(counter)}", **first}]
                while not queue.empty():
                    record = queue.get_nowait()
                    chunk.append({self.MSG_ID_FIELD: f"{next(counter)}", **record})
                yield chunk
        finally:
            with suppress(Exception):
                await srv.stop()
