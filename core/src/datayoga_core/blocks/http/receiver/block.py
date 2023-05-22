import logging
from abc import ABCMeta
from asyncio import Queue
from contextlib import suppress
from itertools import count
from typing import AsyncGenerator, List, Optional

import orjson
from aiohttp.web import BaseRequest, Response, Server, ServerRunner, TCPSite, HTTPOk, HTTPInternalServerError
from datayoga_core.context import Context
from datayoga_core.producer import Message
from datayoga_core.producer import Producer as DyProducer

logger = logging.getLogger("dy")


class Block(DyProducer, metaclass=ABCMeta):
    port: int

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")
        self.port = int(self.properties.get("port", 8080))

    async def produce(self) -> AsyncGenerator[List[Message], None]:
        queue = Queue(maxsize=1000)

        async def handler(request: BaseRequest) -> Response:
            try:
                queue.put_nowait(orjson.loads(await request.read()))
                return HTTPOk()
            except Exception:  # noqa
                logger.exception("Got exception while parsing request:")
                return HTTPInternalServerError()

        runner = ServerRunner(Server(handler))
        await runner.setup()
        srv = TCPSite(runner, "0.0.0.0", self.port)
        await srv.start()
        logger.info(f"Listening on 0.0.0.0:{self.port}...")

        try:
            counter = iter(count())

            while True:
                data = await queue.get()
                yield [{self.MSG_ID_FIELD: f"{next(counter)}", **data}]

        finally:
            with suppress(Exception):
                await srv.stop()
