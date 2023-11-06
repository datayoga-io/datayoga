import asyncio
import logging
from typing import AsyncGenerator, List, Optional

import orjson
from azure.eventhub import EventData, PartitionContext
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import \
    BlobCheckpointStore
from datayoga_core.context import Context
from datayoga_core.producer import Message
from datayoga_core.producer import Producer as DyProducer

logger = logging.getLogger("dy")


class Block(DyProducer):
    """Azure Event Hub block for reading events."""

    def init(self, context: Optional[Context] = None):
        """Initializes the block.

        Args:
            context (Context, optional): The block context. Defaults to None.
        """
        logger.debug(f"Initializing {self.get_block_name()}")

        self.batch_size = self.properties.get("batch_size", 300)

        self.consumer_client = EventHubConsumerClient.from_connection_string(
            conn_str=self.properties["event_hub_connection_string"],
            consumer_group=self.properties["event_hub_consumer_group_name"],
            eventhub_name=self.properties["event_hub_name"],
            checkpoint_store=BlobCheckpointStore.from_connection_string(
                self.properties["checkpoint_store_connection_string"],
                self.properties["checkpoint_store_container_name"])
        )

        self.events = {}  # Retrieved events by sequence number, used for acknowledging them once processed
        self.messages = asyncio.Queue()

    async def produce(self) -> AsyncGenerator[List[Message], None]:
        """Starts the event receiving process and yield batches of messages.

        Yields:
            AsyncGenerator[List[Message], None]: A generator of message batches.
        """
        logger.debug(f"Running {self.get_block_name()}")

        logger.debug("Starting event receiving process")
        asyncio.create_task(self.receive_batch())

        while True:
            if not self.messages.empty():
                batch = []
                while not self.messages.empty():
                    message = await self.messages.get()
                    batch.append(message)

                yield batch

            await asyncio.sleep(0.1)

    async def receive_batch(self):
        """Receives events in batches from the Event Hub."""
        await self.consumer_client.receive_batch(
            on_event_batch=self.on_event_batch,
            max_batch_size=self.batch_size,
            starting_position="-1",  # read from the beginning of the partition.
        )

    async def on_event_batch(self, partition_context: PartitionContext, events: List[EventData]):
        """Processes each batch of events received from the Event Hub.

        Args:
            partition_context (PartitionContext): The partition context.
            events (List[EventData]): The list of events in the batch.
        """
        logger.debug(f"Received batch of events from partition: {partition_context.partition_id}")

        for event in events:
            try:
                payload = orjson.loads(event.body_as_str(encoding="UTF-8"))
                msg_id = event.system_properties[b"x-opt-sequence-number"]
                self.events[msg_id] = (event, partition_context)
                payload[self.MSG_ID_FIELD] = msg_id
                await self.messages.put(payload)
            except Exception as e:
                logger.error(e)

    async def complete_events(self, msg_ids: List[str]):
        """Completes the events and update the checkpoint.

        Args:
            msg_ids (List[str]): The list of message IDs to complete.
        """
        for msg_id in msg_ids:
            logger.debug(f"Acking {msg_id} event")
            event, partition_context = self.events.pop(msg_id, (None, None))

            if event is not None:
                await partition_context.update_checkpoint(event)
            else:
                logger.warning(f"Couldn't find event {msg_id} for acknowledging")

    def ack(self, msg_ids: List[str]):
        """Acknowledges the completion of events.

        Args:
            msg_ids (List[str]): The list of message IDs to acknowledge.
        """
        asyncio.create_task(self.complete_events(msg_ids))
