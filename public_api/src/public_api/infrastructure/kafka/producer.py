import asyncio
import json

from confluent_kafka.aio import AIOProducer

from public_api.core.config import settings
from public_api.core.logger import get_logger


logger = get_logger(__name__)


class KafkaEventPublisher:
    def __init__(self) -> None:
        self._producer = AIOProducer(
            settings.producer_config,
            batch_size=1,
            buffer_timeout=0.5,
        )

    async def publish(self, topic: str, key: str, event: dict) -> None:
        logger.info(
            "kafka_message_publish_started",
            extra={
                "topic": topic,
                "key": key,
                "event_id": event.get("event_id"),
                "event_type": event.get("event_type"),
            },
        )

        delivery = await self._producer.produce(
            topic=topic,
            key=key.encode('utf-8'),
            value=json.dumps(event).encode('utf-8'),
        )

        delivered_message = await asyncio.wait_for(
            delivery,
            timeout=(settings.kafka_delivery_timeout_ms / 1000) + 2,
        )

        logger.info(
            "kafka_message_delivered",
            extra={
                "topic": delivered_message.topic(),
                "partition": delivered_message.partition(),
                "offset": delivered_message.offset(),
                "key": key,
            },
        )

    async def close(self) -> None:
        await self._producer.close()

    async def check_connection(self) -> None:
        await self._producer.list_topics(timeout=1)
