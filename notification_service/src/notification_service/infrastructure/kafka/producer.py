import asyncio
import json

from confluent_kafka.aio import AIOProducer

from notification_service.core.config import settings
from notification_service.core.logger import get_logger


logger = get_logger(__name__)


class KafkaEventPublisher:
    def __init__(self) -> None:
        self._producer = AIOProducer(
            settings.producer_config,
            batch_size=1,
            buffer_timeout=0.5,
        )

    async def publish_dlq(self, topic: str, key: str, event: dict) -> None:
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
        await asyncio.wait_for(
            delivery,
            timeout=(settings.kafka_delivery_timeout_ms / 1000) + 2,
        )

    async def close(self) -> None:
        await self._producer.close()
