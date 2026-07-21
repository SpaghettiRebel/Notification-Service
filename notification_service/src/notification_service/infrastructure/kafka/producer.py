import json

from confluent_kafka import Producer

from notification_service.core.config import settings
from notification_service.core.logger import get_logger


logger = get_logger(__name__)


class KafkaEventPublisher:
    def __init__(self):
        self._producer = Producer(settings.producer_config)

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

        self._producer.produce(
            topic=topic,
            key=key.encode('utf-8'),
            value=json.dumps(event).encode('utf-8'),
        )

        self._producer.poll(0)
        self._producer.flush()
