import json

from confluent_kafka import Producer

from public_api.src.public_api.core.config import settings
from public_api.src.public_api.core.logger import get_logger


logger = get_logger(__name__)


class KafkaEventPublisher:
    def __init__(self):
        self._producer = Producer(settings.producer_config)

    @staticmethod
    def delivery_callback(err, msg) -> None:
        if err:
            logger.error(
                "kafka_message_delivery_failed",
                extra={
                    "error": str(err),
                    "topic": msg.topic() if msg else None,
                },
            )
            return

        key = msg.key().decode("utf-8") if msg.key() else None
        value = msg.value().decode("utf-8") if msg.value() else None

        logger.info(
            "kafka_message_delivered",
            extra={
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "key": key,
            },
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

        self._producer.produce(
            topic=topic,
            key=key.encode('utf-8'),
            value=json.dumps(event).encode('utf-8'),
            callback=self.delivery_callback,
        )

        self._producer.poll(0)
        self._producer.flush()
