import json

from confluent_kafka import Consumer

from notification_service.core.config import settings
from notification_service.core.logger import get_logger
from notification_service.infrastructure.kafka.topics import get_topics


logger = get_logger(__name__)


class KafkaEventConsumer:
    def __init__(self):
        self._consumer = Consumer(settings.consumer_config, logger=logger)
        self._consumer.subscribe(get_topics())

    def listen(self):
        try:
            while True:
                msg = self._consumer.poll(timeout=1.0)
                if msg is None:
                    continue

                if msg.error():
                    logger.error(f"Kafka error: {msg.error()}")
                    continue

                try:
                    payload = json.loads(msg.value().decode('utf-8'))
                    yield payload
                except json.JSONDecodeError:
                    logger.error("Failed to decode message")
                    continue

        except RuntimeError as e:
            logger.error(f"Consumer stopped: {e}")
        finally:
            self._consumer.close()

    def commit(self):
        self._consumer.commit()
