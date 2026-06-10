import json
from confluent_kafka import Producer

from public_api.src.public_api.core.config import settings


class KafkaEventPublisher:
    def __init__(self):
        self._producer = Producer(settings.producer_config)

    @staticmethod
    def delivery_callback(err, msg) -> None:
        if err:
            print(f"ERROR: Message failed delivery: {err}")
            return

        key = msg.key().decode("utf-8") if msg.key() else None
        value = msg.value().decode("utf-8") if msg.value() else None

        print(
            f"Produced event to topic={msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()} "
            f"key={key} value={value}"
        )

    async def publish(self, topic: str, key: str, event: dict) -> None:
        self._producer.produce(
            topic=topic,
            key=key.encode('utf-8'),
            value=json.dumps(event).encode('utf-8'),
            callback=self.delivery_callback,
        )
        self._producer.poll(0)
        self._producer.flush()
