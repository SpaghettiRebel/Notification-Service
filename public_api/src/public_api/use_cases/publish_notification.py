from public_api.src.public_api.infrastructure.kafka.producer import KafkaEventPublisher
from public_api.src.public_api.api.schemas import SendingMessage


class NotificationPublisher:
    def __init__(self, kafka_publisher: KafkaEventPublisher):
        kafka_publisher = kafka_publisher

    async def publish_message(self, msg: SendingMessage):
        ...
