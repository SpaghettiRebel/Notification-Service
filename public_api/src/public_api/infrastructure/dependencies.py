from fastapi import Depends

from public_api.src.public_api.use_cases.publish_notification import NotificationPublisher
from public_api.src.public_api.infrastructure.kafka.producer import KafkaEventPublisher


async def get_kafka_publisher():
    return KafkaEventPublisher()


async def get_notification_publisher(kafka_publisher: KafkaEventPublisher = Depends(get_kafka_publisher)):
    return NotificationPublisher(kafka_publisher)
