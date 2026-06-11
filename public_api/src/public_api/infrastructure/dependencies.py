from functools import lru_cache

from fastapi import Depends

from public_api.use_cases.publish_notification import NotificationPublisher
from public_api.infrastructure.kafka.producer import KafkaEventPublisher


@lru_cache
async def get_kafka_publisher() -> KafkaEventPublisher:
    return KafkaEventPublisher()


async def get_notification_publisher(
        kafka_publisher: KafkaEventPublisher = Depends(get_kafka_publisher)
) -> NotificationPublisher:
    return NotificationPublisher(kafka_publisher)
