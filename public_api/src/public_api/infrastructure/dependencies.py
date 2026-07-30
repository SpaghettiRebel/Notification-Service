from fastapi import Depends, Request

from public_api.use_cases.publish_notification import NotificationPublisher
from public_api.infrastructure.kafka.producer import KafkaEventPublisher


async def get_kafka_publisher(request: Request) -> KafkaEventPublisher:
    return request.app.state.kafka_publisher


async def get_notification_publisher(
        kafka_publisher: KafkaEventPublisher = Depends(get_kafka_publisher)
) -> NotificationPublisher:
    return NotificationPublisher(kafka_publisher)
