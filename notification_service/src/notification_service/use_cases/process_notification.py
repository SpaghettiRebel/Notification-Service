import asyncio
from tenacity import retry, wait_exponential

from notification_service.core.logger import get_logger
from notification_service.infrastructure.db import async_session_factory, NotificationRepository, NotificationStatus
from notification_service.infrastructure.kafka.producer import KafkaEventPublisher
from notification_service.senders.async_sender import asender

logger = get_logger(__name__)


class ProcessNotification:
    def __init__(self):
        self.dlq_publisher = KafkaEventPublisher()

    @staticmethod
    @retry(wait=wait_exponential(multiplier=1, min=4, max=10))
    async def send_on_topic(topic: str, payload: dict) -> None:
        if topic == 'notification.tg':
            await asender.tg_send(payload)
        elif topic == 'notification.email':
            await asender.email_send(payload)
        elif topic == 'notification.sms':
            await asender.sms_send(payload)
        else:
            logger.warning(f"Invalid topic: {topic}")

    async def _async_process(self, msg_data: dict) -> None:
        """Вся асинхронная работа с БД и DLQ"""
        msg_topic = msg_data.get('topic')
        event_id = msg_data.get('key')
        payload = msg_data.get('payload', {})

        msg_topic = str(msg_topic)
        event_id = str(event_id)

        async with async_session_factory() as session:
            repo = NotificationRepository(session)

            response_event_id = await repo.acquire_lock(event_id)
            if response_event_id is None:
                logger.info(f"Event {event_id} is already processed or locked. Skipping.")
                return

            try:
                await self.send_on_topic(msg_topic, payload)
            except Exception as e:
                logger.error(f"Failed to send message for event {event_id}: {e}")

                dlq_topic = f"{msg_topic}.dlq"
                await self.dlq_publisher.publish_dlq(
                    topic=dlq_topic,
                    key=event_id,
                    event=payload
                )

                await repo.update_status(event_id, NotificationStatus.FAILED)
            else:
                await repo.update_status(event_id, NotificationStatus.COMPLETED)

    def process(self, msg_data: dict) -> None:
        """Синхронная точка входа для Kafka Consumer"""
        asyncio.run(self._async_process(msg_data))
