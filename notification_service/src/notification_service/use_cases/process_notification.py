import asyncio
from datetime import datetime, timezone

from pydantic import ValidationError
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from notification_service.core.config import settings
from notification_service.core.logger import get_logger
from notification_service.domain.events import NotificationEvent
from notification_service.domain.exceptions import (
    SmsServerUnavailableError,
    SmtpServerUnavailableError,
    TgServerUnavailableError,
    UnsupportedNotificationTopicError,
)
from notification_service.infrastructure.db import (
    AcquisitionState,
    NotificationRepository,
    NotificationStatus,
    async_session_factory,
)
from notification_service.infrastructure.kafka.producer import KafkaEventPublisher
from notification_service.senders.async_sender import asender


logger = get_logger(__name__)

RETRIABLE_SENDER_ERRORS = (
    TgServerUnavailableError,
    SmsServerUnavailableError,
    SmtpServerUnavailableError,
    TimeoutError,
)


class ProcessNotification:
    def __init__(
        self,
        dlq_publisher=None,
        sender=None,
        session_factory=None,
        repository_factory=None,
    ) -> None:
        self.dlq_publisher = dlq_publisher or KafkaEventPublisher()
        self.sender = sender or asender
        self.session_factory = session_factory or async_session_factory
        self.repository_factory = repository_factory or NotificationRepository

    async def _send_once(
        self,
        topic: str,
        payload: dict,
        event_id: str,
    ) -> None:
        if topic == 'notification.tg':
            await self.sender.tg_send(payload, idempotency_key=event_id)
        elif topic == 'notification.email':
            await self.sender.email_send(payload, idempotency_key=event_id)
        elif topic == 'notification.sms':
            await self.sender.sms_send(payload, idempotency_key=event_id)
        else:
            raise UnsupportedNotificationTopicError(topic)

    async def send_on_topic(
        self,
        topic: str,
        payload: dict,
        event_id: str,
    ) -> None:
        retrying = AsyncRetrying(
            stop=stop_after_attempt(settings.sender_max_attempts),
            wait=wait_exponential(
                multiplier=1,
                min=settings.sender_retry_min_seconds,
                max=settings.sender_retry_max_seconds,
            ),
            retry=retry_if_exception_type(RETRIABLE_SENDER_ERRORS),
            reraise=True,
        )

        async for attempt in retrying:
            with attempt:
                async with asyncio.timeout(
                    settings.sender_attempt_timeout_seconds
                ):
                    await self._send_once(topic, payload, event_id)

    async def _publish_invalid_message(
        self,
        msg_data: dict,
        error: str,
    ) -> None:
        topic = str(msg_data.get('topic'))
        partition = msg_data.get('partition')
        offset = msg_data.get('offset')
        fallback_key = f"{topic}:{partition}:{offset}"
        key = msg_data.get('key') or fallback_key

        await self.dlq_publisher.publish_dlq(
            topic='notification.invalid.dlq',
            key=str(key),
            event={
                'event_id': str(key),
                'event_type': 'invalid_notification',
                'source_topic': topic,
                'partition': partition,
                'offset': offset,
                'error': error,
                'raw_payload': msg_data.get('raw_payload'),
            },
        )

    async def process(self, msg_data: dict) -> None:
        """Process a Kafka event using recoverable lease semantics."""
        if validation_error := msg_data.get('validation_error'):
            await self._publish_invalid_message(msg_data, validation_error)
            return

        msg_topic = str(msg_data.get('topic'))
        event_id = msg_data.get('key')
        payload = msg_data.get('payload', {})

        try:
            event = NotificationEvent.model_validate(payload)
            if event.event_id != event_id:
                raise ValueError("Kafka key does not match payload event_id")
            if msg_topic != f"notification.{event.channel}":
                raise ValueError("Kafka topic does not match payload channel")
        except (ValidationError, ValueError) as exc:
            invalid_data = dict(msg_data)
            invalid_data['raw_payload'] = str(payload)[:4096]
            await self._publish_invalid_message(invalid_data, str(exc))
            return

        event_id = event.event_id
        validated_payload = event.model_dump(mode='json')

        async with self.session_factory() as session:
            repo = self.repository_factory(session)

            while True:
                acquisition = await repo.acquire_lock(
                    event_id,
                    lease_seconds=settings.processing_lease_seconds,
                )
                if acquisition.state == AcquisitionState.ACQUIRED:
                    break
                if acquisition.state == AcquisitionState.COMPLETED:
                    logger.info("Event %s is already completed. Skipping.", event_id)
                    return

                retry_after = 1.0
                if acquisition.locked_until is not None:
                    retry_after = max(
                        (
                            acquisition.locked_until - datetime.now(timezone.utc)
                        ).total_seconds(),
                        0.1,
                    )

                logger.warning(
                    "Event %s is being processed; retrying lock in %.1f seconds.",
                    event_id,
                    retry_after,
                )
                await asyncio.sleep(retry_after)

            try:
                await self.send_on_topic(
                    msg_topic,
                    validated_payload,
                    event_id,
                )
            except RETRIABLE_SENDER_ERRORS as exc:
                logger.error(
                    "Failed to send message for event %s after retries: %s",
                    event_id,
                    exc,
                )

                await self.dlq_publisher.publish_dlq(
                    topic=f"{msg_topic}.dlq",
                    key=event_id,
                    event={
                        **validated_payload,
                        'source_topic': msg_topic,
                        'error': str(exc),
                    },
                )

                await repo.update_status(
                    event_id,
                    NotificationStatus.FAILED,
                    error=str(exc),
                )
            else:
                await repo.update_status(event_id, NotificationStatus.COMPLETED)

    async def close(self) -> None:
        await self.dlq_publisher.close()
