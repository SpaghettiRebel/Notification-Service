from datetime import datetime, timezone, timedelta

from uuid_utils import uuid7
from fastapi import HTTPException, status

from public_api.infrastructure.kafka.producer import KafkaEventPublisher
from public_api.infrastructure.kafka.topics import get_topic_name
from public_api.api.schemas import SendingMessage, MsgType
from public_api.core.logger import get_logger


logger = get_logger(__name__)


class NotificationPublisher:
    def __init__(self, kafka_publisher: KafkaEventPublisher):
        self._kafka_publisher = kafka_publisher

    async def publish_birthday_message(self, msg: SendingMessage):
        """Sends Birthday message through TG and SMS only"""
        if msg.msg_type == MsgType.EMAIL:
            logger.warning(
                "birthday_email_rejected",
                extra={"channel": msg.msg_type.value},
            )
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="You cannot send an Email with birthday message")

        event = {
            'event_id': str(uuid7()),
            'event_type': 'birthday',
            'channel': msg.msg_type.value,
            'msg_text': msg.msg_text,
            'created_at': datetime.now(timezone(timedelta(hours=3))).isoformat(),
        }

        logger.info(
            "notification_event_created",
            extra={
                "event_id": event["event_id"],
                "event_type": event["event_type"],
                "channel": event["channel"],
            },
        )

        topic = get_topic_name(msg_type=msg.msg_type.value)
        await self._kafka_publisher.publish(topic=topic, key=event['event_id'], event=event)

        logger.info(
            "notification_event_published",
            extra={
                "event_id": event["event_id"],
                "topic": topic,
            },
        )

    async def publish_christmas_message(self, msg: SendingMessage):
        """Sends Christmas message through TG and Email only"""
        if msg.msg_type == MsgType.SMS:
            logger.warning(
                "birthday_email_rejected",
                extra={"channel": msg.msg_type.value},
            )
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="You cannot send an SMS with Christmas message")

        event = {
            'event_id': str(uuid7()),
            'event_type': 'christmas',
            'channel': msg.msg_type.value,
            'msg_text': msg.msg_text,
            'created_at': datetime.now(timezone(timedelta(hours=3))).isoformat(),
        }

        logger.info(
            "notification_event_created",
            extra={
                "event_id": event["event_id"],
                "event_type": event["event_type"],
                "channel": event["channel"],
            },
        )

        topic = get_topic_name(msg_type=msg.msg_type.value)
        await self._kafka_publisher.publish(topic=topic, key=event['event_id'], event=event)

        logger.info(
            "notification_event_published",
            extra={
                "event_id": event["event_id"],
                "topic": topic,
            },
        )
