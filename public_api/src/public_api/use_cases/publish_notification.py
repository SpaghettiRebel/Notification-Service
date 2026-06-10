from fastapi import HTTPException, status

from public_api.src.public_api.infrastructure.kafka.producer import KafkaEventPublisher
from public_api.src.public_api.infrastructure.kafka.topics import get_topic_name
from public_api.src.public_api.api.schemas import SendingMessage, MsgType


class NotificationPublisher:
    def __init__(self, kafka_publisher: KafkaEventPublisher):
        self.kafka_publisher = kafka_publisher

    async def publish_birthday_message(self, msg: SendingMessage):
        """Sends Birthday message through TG and SMS only"""
        if msg.msg_type == MsgType.EMAIL:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="You cannot send an Email with birthday message")

        # TODO: добавить логику формирования payload сообщения

        topic = get_topic_name(msg_type=msg.msg_type)
        await self.kafka_publisher.publish(topic=topic, msg=msg.msg_text)
        # TODO: испарвить вызов publish

    async def publish_christmas_message(self, msg: SendingMessage):
        """Sends Christmas message through TG and Email only"""
        if msg.msg_type == MsgType.SMS:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="You cannot send an SMS with Christmas message")

        # TODO: добавить логику формирования payload сообщения

        topic = get_topic_name(msg_type=msg.msg_type)
        await self.kafka_publisher.publish(topic=topic, msg=msg.msg_text)
        # TODO: испарвить вызов publish
