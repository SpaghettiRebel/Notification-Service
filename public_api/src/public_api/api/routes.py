from fastapi import APIRouter, Body, status

from .schemas import SendingMessage
from public_api.src.public_api.use_cases.publish_notification import NotificationPublisher


router = APIRouter()


@router.post('/birthday', status_code=status.HTTP_201_CREATED)
async def send_birthday_greeting(publisher: NotificationPublisher, msg: SendingMessage = Body()):
    return await publisher.publish_birthday_message(msg)


@router.post('/christmas', status_code=status.HTTP_201_CREATED)
async def send_christmas_greeting(publisher: NotificationPublisher, msg: SendingMessage = Body()):
    return await publisher.publish_christmas_message(msg)
