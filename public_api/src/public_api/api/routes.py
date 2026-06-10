from fastapi import APIRouter

from .schemas import SendingMessage
from public_api.src.public_api.use_cases.publish_notification import NotificationPublisher


router = APIRouter()


@router.post('/message')
async def send_message(msg: SendingMessage, publisher: NotificationPublisher):
    return await publisher.publish_message(msg)
