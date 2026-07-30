from typing import Annotated

from fastapi import APIRouter, Depends, Header, status

from public_api.api.schemas import SendingMessage
from public_api.use_cases.publish_notification import NotificationPublisher
from public_api.infrastructure.dependencies import get_notification_publisher
from public_api.core.logger import get_logger


logger = get_logger(__name__)

router = APIRouter()

IdempotencyKey = Annotated[
    str,
    Header(
        alias='Idempotency-Key',
        min_length=8,
        max_length=128,
    ),
]
PublisherDependency = Annotated[
    NotificationPublisher,
    Depends(get_notification_publisher),
]


@router.post('/birthday', status_code=status.HTTP_202_ACCEPTED)
async def send_birthday_greeting(
    msg: SendingMessage,
    idempotency_key: IdempotencyKey,
    publisher: PublisherDependency,
):
    event_id = await publisher.publish_birthday_message(msg, idempotency_key)
    return {"status": "accepted", "event_id": event_id}


@router.post('/christmas', status_code=status.HTTP_202_ACCEPTED)
async def send_christmas_greeting(
    msg: SendingMessage,
    idempotency_key: IdempotencyKey,
    publisher: PublisherDependency,
):
    event_id = await publisher.publish_christmas_message(msg, idempotency_key)
    return {"status": "accepted", "event_id": event_id}
