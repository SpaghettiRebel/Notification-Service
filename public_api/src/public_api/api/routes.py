from fastapi import APIRouter, Body, Depends, status

from public_api.src.public_api.api.schemas import SendingMessage
from public_api.src.public_api.use_cases.publish_notification import NotificationPublisher


router = APIRouter()


@router.post('/birthday', status_code=status.HTTP_202_ACCEPTED)
async def send_birthday_greeting(publisher: NotificationPublisher = Depends(),
                                 # TODO: доделать Depends
                                 msg: SendingMessage = Body()):
    await publisher.publish_birthday_message(msg)
    return {"status": "accepted"}


@router.post('/christmas', status_code=status.HTTP_202_ACCEPTED)
async def send_christmas_greeting(publisher: NotificationPublisher = Depends(),
                                  # TODO: доделать Depends
                                  msg: SendingMessage = Body()):
    await publisher.publish_christmas_message(msg)
    return {"status": "accepted"}
