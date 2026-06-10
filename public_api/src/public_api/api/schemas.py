from enum import Enum

from pydantic import BaseModel, Field


class MsgType(Enum, str):
    TG = 'tg'
    EMAIL = 'email'
    SMS = 'sms'


class SendingMessage(BaseModel):
    msg_type: MsgType
    msg_text: str = Field(min_length=10, max_length=400)
