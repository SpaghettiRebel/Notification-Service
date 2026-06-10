from enum import Enum

from pydantic import BaseModel


class MsgType(Enum, str):
    TG = 1
    EMAIL = 2
    SMS = 3


class SendingMessage(BaseModel):
    msg_type: MsgType
    msg_text: str
