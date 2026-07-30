from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field


class NotificationEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(min_length=1, max_length=128)
    event_type: Literal["birthday", "christmas"]
    channel: Literal["tg", "email", "sms"]
    msg_text: str = Field(min_length=10, max_length=400)
    created_at: AwareDatetime
