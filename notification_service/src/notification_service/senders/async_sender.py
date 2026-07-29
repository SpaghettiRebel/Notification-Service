import random

from notification_service.domain.exceptions import TgServerUnavailableError, SmsServerUnavailableError, SmtpServerUnavailableError

class Sender:
    @staticmethod
    async def email_send(msg_data: dict) -> None:
        if random.randint(1, 5) == 1:
            raise SmtpServerUnavailableError

    @staticmethod
    async def sms_send(msg_data: dict) -> None:
        if random.randint(1, 5) == 1:
            raise SmsServerUnavailableError

    @staticmethod
    async def tg_send(msg_data: dict) -> None:
        if random.randint(1, 5) == 1:
            raise TgServerUnavailableError

asender = Sender()
