from tenacity import retry, wait_exponential

from notification_service.core.logger import get_logger
from notification_service.infrastructure.db import get_pg_repo
from notification_service.senders import sms_sender, tg_sender, email_sender


logger = get_logger(__name__)


class ProcessNotification:
    def __init__(self):
        self.pg_repo = get_pg_repo()

    @staticmethod
    @retry(wait=wait_exponential(multiplier=1, min=4, max=10))
    def send_on_topic(topic, msg_data):
        if topic == 'notification.tg':
            tg_sender(msg_data)
        elif topic == 'notification.email':
            email_sender(msg_data)
        elif topic == 'notification.sms':
            sms_sender(msg_data)
        else:
            logger.info("Invalid topic.")

    def process(self, msg_data: dict):
        msg_topic = msg_data['topic']
        event_id = msg_data['key']

        response_event_id = self.pg_repo.acquire_lock(event_id)
        if response_event_id is None:
            return

        try:
            self.send_on_topic(msg_topic, msg_data)
        except Exception as e:
            ... # TODO: send to dlq
            self.pg_repo.update_failed(event_id)
        else:
            self.pg_repo.update_completed(event_id)
