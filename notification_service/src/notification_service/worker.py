from notification_service.infrastructure.kafka.consumer import KafkaEventConsumer
from notification_service.use_cases.process_notification import ProcessNotification
from notification_service.core.logger import get_logger


logger = get_logger(__name__)


def run():
    consumer = KafkaEventConsumer()
    use_case = ProcessNotification()

    try:
        for msg in consumer.listen():
            use_case.process(msg)
            consumer.commit()

    except KeyboardInterrupt:
        logger.info("Worker stopped")


if __name__ == '__main__':
    run()
