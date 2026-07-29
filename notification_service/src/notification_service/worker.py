import asyncio

from notification_service.infrastructure.kafka.consumer import KafkaEventConsumer
from notification_service.use_cases.process_notification import ProcessNotification
from notification_service.core.logger import get_logger


logger = get_logger(__name__)


async def main() -> None:
    consumer = KafkaEventConsumer()
    use_case = ProcessNotification()

    logger.info("Notification worker started successfully.")

    try:
        async for msg_data in consumer.listen():
            await use_case.process(msg_data)
            await consumer.commit()
    except asyncio.CancelledError:
        logger.info("Worker task was cancelled.")
    finally:
        logger.info("Worker shut down gracefully.")


def run() -> None:
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Worker stopped by user (KeyboardInterrupt).")


if __name__ == '__main__':
    run()
