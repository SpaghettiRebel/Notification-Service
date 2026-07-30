import asyncio

from notification_service.infrastructure.kafka.consumer import KafkaEventConsumer
from notification_service.use_cases.process_notification import ProcessNotification
from notification_service.core.logger import get_logger, setup_logging


setup_logging()
logger = get_logger(__name__)


async def main() -> None:
    consumer = KafkaEventConsumer()
    use_case = ProcessNotification()
    messages = consumer.listen()

    logger.info("Notification worker started successfully.")

    try:
        async for msg_data in messages:
            await use_case.process(msg_data)
            await consumer.commit()
    except asyncio.CancelledError:
        logger.info("Worker task was cancelled.")
    except Exception:
        logger.exception("Worker stopped because of an unhandled processing error.")
        raise
    finally:
        try:
            await messages.aclose()
        finally:
            await use_case.close()
        logger.info("Worker shut down gracefully.")


def run() -> None:
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Worker stopped by user (KeyboardInterrupt).")


if __name__ == '__main__':
    run()
