import json
from collections.abc import AsyncIterator

from confluent_kafka import KafkaError, KafkaException, Message
from confluent_kafka.aio import AIOConsumer

from notification_service.core.config import settings
from notification_service.core.logger import get_logger
from notification_service.infrastructure.kafka.topics import get_topics


logger = get_logger(__name__)


class KafkaEventConsumer:
    def __init__(self) -> None:
        self._consumer = AIOConsumer(settings.consumer_config)
        self._current_message: Message | None = None

    async def listen(self) -> AsyncIterator[dict]:
        try:
            await self._consumer.subscribe(get_topics())
            while True:
                msg = await self._consumer.poll(1.0)
                if msg is None:
                    continue

                error = msg.error()
                if error is not None:
                    if error.code() == KafkaError._PARTITION_EOF:
                        logger.info(
                            "Reached end of Kafka partition %s[%s] at offset %s",
                            msg.topic(),
                            msg.partition(),
                            msg.offset(),
                        )
                        continue
                    raise KafkaException(error)

                raw_value = msg.value()
                raw_key = msg.key()
                key = None
                self._current_message = msg

                try:
                    if raw_key is not None:
                        key = raw_key.decode('utf-8')
                    if raw_value is None:
                        raise ValueError("Kafka message has no value")

                    raw_payload = raw_value.decode('utf-8')
                    payload = json.loads(raw_payload)
                    if not isinstance(payload, dict):
                        raise ValueError("Kafka payload must be a JSON object")

                    if key is None:
                        key = payload.get('event_id')
                    if not isinstance(key, str) or not key:
                        raise ValueError("Kafka message key/event_id is missing")

                    yield {
                        'topic': msg.topic(),
                        'partition': msg.partition(),
                        'offset': msg.offset(),
                        'key': key,
                        'payload': payload,
                    }
                    continue

                except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as exc:
                    logger.warning(
                        "Invalid Kafka message at %s[%s] offset %s: %s",
                        msg.topic(),
                        msg.partition(),
                        msg.offset(),
                        exc,
                    )
                    yield {
                        'topic': msg.topic(),
                        'partition': msg.partition(),
                        'offset': msg.offset(),
                        'key': key,
                        'payload': {},
                        'validation_error': str(exc),
                        'raw_payload': (
                            raw_value.decode('utf-8', errors='replace')[:4096]
                            if raw_value is not None
                            else None
                        ),
                    }

        finally:
            await self._consumer.close()

    async def commit(self) -> None:
        if self._current_message is None:
            raise RuntimeError("Cannot commit before receiving a message")

        committed_partitions = await self._consumer.commit(
            message=self._current_message,
            asynchronous=False,
        )

        commit_errors = [
            partition.error
            for partition in (committed_partitions or [])
            if getattr(partition, "error", None) is not None
        ]
        if commit_errors:
            raise KafkaException(commit_errors[0])

        self._current_message = None
