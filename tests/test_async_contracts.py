import asyncio
import enum
import inspect
import os
import sys
import types
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "notification_service" / "src"))
sys.path.insert(0, str(PROJECT_ROOT / "public_api" / "src"))

os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_PORT", "5432")
os.environ.setdefault("DB_USER", "user")
os.environ.setdefault("DB_PASS", "password")
os.environ.setdefault("DB_NAME", "notification_db")
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

confluent_kafka = types.ModuleType("confluent_kafka")
confluent_kafka.Consumer = object
confluent_kafka.KafkaException = RuntimeError


class StubKafkaError:
    _PARTITION_EOF = -191


confluent_kafka.KafkaError = StubKafkaError
confluent_kafka.Message = object
confluent_kafka.Producer = object
sys.modules["confluent_kafka"] = confluent_kafka


class StubAIOProducer:
    def __init__(self, _config, **_kwargs) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True

    async def list_topics(self, timeout: float):
        return {"timeout": timeout}


confluent_kafka_aio = types.ModuleType("confluent_kafka.aio")
confluent_kafka_aio.AIOConsumer = object
confluent_kafka_aio.AIOProducer = StubAIOProducer
sys.modules["confluent_kafka.aio"] = confluent_kafka_aio

notification_config = types.ModuleType(
    "notification_service.core.config"
)
notification_config.settings = SimpleNamespace(
    consumer_config={},
    producer_config={},
    kafka_delivery_timeout_ms=10_000,
    processing_lease_seconds=60,
    sender_max_attempts=3,
    sender_attempt_timeout_seconds=10,
    sender_retry_min_seconds=0,
    sender_retry_max_seconds=0,
)
sys.modules["notification_service.core.config"] = notification_config

notification_db = types.ModuleType(
    "notification_service.infrastructure.db"
)
notification_db.async_session_factory = object()
notification_db.NotificationRepository = object


class StubAcquisitionState(str, enum.Enum):
    ACQUIRED = "acquired"
    COMPLETED = "completed"
    BUSY = "busy"


notification_db.AcquisitionState = StubAcquisitionState
notification_db.NotificationStatus = SimpleNamespace(
    FAILED="failed",
    COMPLETED="completed",
)
sys.modules["notification_service.infrastructure.db"] = notification_db

public_api_config = types.ModuleType("public_api.core.config")
public_api_config.settings = SimpleNamespace(
    producer_config={},
    kafka_delivery_timeout_ms=10_000,
)
sys.modules["public_api.core.config"] = public_api_config

from notification_service.infrastructure.kafka.consumer import KafkaEventConsumer
from notification_service.infrastructure.kafka.producer import (
    KafkaEventPublisher as WorkerKafkaEventPublisher,
)
from notification_service.domain.exceptions import SmtpServerUnavailableError
from notification_service.use_cases.process_notification import ProcessNotification
from public_api.api.schemas import MsgType, SendingMessage
from public_api.infrastructure.kafka.producer import (
    KafkaEventPublisher as ApiKafkaEventPublisher,
)
from public_api.use_cases.publish_notification import NotificationPublisher


class FakeMessage:
    def topic(self) -> str:
        return "notification.email"

    def key(self) -> bytes:
        return b"event-1"

    def value(self) -> bytes:
        return b'{"event_id": "event-1", "msg_text": "hello"}'

    def error(self):
        return None

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return 1


class InvalidPayloadMessage(FakeMessage):
    def key(self):
        return None

    def value(self) -> bytes:
        return b'[]'


class FakeConsumer:
    def __init__(self, message=None) -> None:
        self.message = message or FakeMessage()
        self.commits = []
        self.closed = False
        self.subscriptions = []

    async def subscribe(self, topics) -> None:
        self.subscriptions.append(topics)

    async def poll(self, _timeout: float):
        return self.message

    async def commit(self, **kwargs):
        self.commits.append(kwargs)
        return getattr(self, "commit_result", [])

    async def close(self) -> None:
        self.closed = True


class FailingSubscribeConsumer(FakeConsumer):
    async def subscribe(self, topics) -> None:
        raise RuntimeError("subscribe failed")


class FakeProducer:
    def __init__(self) -> None:
        self.messages = []
        self.closed = False

    async def produce(self, **kwargs):
        self.messages.append(kwargs)
        delivery = asyncio.get_running_loop().create_future()
        delivery.set_result(FakeMessage())
        return delivery

    async def close(self) -> None:
        self.closed = True

    async def list_topics(self, timeout: float):
        self.list_topics_timeout = timeout
        return {}


class FakeDlqPublisher:
    def __init__(self) -> None:
        self.messages = []
        self.closed = False

    async def publish_dlq(self, topic: str, key: str, event: dict) -> None:
        self.messages.append({"topic": topic, "key": key, "event": event})

    async def close(self) -> None:
        self.closed = True


class FakeSender:
    def __init__(self, failures: int = 0) -> None:
        self.failures = failures
        self.calls = []

    async def email_send(self, payload: dict, idempotency_key: str) -> None:
        self.calls.append((payload, idempotency_key))
        if len(self.calls) <= self.failures:
            raise SmtpServerUnavailableError("SMTP unavailable")

    async def sms_send(self, payload: dict, idempotency_key: str) -> None:
        self.calls.append((payload, idempotency_key))

    async def tg_send(self, payload: dict, idempotency_key: str) -> None:
        self.calls.append((payload, idempotency_key))


class CrashingSender(FakeSender):
    async def email_send(self, payload: dict, idempotency_key: str) -> None:
        raise RuntimeError("programming error")


class FakeRepository:
    def __init__(self, acquisitions) -> None:
        self.acquisitions = list(acquisitions)
        self.acquire_calls = []
        self.status_updates = []

    async def acquire_lock(self, event_id: str, lease_seconds: int):
        self.acquire_calls.append((event_id, lease_seconds))
        return self.acquisitions.pop(0)

    async def update_status(self, event_id, status, error=None) -> None:
        self.status_updates.append((event_id, status, error))


class FakeSessionContext:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, *_args) -> None:
        return None


class FakeSessionFactory:
    def __call__(self):
        return FakeSessionContext()


class FakeApiPublisher:
    def __init__(self) -> None:
        self.messages = []

    async def publish(self, topic: str, key: str, event: dict) -> None:
        self.messages.append({"topic": topic, "key": key, "event": event})


def valid_email_message() -> dict:
    return {
        "topic": "notification.email",
        "partition": 0,
        "offset": 1,
        "key": "event-1",
        "payload": {
            "event_id": "event-1",
            "event_type": "christmas",
            "channel": "email",
            "msg_text": "A sufficiently long message",
            "created_at": datetime.now(timezone.utc).isoformat(),
        },
    }


class AsyncContractTests(unittest.IsolatedAsyncioTestCase):
    def test_public_interfaces_are_async(self) -> None:
        self.assertTrue(inspect.isasyncgenfunction(KafkaEventConsumer.listen))
        self.assertTrue(inspect.iscoroutinefunction(KafkaEventConsumer.commit))
        self.assertTrue(inspect.iscoroutinefunction(ProcessNotification.process))
        self.assertTrue(
            inspect.iscoroutinefunction(WorkerKafkaEventPublisher.publish_dlq)
        )
        self.assertTrue(inspect.iscoroutinefunction(ApiKafkaEventPublisher.publish))

    async def test_api_lifespan_reuses_and_closes_producer(self) -> None:
        from public_api.main import app, lifespan

        async with lifespan(app):
            publisher = app.state.kafka_publisher
            self.assertIsInstance(publisher, ApiKafkaEventPublisher)

        self.assertTrue(publisher._producer.closed)

    async def test_consumer_polls_and_commits_asynchronously(self) -> None:
        fake_consumer = FakeConsumer()
        consumer = KafkaEventConsumer.__new__(KafkaEventConsumer)
        consumer._consumer = fake_consumer
        consumer._current_message = None

        messages = consumer.listen()
        event = await anext(messages)

        self.assertEqual(event["topic"], "notification.email")
        self.assertEqual(event["key"], "event-1")
        await consumer.commit()
        await messages.aclose()

        self.assertEqual(
            fake_consumer.commits,
            [{"message": fake_consumer.message, "asynchronous": False}],
        )
        self.assertTrue(fake_consumer.closed)

    async def test_consumer_closes_when_subscription_fails(self) -> None:
        fake_consumer = FailingSubscribeConsumer()
        consumer = KafkaEventConsumer.__new__(KafkaEventConsumer)
        consumer._consumer = fake_consumer
        consumer._current_message = None

        messages = consumer.listen()
        with self.assertRaisesRegex(RuntimeError, "subscribe failed"):
            await anext(messages)

        self.assertTrue(fake_consumer.closed)

    async def test_producers_publish_asynchronously(self) -> None:
        worker_producer = WorkerKafkaEventPublisher.__new__(
            WorkerKafkaEventPublisher
        )
        worker_producer._producer = FakeProducer()

        api_producer = ApiKafkaEventPublisher.__new__(ApiKafkaEventPublisher)
        api_producer._producer = FakeProducer()

        event = {"event_id": "event-1", "event_type": "test"}
        await worker_producer.publish_dlq(
            "notification.email.dlq",
            "event-1",
            event,
        )
        await api_producer.publish("notification.email", "event-1", event)
        await worker_producer.close()
        await api_producer.close()

        self.assertEqual(len(worker_producer._producer.messages), 1)
        self.assertEqual(len(api_producer._producer.messages), 1)
        self.assertTrue(worker_producer._producer.closed)
        self.assertTrue(api_producer._producer.closed)

    async def test_sender_retry_is_bounded_and_routes_to_dlq(self) -> None:
        repository = FakeRepository(
            [SimpleNamespace(state=StubAcquisitionState.ACQUIRED, locked_until=None)]
        )
        sender = FakeSender(failures=10)
        dlq = FakeDlqPublisher()
        processor = ProcessNotification(
            dlq_publisher=dlq,
            sender=sender,
            session_factory=FakeSessionFactory(),
            repository_factory=lambda _session: repository,
        )

        await processor.process(valid_email_message())

        self.assertEqual(len(sender.calls), 3)
        self.assertEqual(dlq.messages[0]["topic"], "notification.email.dlq")
        self.assertEqual(
            repository.status_updates,
            [("event-1", "failed", "SMTP unavailable")],
        )

    async def test_busy_processing_lease_is_retried_without_skipping(self) -> None:
        repository = FakeRepository(
            [
                SimpleNamespace(
                    state=StubAcquisitionState.BUSY,
                    locked_until=datetime.now(timezone.utc),
                ),
                SimpleNamespace(
                    state=StubAcquisitionState.ACQUIRED,
                    locked_until=None,
                ),
            ]
        )
        sender = FakeSender()
        processor = ProcessNotification(
            dlq_publisher=FakeDlqPublisher(),
            sender=sender,
            session_factory=FakeSessionFactory(),
            repository_factory=lambda _session: repository,
        )

        await processor.process(valid_email_message())

        self.assertEqual(len(repository.acquire_calls), 2)
        self.assertEqual(len(sender.calls), 1)
        self.assertEqual(
            repository.status_updates,
            [("event-1", "completed", None)],
        )

    async def test_unexpected_sender_error_is_not_committed_as_failed(self) -> None:
        repository = FakeRepository(
            [SimpleNamespace(state=StubAcquisitionState.ACQUIRED, locked_until=None)]
        )
        dlq = FakeDlqPublisher()
        processor = ProcessNotification(
            dlq_publisher=dlq,
            sender=CrashingSender(),
            session_factory=FakeSessionFactory(),
            repository_factory=lambda _session: repository,
        )

        with self.assertRaisesRegex(RuntimeError, "programming error"):
            await processor.process(valid_email_message())

        self.assertEqual(dlq.messages, [])
        self.assertEqual(repository.status_updates, [])

    async def test_invalid_message_is_confirmed_to_poison_dlq(self) -> None:
        dlq = FakeDlqPublisher()
        processor = ProcessNotification(
            dlq_publisher=dlq,
            sender=FakeSender(),
            session_factory=FakeSessionFactory(),
            repository_factory=lambda _session: None,
        )

        await processor.process(
            {
                "topic": "notification.email",
                "partition": 2,
                "offset": 10,
                "key": None,
                "payload": {},
                "validation_error": "invalid JSON",
                "raw_payload": "{",
            }
        )

        self.assertEqual(dlq.messages[0]["topic"], "notification.invalid.dlq")
        self.assertEqual(
            dlq.messages[0]["key"],
            "notification.email:2:10",
        )

    async def test_non_object_kafka_payload_is_yielded_as_invalid(self) -> None:
        fake_consumer = FakeConsumer(InvalidPayloadMessage())
        consumer = KafkaEventConsumer.__new__(KafkaEventConsumer)
        consumer._consumer = fake_consumer
        consumer._current_message = None

        messages = consumer.listen()
        event = await anext(messages)
        await messages.aclose()

        self.assertIn("JSON object", event["validation_error"])
        self.assertEqual(event["raw_payload"], "[]")
        self.assertTrue(fake_consumer.closed)

    async def test_commit_partition_error_is_not_ignored(self) -> None:
        fake_consumer = FakeConsumer()
        fake_consumer.commit_result = [
            SimpleNamespace(error=RuntimeError("commit failed"))
        ]
        consumer = KafkaEventConsumer.__new__(KafkaEventConsumer)
        consumer._consumer = fake_consumer
        consumer._current_message = fake_consumer.message

        with self.assertRaisesRegex(RuntimeError, "commit failed"):
            await consumer.commit()

        self.assertIs(consumer._current_message, fake_consumer.message)

    async def test_api_idempotency_key_produces_stable_event_id(self) -> None:
        kafka = FakeApiPublisher()
        publisher = NotificationPublisher(kafka)
        message = SendingMessage(
            msg_type=MsgType.TG,
            msg_text="A sufficiently long message",
        )

        first = await publisher.publish_birthday_message(
            message,
            "request-key-123",
        )
        second = await publisher.publish_birthday_message(
            message,
            "request-key-123",
        )

        self.assertEqual(first, second)
        self.assertEqual(kafka.messages[0]["key"], kafka.messages[1]["key"])

        christmas = await publisher.publish_christmas_message(
            message,
            "request-key-123",
        )
        self.assertNotEqual(first, christmas)


if __name__ == "__main__":
    unittest.main()
