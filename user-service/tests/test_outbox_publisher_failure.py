import pytest

from sqlalchemy import select
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.kafka.outbox_publisher import OutboxPublisher
from app.models.outbox_event import OutboxEvent
from tests.config import test_settings


class FailingProducer:
    async def send(self, topic: str, value: bytes) -> None:
        raise RuntimeError("Kafka is unavailable")


class SuccessfulProducer:
    def __init__(self) -> None:
        self.sent_messages: list[dict[str, str | bytes]] = []

    async def send(self, topic: str, value: bytes) -> None:
        self.sent_messages.append(
            {
                "topic": topic,
                "value": value,
            }
        )


@pytest.mark.asyncio
async def test_failed_publish_keeps_event_unpublished() -> None:
    engine = create_async_engine(test_settings.database_url)
    session_factory = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    event_id: int | None = None

    try:
        async with session_factory() as db:
            event = OutboxEvent(
                event_type="test.failure",
                payload='{"message":"failure test"}',
            )
            db.add(event)
            await db.commit()
            await db.refresh(event)

            event_id = event.id

            publisher = OutboxPublisher(
                producer=FailingProducer(),
                topic="user-events",
            )

            with pytest.raises(
                RuntimeError,
                match="Kafka is unavailable",
            ):
                await publisher.publish_pending_events(
                    db=db,
                    batch_size=1,
                )

        async with session_factory() as db:
            result = await db.execute(
                select(OutboxEvent).where(
                    OutboxEvent.id == event_id,
                )
            )
            saved_event = result.scalar_one()

            assert saved_event.published_at is None

    finally:
        try:
            if event_id is not None:
                async with session_factory() as db:
                    event = await db.get(OutboxEvent, event_id)
                    if event is not None:
                        await db.delete(event)
                        await db.commit()
        finally:
            await engine.dispose()


@pytest.mark.asyncio
async def test_failed_event_can_be_published_on_next_attempt() -> None:
    engine = create_async_engine(test_settings.database_url)
    session_factory = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    event_id: int | None = None

    try:
        async with session_factory() as db:
            event = OutboxEvent(
                event_type="test.retry",
                payload='{"message":"retry test"}',
            )
            db.add(event)
            await db.commit()
            await db.refresh(event)

            event_id = event.id

            # تلاش اول: ارسال ناموفق
            failing_publisher = OutboxPublisher(
                producer=FailingProducer(),
                topic="user-events",
            )

            with pytest.raises(
                RuntimeError,
                match="Kafka is unavailable",
            ):
                await failing_publisher.publish_pending_events(
                    db=db,
                    batch_size=1,
                )

            result = await db.execute(
                select(OutboxEvent).where(
                    OutboxEvent.id == event_id,
                )
            )
            saved_event = result.scalar_one()

            assert saved_event.published_at is None

            # تلاش دوم: ارسال موفق
            successful_producer = SuccessfulProducer()
            successful_publisher = OutboxPublisher(
                producer=successful_producer,
                topic="user-events",
            )

            published_count = (
                await successful_publisher.publish_pending_events(
                    db=db,
                    batch_size=100,
                )
            )

            assert published_count >= 1

            assert any(
                message["topic"] == "user-events"
                and message["value"]
                == b'{"message":"retry test"}'
                for message in successful_producer.sent_messages
            )

            result = await db.execute(
                select(OutboxEvent).where(
                    OutboxEvent.id == event_id,
                )
            )
            saved_event = result.scalar_one()

            assert saved_event.published_at is not None

    finally:
        try:
            if event_id is not None:
                async with session_factory() as db:
                    event = await db.get(OutboxEvent, event_id)
                    if event is not None:
                        await db.delete(event)
                        await db.commit()
        finally:
            await engine.dispose()
