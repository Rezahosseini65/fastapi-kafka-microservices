import asyncio

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.db.session import engine
from app.kafka.outbox_publisher import OutboxPublisher
from app.models.outbox_event import OutboxEvent


class FakeProducer:
    async def send(
        self,
        topic: str,
        value: bytes,
    ) -> None:
        raise RuntimeError("Kafka is unavailable")


async def main():
    session_factory = async_sessionmaker(
        bind=engine,
        expire_on_commit=False,
    )

    publisher = OutboxPublisher(
        producer=FakeProducer(),
        topic="user-events",
    )

    async with session_factory() as db:
        event = OutboxEvent(
            event_type="test.failure",
            payload='{"message":"failure test"}',
        )

        db.add(event)
        await db.commit()
        await db.refresh(event)

        event_id = event.id

        print(f"Created test event: {event_id}")

        try:
            await publisher.publish_pending_events(
                db=db,
                batch_size=1,
            )
        except RuntimeError as exc:
            print(f"Publisher failed: {exc}")

        await db.rollback()

        result = await db.execute(
            select(OutboxEvent).where(
                OutboxEvent.id == event_id,
            )
        )

        event = result.scalar_one()

        print(
            f"Event {event.id} published_at: "
            f"{event.published_at}"
        )

        await db.delete(event)
        await db.commit()

        print(f"Deleted test event: {event_id}")


if __name__ == "__main__":
    asyncio.run(main())