import asyncio

from sqlalchemy.ext.asyncio import async_sessionmaker

from app.db.session import engine
from app.kafka.outbox_publisher import OutboxPublisher
from app.kafka.producer import KafkaProducer


async def main():
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
    )

    session_factory = async_sessionmaker(
        bind=engine,
        expire_on_commit=False,
    )

    publisher = OutboxPublisher(
        producer=producer,
        topic="user-events",
    )

    await producer.start()

    try:
        async with session_factory() as db:
            published_count = await publisher.publish_pending_events(
                db=db,
            )

            print(
                f"Published events: {published_count}",
            )
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())