import asyncio

from app.kafka.producer import KafkaProducer
from app.schemas.events import UserCreatedData, UserCreatedEvent


async def main():
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
    )

    event = UserCreatedEvent(
        data=UserCreatedData(
            id=1,
            name="Reza",
            email="reza@example.com",
        )
    )

    await producer.start()

    try:
        await producer.send(
            topic="user-events",
            value=event.to_bytes(),
        )
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
    