import asyncio

from app.kafka.outbox_worker import OutboxWorker


class FakeSession:
    async def __aenter__(self):
        return self

    async def __aexit__(
        self,
        exc_type,
        exc_value,
        traceback,
    ):
        return False


class FakeSessionFactory:
    def __call__(self):
        return FakeSession()


class FakePublisher:
    async def publish_pending_events(
        self,
        db,
        batch_size: int,
    ) -> int:
        print(
            f"Fake publisher called with batch_size={batch_size}"
        )

        return 0


async def main():
    worker = OutboxWorker(
        publisher=FakePublisher(),
        session_factory=FakeSessionFactory(),
        poll_interval=1.0,
        batch_size=10,
    )

    print("Starting worker...")

    await worker.start()

    print("Worker started.")

    await asyncio.sleep(2.5)

    print("Stopping worker...")

    await worker.stop()

    print("Worker stopped.")


if __name__ == "__main__":
    asyncio.run(main())