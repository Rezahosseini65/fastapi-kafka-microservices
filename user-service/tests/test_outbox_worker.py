import asyncio

import pytest

from app.kafka.outbox_worker import OutboxWorker


class FakePublisher:
    def __init__(self, results=None, errors=None):
        self.calls = 0
        self.results = list(results or [])
        self.errors = list(errors or [])

    async def publish_pending_events(self, db, batch_size=100):
        self.calls += 1

        if self.errors:
            error = self.errors.pop(0)
            raise error

        if self.results:
            return self.results.pop(0)

        return 0


class FakeSession:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeSessionFactory:
    def __call__(self):
        return FakeSession()


@pytest.mark.asyncio
async def test_worker_starts_and_stops():
    publisher = FakePublisher()
    worker = OutboxWorker(
        publisher=publisher,
        session_factory=FakeSessionFactory(),
        poll_interval=0.01,
    )

    await worker.start()

    # به Worker فرصت می‌دهیم حداقل یک دور اجرا شود.
    await asyncio.sleep(0.03)

    await worker.stop()

    assert publisher.calls >= 1
    assert worker._task is None


@pytest.mark.asyncio
async def test_worker_continues_after_publish_error(caplog):
    publisher = FakePublisher(
        errors=[RuntimeError("Kafka unavailable")],
        results=[0],
    )
    worker = OutboxWorker(
        publisher=publisher,
        session_factory=FakeSessionFactory(),
        poll_interval=0.01,
    )

    await worker.start()

    # منتظر می‌مانیم تا خطای اول رخ دهد و دور بعد اجرا شود.
    for _ in range(50):
        if publisher.calls >= 2:
            break
        await asyncio.sleep(0.01)

    await worker.stop()

    assert publisher.calls >= 2
    assert "Outbox worker failed while publishing events" in caplog.text


@pytest.mark.asyncio
async def test_worker_stop_is_safe_when_not_started():
    worker = OutboxWorker(
        publisher=FakePublisher(),
        session_factory=FakeSessionFactory(),
        poll_interval=0.01,
    )

    await worker.stop()

    assert worker._task is None