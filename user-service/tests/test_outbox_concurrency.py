import pytest

import asyncio
import json
from uuid import uuid4

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.kafka.outbox_publisher import OutboxPublisher
from app.models.outbox_event import OutboxEvent
from tests.config import test_settings


class BlockingProducer:
    """Producer آزمایشی که ارسال اول را تا اجازهٔ تست متوقف می‌کند."""

    def __init__(self) -> None:
        self.send_started = asyncio.Event()
        self.allow_send = asyncio.Event()
        self.sent_values: list[bytes] = []

    async def send(self, topic: str, value: bytes) -> None:
        self.send_started.set()
        await self.allow_send.wait()
        self.sent_values.append(value)


class ImmediateProducer:
    """Producer آزمایشی که رویداد را بلافاصله ارسال می‌کند."""

    def __init__(self) -> None:
        self.sent_values: list[bytes] = []

    async def send(self, topic: str, value: bytes) -> None:
        self.sent_values.append(value)


@pytest.mark.asyncio
async def test_two_publishers_skip_locked_events() -> None:
    engine = create_async_engine(test_settings.database_url)

    SessionLocal = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    event_id_1 = uuid4()
    event_id_2 = uuid4()
    event_ids = [event_id_1, event_id_2]

    producer_1 = BlockingProducer()
    producer_2 = ImmediateProducer()

    publisher_1 = OutboxPublisher(
        producer=producer_1,
        topic="user-events",
    )
    publisher_2 = OutboxPublisher(
        producer=producer_2,
        topic="user-events",
    )

    publisher_task: asyncio.Task[int] | None = None

    try:
        # ابتدا دو رویداد مستقل در دیتابیس ایجاد می‌کنیم.
        async with SessionLocal() as setup_session:
            event_1 = OutboxEvent(
                event_id=event_id_1,
                event_type="test.concurrent",
                payload=json.dumps({"event_id": str(event_id_1)}),
            )
            event_2 = OutboxEvent(
                event_id=event_id_2,
                event_type="test.concurrent",
                payload=json.dumps({"event_id": str(event_id_2)}),
            )

            setup_session.add_all([event_1, event_2])
            await setup_session.flush()

            # ترتیب انتخاب را برای تست قطعی می‌کنیم.
            event_1_id = event_1.id
            event_2_id = event_2.id
            assert event_1_id < event_2_id

            await setup_session.commit()

        async with SessionLocal() as session_1:
            async with SessionLocal() as session_2:
                # Publisher اول رویداد اول را قفل می‌کند و
                # هنگام ارسال، تراکنش را باز نگه می‌دارد.
                publisher_task = asyncio.create_task(
                    publisher_1.publish_pending_events(
                        db=session_1,
                        batch_size=1,
                    )
                )

                await asyncio.wait_for(
                    producer_1.send_started.wait(),
                    timeout=5,
                )

                # Publisher دوم باید رویداد قفل‌شده را رد کند
                # و رویداد دوم را انتخاب کند.
                second_count = await asyncio.wait_for(
                    publisher_2.publish_pending_events(
                        db=session_2,
                        batch_size=1,
                    ),
                    timeout=5,
                )

                assert second_count == 1
                assert len(producer_2.sent_values) == 1

                second_event = json.loads(
                    producer_2.sent_values[0].decode("utf-8")
                )
                assert second_event["event_id"] == str(event_id_2)

                # حالا اجازه می‌دهیم Publisher اول تمام شود.
                producer_1.allow_send.set()

                first_count = await asyncio.wait_for(
                    publisher_task,
                    timeout=5,
                )

                assert first_count == 1
                assert len(producer_1.sent_values) == 1

                first_event = json.loads(
                    producer_1.sent_values[0].decode("utf-8")
                )
                assert first_event["event_id"] == str(event_id_1)

                # اثبات می‌کنیم دو Publisher رویدادهای متفاوتی فرستادند.
                assert (
                    first_event["event_id"]
                    != second_event["event_id"]
                )

        # هر دو رویداد باید در دیتابیس منتشرشده ثبت شده باشند.
        async with SessionLocal() as verify_session:
            result = await verify_session.execute(
                select(OutboxEvent).where(
                    OutboxEvent.event_id.in_(event_ids)
                )
            )
            events = result.scalars().all()

            assert len(events) == 2
            assert all(event.published_at is not None for event in events)

    finally:
        # اگر تست زودتر از موعد شکست خورد، Publisher اول معطل نماند.
        producer_1.allow_send.set()

        if publisher_task is not None and not publisher_task.done():
            await asyncio.gather(publisher_task, return_exceptions=True)

        # فقط داده‌های ساخته‌شده توسط همین تست را حذف می‌کنیم.
        try:
            async with SessionLocal() as cleanup_session:
                await cleanup_session.execute(
                    delete(OutboxEvent).where(
                        OutboxEvent.event_id.in_(event_ids)
                    )
                )
                await cleanup_session.commit()
        finally:
            await engine.dispose()
