from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.kafka.producer import KafkaProducer
from app.models.outbox_event import OutboxEvent


class OutboxPublisher:
    def __init__(
        self,
        producer: KafkaProducer,
        topic: str,
    ):
        self.producer = producer
        self.topic = topic

    async def publish_pending_events(
        self,
        db: AsyncSession,
        batch_size: int = 100,
    ) -> int:
        try:
            result = await db.execute(
                select(OutboxEvent)
                .where(
                    OutboxEvent.published_at.is_(None),
                )
                .order_by(OutboxEvent.id)
                .limit(batch_size)
                .with_for_update(skip_locked=True)
            )

            events = result.scalars().all()

            published_count = 0

            for event in events:
                await self.producer.send(
                    topic=self.topic,
                    value=event.payload.encode("utf-8"),
                )

                event.published_at = datetime.now(timezone.utc)
                published_count += 1

            if published_count:
                await db.commit()

            return published_count

        except Exception:
            await db.rollback()
            raise
        