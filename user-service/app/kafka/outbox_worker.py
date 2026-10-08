import asyncio
import logging

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.kafka.outbox_publisher import OutboxPublisher

logger = logging.getLogger(__name__)


class OutboxWorker:
    def __init__(
        self,
        publisher:OutboxPublisher,
        session_factory:async_sessionmaker[AsyncSession],
        poll_interval:float = 5.0,
        batch_size:int= 100
    ):
        self.publisher = publisher
        self.session_factory = session_factory
        self.poll_interval = poll_interval
        self.batch_size = batch_size

        self._task:asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()

    async def start(self)-> None:
        if self._task is not None:
            return

        self._stop_event.clear()
        self._task = asyncio.create_task(
            self._run()
        )

    async def stop(self)-> None:
        if self._task is None:
            return

        self._stop_event.set()

        await self._task

        self._task = None

    async def _run(self)-> None:
        while not self._stop_event.is_set():
            try:
                async with self.session_factory() as db:
                    published_count = (
                        await self.publisher.publish_pending_events(
                            db=db,
                            batch_size=self.batch_size
                        )
                    )

                    if published_count:
                        logger.info(
                            "Published %s outbox events",
                            published_count,
                        )

            except Exception:
                logger.exception(
                    "Outbox worker failed while publishing events",
                )

            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self.poll_interval,
                )
            except asyncio.TimeoutError:
                pass                    
    