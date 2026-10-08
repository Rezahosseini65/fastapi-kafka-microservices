from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.api.routers.user import router as user_router
from app.db.session import engine
from app.kafka.outbox_publisher import OutboxPublisher
from app.kafka.outbox_worker import OutboxWorker
from app.kafka.producer import KafkaProducer


kafka_producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
)

session_factory = async_sessionmaker(
    bind=engine,
    expire_on_commit=False
)

outbox_publisher = OutboxPublisher(
    producer=kafka_producer,
    topic="user-events"
)

outbox_worker = OutboxWorker(
    publisher=outbox_publisher,
    session_factory=session_factory,
    poll_interval=5.0,
    batch_size=100
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await kafka_producer.start()
    await outbox_worker.start()

    app.state.kafka_producer = kafka_producer
    app.state.outbox_worker = outbox_worker

    yield

    await outbox_worker.stop()
    await kafka_producer.stop()


def create_app() -> FastAPI:
    app = FastAPI(
        title="User Service",
        version="0.1.0",
        lifespan=lifespan,
    )

    app.include_router(user_router)

    return app


app = create_app()