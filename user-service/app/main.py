from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routers.user import router as user_router
from app.kafka.producer import KafkaProducer


kafka_producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await kafka_producer.start()

    app.state.kafka_producer = kafka_producer

    yield

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