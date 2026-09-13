import pytest_asyncio
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from httpx import ASGITransport, AsyncClient

from app.models.user import User
from tests.config import test_settings
from app.main import create_app
from app.db.session import get_db


@pytest_asyncio.fixture
async def db_session():
    engine = create_async_engine(
        test_settings.database_url,
    )

    TestSessionLocal = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    async with TestSessionLocal() as session:
        try:
            yield session
        finally:
            await session.execute(delete(User))
            await session.commit()

    await engine.dispose()


@pytest_asyncio.fixture
async def client(db_session):
    app = create_app()

    async def override_get_db():
        yield db_session

    app.dependency_overrides[get_db] = override_get_db

    transport = ASGITransport(app=app)

    async with AsyncClient(
        transport=transport,
        base_url="http://test",
    ) as client:
        yield client