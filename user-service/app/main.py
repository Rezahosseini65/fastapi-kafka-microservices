from fastapi import FastAPI

from app.api.routers.user import router as user_router


def create_app() -> FastAPI:
    app = FastAPI(
        title="User Service",
        version="0.1.0",
    )

    app.include_router(user_router)

    return app


app = create_app()