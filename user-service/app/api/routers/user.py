from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    status,
    Path
)
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.models.user import User
from app.schemas.user import (
    UserCreateSchema,
    UserResponseSchema,
)

router = APIRouter(
    tags=["users"],
    prefix="/api/users",
)


@router.post(
    "/create/",
    response_model=UserResponseSchema,
    status_code=status.HTTP_201_CREATED,
)
async def create_user(
    request: UserCreateSchema,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(User).where(
            User.email == request.email.lower()
        )
    )

    existing_user = result.scalar_one_or_none()

    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already exists",
        )

    user_obj = User(
        name=request.name.strip(),
        email=request.email.lower(),
    )

    db.add(user_obj)

    await db.commit()
    await db.refresh(user_obj)

    return user_obj


@router.get(
    "/", 
    response_model=list[UserResponseSchema]
)
async def get_users(
    db: AsyncSession = Depends(get_db)
):
    result = await db.execute(
        select(User).order_by(User.id)
    )

    users = result.scalars().all()

    return users


@router.get(
        "/{user_id}/",
        response_model=UserResponseSchema
)
async def get_user(
    user_id: int = Path(...),
    db: AsyncSession = Depends(get_db)
):
    result = await db.execute(
        select(User).where(User.id==user_id)
    )

    user = result.scalar_one_or_none()

    if user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )

    return user