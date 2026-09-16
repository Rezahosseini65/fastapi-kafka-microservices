from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    status,
    Request,
    Path,
    Body
)
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.kafka.producer import KafkaProducer
from app.schemas.events import UserCreatedData, UserCreatedEvent
from app.db.session import get_db
from app.models.user import User
from app.schemas.user import (
    UserCreateSchema,
    UserResponseSchema,
    UserUpdateSchema,
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
    http_request: Request,
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

    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already exists",
        )
    
    await db.refresh(user_obj)

    event = UserCreatedEvent(
        data=UserCreatedData(
            id=user_obj.id,
            name=user_obj.name,
            email=user_obj.email
        )
    )

    kafka_producer: KafkaProducer = http_request.app.state.kafka_producer

    await kafka_producer.send(
        topic="user-events",
        value=event.to_bytes(),
    )
    
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


@router.patch("/{user_id}/", response_model=UserResponseSchema)
async def update_user(
    user_id: int = Path(...),
    request: UserUpdateSchema = Body(...),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(User).where(User.id==user_id)
    )

    user = result.scalar_one_or_none()

    if user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    update_data = request.model_dump(exclude_unset=True)

    if "name" in update_data:
        user.name = update_data["name"].strip()

    if "email" in update_data:
        normalized_email = update_data["email"].lower()

        result = await db.execute(
            select(User).where(
                User.email == normalized_email,
                User.id != user_id,
            )
        )

        existing_user = result.scalar_one_or_none()

        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Email already exists",
            )

        user.email = normalized_email

    try:
        await db.commit()
    except:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already exists",
        )
    
    await db.refresh(user)

    return user


@router.delete("/{user_id}/")
async def delete_user(
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
            detail="User not found"
        )

    await db.delete(user)
    await db.commit()


    return {
        "message": "user removed successfully"
    }