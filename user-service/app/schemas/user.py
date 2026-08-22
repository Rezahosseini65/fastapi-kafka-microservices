from pydantic import BaseModel, Field, EmailStr, ConfigDict


class UserBaseSchema(BaseModel):
    name: str = Field(
        ...,
        max_length=64,
        description="name of the user(maximum 64 characters)"
    )
    email: EmailStr = Field(
        ...,
        description="valid email address"
    )


class UserCreateSchema(UserBaseSchema):
    pass


class UserResponseSchema(UserBaseSchema):
    id: int

    model_config = ConfigDict(from_attributes=True)



__all__ = [
    "UserBaseSchema",
    "UserCreateSchema",
    "UserResponseSchema"
]