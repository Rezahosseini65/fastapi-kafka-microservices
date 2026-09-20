from datetime import datetime, timezone
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class UserCreatedData(BaseModel):
    id: int
    name: str
    email: str


class UserCreatedEvent(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    event_type: str = "user.created"
    occurred_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    data: UserCreatedData

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode("utf-8")


class UserUpdatedData(BaseModel):
    id:int
    name:str
    email:str


class UserUpdatedEvent(BaseModel):
    event_id: UUID= Field(default_factory=uuid4)
    event_type: str= "user.updated"
    occurred_at: datetime= Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    data:UserUpdatedData

    def to_bytes(self)-> bytes:
        return self.model_dump_json().encode("utf-8")


class UserDeletedData(BaseModel):
    id: int


class UserDeletedEvent(BaseModel):
    event_id: UUID= Field(default_factory=uuid4)
    event_type: str= "user.deleted"
    occurred_at: datetime = Field(
            default_factory=lambda: datetime.now(timezone.utc)
        )
    data: UserDeletedData

    def to_bytes(self)-> bytes:
        return self.model_dump_json().encode("utf-8")