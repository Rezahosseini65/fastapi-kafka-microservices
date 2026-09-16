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
