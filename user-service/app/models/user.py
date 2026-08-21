"""The ``users`` table."""

from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class User(Base):
    """
    A customer and the balance they can spend on orders.
    """

    __tablename__ = "users"
    id: Mapped[int] = mapped_column(
        primary_key=True,
        autoincrement=True
    )
    name: Mapped[str] = mapped_column(
        String(255), 
        nullable=False, 
        index=True
    )
    email: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        unique=True,
        index=True,
    )

    def __repr__(self)-> str:
        return f"User(id={self.id!r}, name={self.name!r})"


__all__ = ["User"]
