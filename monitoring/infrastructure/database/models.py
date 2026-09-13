from datetime import datetime
from uuid import UUID

from sqlalchemy import DateTime, Integer, Uuid
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Базовый класс ORM-моделей monitoring-сервиса."""


class EventPaymentActivity(Base):
    """Агрегированная активность оплат одного мероприятия внутри батча."""

    __tablename__ = "event_payment_activity"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    batch_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    event_id: Mapped[int] = mapped_column(Integer, nullable=False)
    payments_count: Mapped[int] = mapped_column(Integer, nullable=False)
    tickets_count: Mapped[int] = mapped_column(Integer, nullable=False)
    total_amount: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
