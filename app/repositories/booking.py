from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.dto import BookingDTO
from app.infrastructure.database.models import Booking


class BookingRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(self, event_id: int, user_id: int, amount: int, reserved_until: datetime) -> BookingDTO:
        booking = Booking(
            event_id=event_id,
            user_id=user_id,
            amount=amount,
            payment_commission=0,
            protection_price=None,
            with_protection=False,
            reserved_until=reserved_until,
        )
        self._session.add(booking)
        await self._session.flush()
        return BookingDTO.model_validate(booking)
