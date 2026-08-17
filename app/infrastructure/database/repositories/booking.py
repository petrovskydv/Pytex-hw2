from datetime import datetime

from sqlalchemy import func, select, update

from app.domain.dto import BookingDTO, SalesDashboardDTO
from app.domain.statuses import BookingStatus, SeatStatus
from app.infrastructure.database.models import Booking, EventSeat
from app.infrastructure.database.repositories.base import BaseRepository


class BookingRepository(BaseRepository):
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

    async def save_checkout_calculation(
        self,
        booking_id: int,
        payment_commission: int,
        protection_price: int | None,
    ) -> None:
        statement = (
            update(Booking)
            .where(Booking.id == booking_id)
            .values(
                payment_commission=payment_commission,
                protection_price=protection_price,
            )
        )
        await self._session.execute(statement)

    async def cancel(self, booking_id: int) -> None:
        statement = (
            update(Booking)
            .where(Booking.id == booking_id, Booking.status == BookingStatus.pending_payment)
            .values(status=BookingStatus.cancelled)
        )
        await self._session.execute(statement)

    async def get_sales_dashboard(self, event_id: int) -> SalesDashboardDTO:
        sold_tickets = (
            select(func.count(EventSeat.id))
            .where(EventSeat.event_id == event_id, EventSeat.status == SeatStatus.sold)
            .scalar_subquery()
        )
        statement = select(
            func.count(Booking.id).filter(Booking.status == BookingStatus.paid),
            func.coalesce(sold_tickets, 0),
            func.coalesce(func.sum(Booking.amount).filter(Booking.status == BookingStatus.paid), 0),
            func.coalesce(func.round(func.avg(Booking.amount).filter(Booking.status == BookingStatus.paid)), 0),
        ).where(Booking.event_id == event_id)
        paid_orders, sold_tickets, revenue, average_order = (await self._session.execute(statement)).one()
        return SalesDashboardDTO(
            paid_orders=paid_orders,
            sold_tickets=sold_tickets,
            revenue=revenue,
            average_order=average_order,
        )
