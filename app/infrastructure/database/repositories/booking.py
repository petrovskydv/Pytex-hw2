from datetime import datetime

from sqlalchemy import delete, func, select, update

from app.domain.dto import BookingDTO, PendingProtectionDTO, SalesDashboardDTO
from app.domain.statuses import BookingStatus, SeatStatus
from app.infrastructure.database.models import Booking, Event, EventSeat
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

    async def get_expired_pending_for_update(self, now: datetime) -> list[int]:
        """Блокирует и возвращает просроченные неоплаченные брони."""
        statement = (
            select(Booking.id)
            .where(
                Booking.status == BookingStatus.pending_payment,
                Booking.reserved_until < now,
            )
            .with_for_update(skip_locked=True)
        )
        return list((await self._session.scalars(statement)).all())

    async def delete_expired_pending(self, booking_ids: list[int], now: datetime) -> int:
        """Удаляет только всё ещё просроченные pending-брони из заблокированного набора."""
        if not booking_ids:
            return 0
        statement = delete(Booking).where(
            Booking.id.in_(booking_ids),
            Booking.status == BookingStatus.pending_payment,
            Booking.reserved_until < now,
        )
        result = await self._session.execute(statement)
        return result.rowcount

    async def get_pending_protection(self, booking_id: int) -> PendingProtectionDTO | None:
        """Возвращает данные брони, если для неё ещё допустим расчёт защиты."""
        statement = (
            select(
                Booking.id.label("booking_id"),
                Booking.amount,
                Event.category.label("event_category"),
                Event.starts_at.label("event_starts_at"),
            )
            .join(Event, Event.id == Booking.event_id)
            .where(
                Booking.id == booking_id,
                Booking.status == BookingStatus.pending_payment,
                Booking.protection_price.is_(None),
            )
        )
        row = (await self._session.execute(statement)).mappings().one_or_none()
        return PendingProtectionDTO.model_validate(row) if row else None

    async def save_protection_if_pending(self, booking_id: int, protection_price: int) -> bool:
        """Идемпотентно сохраняет защиту только для актуальной pending-брони."""
        statement = (
            update(Booking)
            .where(
                Booking.id == booking_id,
                Booking.status == BookingStatus.pending_payment,
                Booking.protection_price.is_(None),
            )
            .values(protection_price=protection_price)
        )
        result = await self._session.execute(statement)
        return result.rowcount == 1

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
