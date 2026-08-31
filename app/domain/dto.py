from datetime import datetime

from pydantic import BaseModel, ConfigDict


class BookingDTO(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    event_id: int
    user_id: int
    amount: int
    reserved_until: datetime


class EventSeatDTO(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    event_id: int
    seat_id: int
    price: int


class EventCheckoutDTO(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    title: str
    category: str
    starts_at: datetime


class EventDetailsDTO(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    organizer_id: int
    location_id: int
    title: str
    description: str | None
    category: str
    starts_at: datetime
    base_price: int


class PaymentCalculationDTO(BaseModel):
    commission: int
    total: int
    payment_methods: list[str]
    expires_at: datetime | None = None


class ProtectionCalculationDTO(BaseModel):
    available: bool
    price: int
    covered_amount: int
    description: str | None = None


class CheckoutDTO(BaseModel):
    booking: BookingDTO
    event: EventCheckoutDTO
    seats: list[EventSeatDTO]
    payment: PaymentCalculationDTO
    protection: ProtectionCalculationDTO | None


class DashboardEventDTO(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    title: str
    starts_at: datetime


class SalesDashboardDTO(BaseModel):
    paid_orders: int
    sold_tickets: int
    revenue: int
    average_order: int


class OccupancyDashboardDTO(BaseModel):
    total: int
    available: int
    reserved: int
    sold: int


class EventDashboardDTO(BaseModel):
    event: DashboardEventDTO
    sales: SalesDashboardDTO
    occupancy: OccupancyDashboardDTO
