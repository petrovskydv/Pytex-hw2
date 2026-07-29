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


class CheckoutEventDTO(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    title: str
    category: str
    starts_at: datetime


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
    event: CheckoutEventDTO
    seats: list[EventSeatDTO]
    payment: PaymentCalculationDTO
    protection: ProtectionCalculationDTO | None
