import enum


class SeatStatus(str, enum.Enum):
    available = "available"
    reserved = "reserved"
    sold = "sold"


class BookingStatus(str, enum.Enum):
    pending_payment = "pending_payment"
    paid = "paid"
    cancelled = "cancelled"
    expired = "expired"
