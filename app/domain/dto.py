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
