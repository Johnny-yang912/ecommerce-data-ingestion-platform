from pydantic import BaseModel
from typing import Optional, Any
from datetime import datetime

class UserIN(BaseModel):
    name: Optional[Any] = None
    value: Optional[Any] = None


class RawOut(BaseModel):
    id: int
    payload_text: str
    received_at: datetime
    status: str
    error_message: Optional[str] = None
    processed_at: Optional[datetime] = None
