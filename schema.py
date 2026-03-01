from pydantic import BaseModel
from typing import Optional, Any
from datetime import datetime

class UserIN(BaseModel):
    name: Optional[Any] = None
    value: Optional[Any] = None
    age: int | None = None

class RawOut(BaseModel):
    id: int
    status: str
    error_message: Optional[str] = None
    processed_at: Optional[datetime] = None
    payload_preview: Optional[str] = None  # 只回傳截斷內容（避免整包太大）

    class Config:
        from_attributes = True

class RawListItem(BaseModel):
    id: int
    status: str
    error_message: Optional[str] = None
    processed_at: Optional[datetime] = None

    class Config:
        from_attributes = True