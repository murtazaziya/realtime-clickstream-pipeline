from pydantic import BaseModel, Field
from uuid import uuid4
from datetime import datetime, timezone
from typing import Optional

class ClickstreamEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    user_id: str
    session_id: str
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    page_url: str
    referrer_url: Optional[str] = None
    event_type: str           # click, scroll, page_view, add_to_cart, purchase
    device_type: str          # desktop, mobile, tablet
    browser: str
    country: str
    city: str
    product_id: Optional[str] = None
    product_category: Optional[str] = None
    time_spent_seconds: int