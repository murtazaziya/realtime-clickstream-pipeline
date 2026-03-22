from faker import Faker
from uuid import uuid4
from event_schema import ClickstreamEvent
import random

fake = Faker()

PAGES = [
    "/home", "/products", "/products/shoes", "/products/shirts",
    "/products/pants", "/products/jackets", "/products/accessories",
    "/cart", "/checkout", "/order-confirmation", "/about", "/contact"
]

EVENT_TYPES = ["page_view", "click", "scroll", "add_to_cart", "purchase"]
DEVICE_TYPES = ["desktop", "mobile", "tablet"]
BROWSERS = ["Chrome", "Firefox", "Safari", "Edge"]
CATEGORIES = ["shoes", "shirts", "accessories", "pants", "jackets"]

def generate_event(user_id: str = None, session_id: str = None) -> ClickstreamEvent:
    page = random.choice(PAGES)
    is_product_page = "products" in page

    return ClickstreamEvent(
        user_id=user_id or str(uuid4()),
        session_id=session_id or str(uuid4()),
        page_url=page,
        referrer_url=random.choice(PAGES + [None]),
        event_type=random.choice(EVENT_TYPES),
        device_type=random.choice(DEVICE_TYPES),
        browser=random.choice(BROWSERS),
        country=fake.country(),
        city=fake.city(),
        product_id=str(uuid4()) if is_product_page else None,
        product_category=random.choice(CATEGORIES) if is_product_page else None,
        time_spent_seconds=random.randint(3, 300)
    )

def generate_batch(batch_size: int = 10) -> list[ClickstreamEvent]:
    return [generate_event() for _ in range(batch_size)]