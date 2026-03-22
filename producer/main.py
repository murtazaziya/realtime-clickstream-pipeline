from fastapi import FastAPI, HTTPException, BackgroundTasks
from simulator import generate_event, generate_batch
from kafka_producer import get_producer, publish_event
from event_schema import ClickstreamEvent
import asyncio
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Clickstream Event Producer",
    description="Simulates and streams website clickstream events to Kafka",
    version="1.0.0"
)

producer = get_producer()

@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "clickstream-producer"}

@app.post("/event/single", response_model=ClickstreamEvent)
def send_single_event():
    """Generate and publish a single clickstream event."""
    try:
        event = generate_event()
        publish_event(producer, event)
        return event
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/event/batch")
def send_batch_events(batch_size: int = 10):
    """Generate and publish a batch of clickstream events."""
    if batch_size > 1000:
        raise HTTPException(
            status_code=400,
            detail="Batch size cannot exceed 1000"
        )
    try:
        events = generate_batch(batch_size)
        for event in events:
            publish_event(producer, event)
        return {
            "status": "success",
            "events_published": len(events)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def continuous_stream(events_per_second: int, duration_seconds: int):
    """Background task to stream events continuously."""
    total = events_per_second * duration_seconds
    published = 0
    interval = 1.0 / events_per_second

    while published < total:
        event = generate_event()
        publish_event(producer, event)
        published += 1
        await asyncio.sleep(interval)

    logger.info(f"Stream complete. Published {published} events.")

@app.post("/event/stream")
def start_stream(
    background_tasks: BackgroundTasks,
    events_per_second: int = 5,
    duration_seconds: int = 60
):
    """Start a continuous background stream of events."""
    background_tasks.add_task(
        continuous_stream,
        events_per_second,
        duration_seconds
    )
    return {
        "status": "streaming",
        "events_per_second": events_per_second,
        "duration_seconds": duration_seconds,
        "total_events": events_per_second * duration_seconds
    }