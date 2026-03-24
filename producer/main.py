from fastapi import FastAPI, HTTPException, BackgroundTasks
from contextlib import asynccontextmanager
from simulator import generate_event, generate_batch
from kafka_producer import get_producer, publish_event
from event_schema import ClickstreamEvent
import asyncio
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

producer = get_producer()
streaming_task = None

async def continuous_stream(events_per_second: int):
    """Runs forever until cancelled."""
    interval = 1.0 / events_per_second
    while True:
        try:
            event = generate_event()
            publish_event(producer, event)
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            logger.info("Stream cancelled.")
            break
        except Exception as e:
            logger.error(f"Stream error: {e}")
            await asyncio.sleep(1)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Auto-start streaming when FastAPI launches."""
    global streaming_task
    logger.info("Auto-starting clickstream event generator...")
    streaming_task = asyncio.create_task(continuous_stream(events_per_second=5))
    yield
    # Shutdown
    if streaming_task:
        streaming_task.cancel()
        await asyncio.gather(streaming_task, return_exceptions=True)
    logger.info("Stream stopped.")

app = FastAPI(
    title="Clickstream Event Producer",
    description="Simulates and streams website clickstream events to Kafka",
    version="1.0.0",
    lifespan=lifespan
)

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

@app.post("/stream/stop")
async def stop_stream():
    """Stop the auto stream."""
    global streaming_task
    if streaming_task:
        streaming_task.cancel()
        return {"status": "stream stopped"}
    return {"status": "no stream running"}

@app.post("/stream/start")
async def start_stream(events_per_second: int = 5):
    """Restart the auto stream."""
    global streaming_task
    if streaming_task:
        streaming_task.cancel()
    streaming_task = asyncio.create_task(
        continuous_stream(events_per_second=events_per_second)
    )
    return {"status": "streaming", "events_per_second": events_per_second}