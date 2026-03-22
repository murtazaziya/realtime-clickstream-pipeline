from kafka import KafkaProducer
from config import EVENTHUB_CONNECTION_STRING, EVENTHUB_NAME
from event_schema import ClickstreamEvent
import json
import logging

logger = logging.getLogger(__name__)

def get_producer() -> KafkaProducer:
    # Azure Event Hubs uses SASL_SSL with PLAIN mechanism
    connection_string = EVENTHUB_CONNECTION_STRING
    
    # Extract the endpoint from the connection string
    endpoint = connection_string.split(";")[0].replace(
        "Endpoint=sb://", ""
    ).replace("/", "")

    return KafkaProducer(
        bootstrap_servers=f"{endpoint}:9093",
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username="$ConnectionString",
        sasl_plain_password=connection_string,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=3
    )

def publish_event(producer: KafkaProducer, event: ClickstreamEvent) -> None:
    try:
        future = producer.send(
            EVENTHUB_NAME,
            value=event.model_dump()
        )
        future.get(timeout=10)
        logger.info(f"Published event: {event.event_id}")
    except Exception as e:
        logger.error(f"Failed to publish event: {e}")
        raise