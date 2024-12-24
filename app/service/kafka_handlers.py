from typing import List, Dict, Any
from app.service.kafka_settings.producer import produce


def publish_batch(topic: str, batch: List[Dict[str, Any]], id_field: str):
    try:
        produce(
            topic=topic,
            key=id_field,
            value=batch
        )
        print(f"Published batch of {len(batch)} records to {topic}")
    except Exception as e:
        print(f"Error publishing batch to {topic}: {str(e)}")