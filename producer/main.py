import pyarrow.dataset as ds
from confluent_kafka import Producer
import json
import time
from datetime import datetime 
producer = Producer({
    "bootstrap.servers": "kafka-1:9092",
    "client.id": "car-telematics-producer",
})

TOPIC = "car_telematics"

dataset = ds.dataset(
    "./data",
    format="parquet"
)
print(f"STARTING SEND DATA TO KAFKA, TOPIC: {TOPIC}")

def log(level, message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(
        f"{timestamp} | {level:<5} | PRODUCER | {message}",
        flush=True
    )
    
batch_number = 0
total_messages = 0

while True:

    for batch in dataset.to_batches(batch_size=100):

        batch_number += 1
        batch_messages = 0

        for row in batch.to_pylist():
            producer.produce(
                TOPIC,
                value=json.dumps(
                    row,
                    ensure_ascii=False,
                    default=str
                ).encode("utf-8")
            )

            producer.poll(0)

            batch_messages += 1
            total_messages += 1

        log(
            "INFO",
            f"Batch #{batch_number:04d} | "
            f"Sent: {batch_messages:03d} messages | "
            f"Total: {total_messages:,}"
        )

        time.sleep(3)

    log(
        "INFO",
        "Dataset completed | Restarting from beginning..."
    )

        