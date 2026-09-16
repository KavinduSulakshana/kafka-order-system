import random
import time
from confluent_kafka.avro import AvroProducer, load

value_schema = load("order.avsc")

producer = AvroProducer(
    {
        "bootstrap.servers": "localhost:9092",
        "schema.registry.url": "http://localhost:8082",
    },
    default_value_schema=value_schema,
)

products = ["Item1", "Item2", "Item3", "Item4"]

for i in range(1, 21):
    order = {
        "orderId": str(1000 + i),
        "product": random.choice(products),
        "price": round(random.uniform(10.0, 500.0), 2),
    }

    producer.produce(
        topic="orders",
        value=order
    )

    producer.flush()

    print(f"Produced: {order}")
    time.sleep(1)

print("Done producing orders.")