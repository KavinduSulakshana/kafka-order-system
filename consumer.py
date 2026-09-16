from confluent_kafka.avro import AvroConsumer, AvroProducer, load
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka import KafkaError


# --- DLQ producer ---
dlq_producer = AvroProducer(
    {
        "bootstrap.servers": "localhost:9092",
        "schema.registry.url": "http://localhost:8082",
    },
    default_value_schema=load("order.avsc"),
)


# --- Consumer ---
consumer = AvroConsumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "order-consumer-group",
    "schema.registry.url": "http://localhost:8082",
    "auto.offset.reset": "earliest",
})

consumer.subscribe(["orders"])


# --- Real-time aggregation state ---
total_price = 0.0
count = 0

MAX_RETRIES = 3


def process_order(order):
    """Simulate processing. Randomly fail to demonstrate retry/DLQ."""
    import random

    r = random.random()

    if r < 0.1:
        # 10% permanent failure
        raise ValueError("Permanent failure: bad data")

    if r < 0.3:
        # 20% temporary failure
        raise ConnectionError("Temporary failure: retry me")

    return True


print("Consumer started...")

try:
    while True:

        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print("Consumer error:", msg.error())
            continue

        order = msg.value()


        # --- Retry logic ---
        success = False

        for attempt in range(1, MAX_RETRIES + 1):

            try:
                process_order(order)

                success = True
                break

            except ConnectionError as e:
                print(
                    f"[Retry {attempt}/{MAX_RETRIES}] "
                    f"{order['orderId']}: {e}"
                )

            except Exception as e:
                print(
                    f"[Permanent fail] "
                    f"{order['orderId']}: {e}"
                )
                break


        # --- Successful processing ---
        if success:

            # Real-time running average
            total_price += order["price"]
            count += 1

            avg = total_price / count

            print(
                f"Processed {order['orderId']} | "
                f"price={order['price']} | "
                f"RUNNING AVG={avg:.2f}"
            )


        # --- Failed processing -> DLQ ---
        else:

            dlq_producer.produce(
                topic="orders-dlq",
                value=order
            )

            dlq_producer.flush()

            print(
                f"--> Sent {order['orderId']} to DLQ"
            )


except KeyboardInterrupt:
    print("\nConsumer stopped.")

finally:
    consumer.close()