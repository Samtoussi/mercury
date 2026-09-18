import json

from confluent_kafka import Consumer, KafkaException

from consumer.state import (
    append_order_history,
    init_state_db,
    update_order_state,
    update_pipeline_metric,
)


CONSUMER_GROUP = "mercury-order-consumer"

consumer = Consumer(
    {
        "bootstrap.servers": "localhost:9092",
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    }
)

consumer.subscribe(["mercury.public.orders"])

init_state_db()

print("Listening for Mercury order events...")
print(f"Consumer group: {CONSUMER_GROUP}\n")

try:
    while True:
        message = consumer.poll(1.0)

        if message is None:
            continue

        if message.error():
            raise KafkaException(message.error())

        event = json.loads(message.value().decode("utf-8"))
        payload = event.get("payload", {})

        after = payload.get("after")
        operation = payload.get("op")

        source = payload.get("source", {})
        source_timestamp_ms = source.get("ts_ms")

        if after is None:
            continue

        update_order_state(after)

        append_order_history(
            topic=message.topic(),
            partition=message.partition(),
            offset=message.offset(),
            order=after,
        )

        if source_timestamp_ms is not None:
            update_pipeline_metric(
                "latest_source_timestamp_ms",
                str(source_timestamp_ms),
            )

        print("=" * 50)
        print(f"Partition: {message.partition()}")
        print(f"Offset:    {message.offset()}")
        print(f"Operation: {operation}")
        print(f"Order ID:  {after['order_id']}")
        print(f"Status:    {after['status']}")
        print("Current state updated.")
        print("History appended.")
        print()

        consumer.commit(message=message, asynchronous=False)

except KeyboardInterrupt:
    print("\nStopping order consumer...")

finally:
    consumer.close()
    print("Order consumer stopped.")