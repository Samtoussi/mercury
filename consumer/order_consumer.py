import json

from confluent_kafka import Consumer, KafkaException

from consumer.state import (
    append_order_history,
    init_state_db,
    update_order_state,
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

        if after is None:
            continue

        update_order_state(after)

        append_order_history(
            topic=message.topic(),
            partition=message.partition(),
            offset=message.offset(),
            order=after,
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