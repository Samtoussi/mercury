import json

from confluent_kafka import Consumer, KafkaException


CONSUMER_GROUP = "mercury-order-inspector-v3"

consumer = Consumer(
    {
        "bootstrap.servers": "localhost:9092",
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    }
)

consumer.subscribe(["mercury.public.orders"])

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

        before = payload.get("before")
        after = payload.get("after")
        operation = payload.get("op")

        print("=" * 50)
        print(f"Partition: {message.partition()}")
        print(f"Offset:    {message.offset()}")
        print(f"Operation: {operation}")
        print(f"Before:    {before}")
        print(f"After:     {after}")
        print()

except KeyboardInterrupt:
    print("\nStopping order inspector...")

finally:
    consumer.close()
    print("Order inspector stopped.")