import json

from confluent_kafka import Consumer, KafkaException

from consumer.state import (
    append_failed_event,
    append_order_history,
    init_state_db,
    update_order_state,
    update_pipeline_metric,
)


CONSUMER_GROUP = "mercury-order-consumer"
REQUIRED_ORDER_FIELDS = (
    "order_id",
    "status",
    "updated_at",
)


def validate_order(order):
    if not isinstance(order, dict):
        return False, "Order payload is not an object"

    missing_fields = [
        field
        for field in REQUIRED_ORDER_FIELDS
        if field not in order or order[field] is None
    ]

    if missing_fields:
        return (
            False,
            f"Missing required fields: {', '.join(missing_fields)}",
        )

    return True, None


def quarantine_event(message, raw_event, error_reason):
    append_failed_event(
        topic=message.topic(),
        partition=message.partition(),
        offset=message.offset(),
        error_reason=error_reason,
        event_value=raw_event,
    )

    print("=" * 50)
    print("INVALID EVENT")
    print(f"Partition: {message.partition()}")
    print(f"Offset:    {message.offset()}")
    print(f"Reason:    {error_reason}")
    print("Event quarantined.")
    print()

    consumer.commit(
        message=message,
        asynchronous=False,
    )


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

        try:
            raw_event = message.value().decode("utf-8")
        except UnicodeDecodeError as error:
            raw_event = repr(message.value())

            quarantine_event(
                message=message,
                raw_event=raw_event,
                error_reason=f"Invalid UTF-8: {error}",
            )

            continue

        try:
            event = json.loads(raw_event)
        except json.JSONDecodeError as error:
            quarantine_event(
                message=message,
                raw_event=raw_event,
                error_reason=f"Invalid JSON: {error.msg}",
            )

            continue

        payload = event.get("payload", {})
        after = payload.get("after")
        operation = payload.get("op")

        source = payload.get("source", {})
        source_timestamp_ms = source.get("ts_ms")

        if after is None:
            continue

        is_valid, error_reason = validate_order(after)

        if not is_valid:
            quarantine_event(
                message=message,
                raw_event=raw_event,
                error_reason=error_reason,
            )

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

        consumer.commit(
            message=message,
            asynchronous=False,
        )

except KeyboardInterrupt:
    print("\nStopping order consumer...")

finally:
    consumer.close()
    print("Order consumer stopped.")