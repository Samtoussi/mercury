import base64
import json
from decimal import Decimal, InvalidOperation

from confluent_kafka import Consumer, KafkaException

from analytics.db import (
    upsert_inventory,
    upsert_order,
    upsert_payment,
)

CONSUMER_GROUP = "mercury-analytics-consumer"

ORDERS_TOPIC = "mercury.public.orders"
PAYMENTS_TOPIC = "mercury.public.payments"
INVENTORY_TOPIC = "mercury.public.inventory"

TOPICS = [
    ORDERS_TOPIC,
    PAYMENTS_TOPIC,
    INVENTORY_TOPIC,
]

REQUIRED_FIELDS = {
    ORDERS_TOPIC: (
        "order_id",
        "customer_id",
        "status",
        "total_amount",
        "created_at",
        "updated_at",
    ),
    PAYMENTS_TOPIC: (
        "payment_id",
        "order_id",
        "amount",
        "status",
        "created_at",
    ),
    INVENTORY_TOPIC: (
        "product_id",
        "stock_quantity",
        "updated_at",
    ),
}


class InvalidEventError(Exception):
    pass


def find_field_schema(event, field_name):
    schema = event.get("schema", {})

    for field in schema.get("fields", []):
        if field.get("field") != "after":
            continue

        for nested_field in field.get("fields", []):
            if nested_field.get("field") == field_name:
                return nested_field

    return None


def decode_decimal(event, field_name, value):
    field_schema = find_field_schema(event, field_name)

    if field_schema is None:
        raise InvalidEventError(
            f"Schema not found for Decimal field: {field_name}"
        )

    try:
        if field_schema.get("type") == "string":
            return Decimal(value)

        if (
            field_schema.get("type") == "bytes"
            and field_schema.get("name")
            == "org.apache.kafka.connect.data.Decimal"
        ):
            parameters = field_schema.get("parameters", {})
            scale = int(parameters["scale"])

            raw_bytes = base64.b64decode(
                value,
                validate=True,
            )

            unscaled_value = int.from_bytes(
                raw_bytes,
                byteorder="big",
                signed=True,
            )

            return Decimal(unscaled_value).scaleb(-scale)

    except (
        InvalidOperation,
        ValueError,
        TypeError,
        KeyError,
    ) as error:
        raise InvalidEventError(
            f"Invalid Decimal field {field_name}: {error}"
        ) from error

    raise InvalidEventError(
        f"Unsupported Decimal representation for field: {field_name}"
    )


def validate_required_fields(topic, record):
    required_fields = REQUIRED_FIELDS.get(topic)

    if required_fields is None:
        raise InvalidEventError(
            f"No validation contract for topic: {topic}"
        )

    missing_fields = [
        field
        for field in required_fields
        if field not in record or record[field] is None
    ]

    if missing_fields:
        raise InvalidEventError(
            f"Missing required fields: {', '.join(missing_fields)}"
        )


def normalize_order(event, order):
    validate_required_fields(
        ORDERS_TOPIC,
        order,
    )

    normalized = dict(order)

    normalized["total_amount"] = str(
        decode_decimal(
            event=event,
            field_name="total_amount",
            value=order["total_amount"],
        )
    )

    return normalized


def normalize_payment(event, payment):
    validate_required_fields(
        PAYMENTS_TOPIC,
        payment,
    )

    normalized = dict(payment)

    normalized["amount"] = str(
        decode_decimal(
            event=event,
            field_name="amount",
            value=payment["amount"],
        )
    )

    return normalized


def normalize_inventory(inventory):
    validate_required_fields(
        INVENTORY_TOPIC,
        inventory,
    )

    return dict(inventory)


def process_event(message, event):
    payload = event.get("payload")

    if not isinstance(payload, dict):
        raise InvalidEventError(
            "Missing or invalid Debezium payload"
        )

    after = payload.get("after")

    if after is None:
        return

    if not isinstance(after, dict):
        raise InvalidEventError(
            "Debezium after payload is not an object"
        )

    topic = message.topic()

    if topic == ORDERS_TOPIC:
        order = normalize_order(
            event,
            after,
        )

        upsert_order(order)

        print(
            f"ORDER     "
            f"id={order['order_id']} "
            f"status={order['status']} "
            f"amount={order['total_amount']}"
        )

    elif topic == PAYMENTS_TOPIC:
        payment = normalize_payment(
            event,
            after,
        )

        upsert_payment(payment)

        print(
            f"PAYMENT   "
            f"id={payment['payment_id']} "
            f"status={payment['status']} "
            f"amount={payment['amount']}"
        )

    elif topic == INVENTORY_TOPIC:
        inventory = normalize_inventory(after)

        upsert_inventory(inventory)

        print(
            f"INVENTORY "
            f"product={inventory['product_id']} "
            f"stock={inventory['stock_quantity']}"
        )

    else:
        raise InvalidEventError(
            f"Unsupported topic: {topic}"
        )


def print_invalid_event(message, reason):
    print("=" * 60)
    print("INVALID ANALYTICS EVENT")
    print(f"Topic:     {message.topic()}")
    print(f"Partition: {message.partition()}")
    print(f"Offset:    {message.offset()}")
    print(f"Reason:    {reason}")
    print("Event skipped.")
    print("=" * 60)
    print()


consumer = Consumer(
    {
        "bootstrap.servers": "localhost:9092",
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
)

consumer.subscribe(TOPICS)

print("Mercury analytics consumer started.")
print(f"Consumer group: {CONSUMER_GROUP}")
print("Topics:")

for topic in TOPICS:
    print(f"  - {topic}")

print()

try:
    while True:
        message = consumer.poll(1.0)

        if message is None:
            continue

        if message.error():
            raise KafkaException(message.error())

        try:
            raw_event = message.value().decode("utf-8")
            event = json.loads(raw_event)

            process_event(
                message=message,
                event=event,
            )

        except UnicodeDecodeError as error:
            print_invalid_event(
                message,
                f"Invalid UTF-8: {error}",
            )

        except json.JSONDecodeError as error:
            print_invalid_event(
                message,
                f"Invalid JSON: {error.msg}",
            )

        except InvalidEventError as error:
            print_invalid_event(
                message,
                str(error),
            )

        consumer.commit(
            message=message,
            asynchronous=False,
        )

except KeyboardInterrupt:
    print("\nStopping analytics consumer...")

finally:
    consumer.close()
    print("Analytics consumer stopped.")