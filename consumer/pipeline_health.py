import json
import sqlite3
import time
from pathlib import Path
from urllib.request import urlopen

from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient


CONNECTOR_STATUS_URL = (
    "http://localhost:8083/connectors/"
    "mercury-postgres-source/status"
)

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
CONSUMER_GROUP = "mercury-order-consumer"
DB_PATH = Path("consumer_state.db")


def get_connector_health():
    with urlopen(CONNECTOR_STATUS_URL, timeout=5) as response:
        status = json.load(response)

    connector_state = status["connector"]["state"]
    task_states = [task["state"] for task in status["tasks"]]

    return connector_state, task_states


def get_consumer_offsets():
    admin = AdminClient(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        }
    )

    request = ConsumerGroupTopicPartitions(
        CONSUMER_GROUP
    )

    result = admin.list_consumer_group_offsets(
        [request]
    )

    group_result = result[CONSUMER_GROUP].result()

    return group_result.topic_partitions


def get_consumer_lag(offsets):
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "mercury-health-check",
            "enable.auto.commit": False,
        }
    )

    total_lag = 0

    try:
        for partition in offsets:
            topic_partition = TopicPartition(
                partition.topic,
                partition.partition,
            )

            _, log_end_offset = consumer.get_watermark_offsets(
                topic_partition,
                timeout=5,
            )

            lag = max(
                0,
                log_end_offset - partition.offset,
            )

            total_lag += lag

        return total_lag

    finally:
        consumer.close()


def get_freshness_seconds():
    with sqlite3.connect(DB_PATH) as connection:
        row = connection.execute(
            """
            SELECT metric_value
            FROM pipeline_metrics
            WHERE metric_name = ?
            """,
            ("latest_source_timestamp_ms",),
        ).fetchone()

    if row is None:
        return None

    latest_source_timestamp_ms = int(row[0])
    current_timestamp_ms = int(time.time() * 1000)

    return (
        current_timestamp_ms - latest_source_timestamp_ms
    ) / 1000


def main():
    connector_state, task_states = get_connector_health()
    offsets = get_consumer_offsets()
    lag = get_consumer_lag(offsets)
    freshness = get_freshness_seconds()

    print("Mercury Pipeline Health")
    print("-" * 40)
    print(f"Connector:  {connector_state}")
    print(f"Tasks:      {', '.join(task_states)}")
    print(f"Lag:        {lag} events")

    if freshness is None:
        print("Freshness:  No source events processed yet")
    else:
        print(f"Freshness:  {freshness:.2f} seconds")


if __name__ == "__main__":
    main()