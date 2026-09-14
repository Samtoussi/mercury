import sqlite3
from pathlib import Path


DB_PATH = Path("consumer_state.db")


def init_state_db():
    with sqlite3.connect(DB_PATH) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS order_state (
                order_id INTEGER PRIMARY KEY,
                status TEXT NOT NULL,
                updated_at TEXT
            )
            """
        )

        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS order_history (
                topic TEXT NOT NULL,
                partition INTEGER NOT NULL,
                offset INTEGER NOT NULL,
                order_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                updated_at TEXT,
                PRIMARY KEY (topic, partition, offset)
            )
            """
        )


def update_order_state(order):
    with sqlite3.connect(DB_PATH) as connection:
        connection.execute(
            """
            INSERT INTO order_state (
                order_id,
                status,
                updated_at
            )
            VALUES (?, ?, ?)
            ON CONFLICT(order_id)
            DO UPDATE SET
                status = excluded.status,
                updated_at = excluded.updated_at
            """,
            (
                order["order_id"],
                order["status"],
                order["updated_at"],
            ),
        )


def append_order_history(topic, partition, offset, order):
    with sqlite3.connect(DB_PATH) as connection:
        connection.execute(
            """
            INSERT OR IGNORE INTO order_history (
                topic,
                partition,
                offset,
                order_id,
                status,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                topic,
                partition,
                offset,
                order["order_id"],
                order["status"],
                order["updated_at"],
            ),
        )