import os
from decimal import Decimal

import psycopg
from dotenv import load_dotenv

load_dotenv()


def get_connection():
    return psycopg.connect(
        dbname=os.getenv("ANALYTICS_DB"),
        user=os.getenv("ANALYTICS_USER"),
        password=os.getenv("ANALYTICS_PASSWORD"),
        host=os.getenv("ANALYTICS_HOST"),
        port=os.getenv("ANALYTICS_PORT"),
    )


def upsert_order(order):
    with get_connection() as connection:
        connection.execute(
            """
            INSERT INTO orders (
                order_id,
                customer_id,
                status,
                total_amount,
                created_at,
                updated_at,
                shipping_method
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id)
            DO UPDATE SET
                customer_id = EXCLUDED.customer_id,
                status = EXCLUDED.status,
                total_amount = EXCLUDED.total_amount,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at,
                shipping_method = EXCLUDED.shipping_method
            WHERE EXCLUDED.updated_at >= orders.updated_at
            """,
            (
                order["order_id"],
                order["customer_id"],
                order["status"],
                Decimal(order["total_amount"]),
                order["created_at"],
                order["updated_at"],
                order.get("shipping_method"),
            ),
        )


def upsert_payment(payment):
    with get_connection() as connection:
        connection.execute(
            """
            INSERT INTO payments (
                payment_id,
                order_id,
                amount,
                status,
                created_at
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (payment_id)
            DO UPDATE SET
                order_id = EXCLUDED.order_id,
                amount = EXCLUDED.amount,
                status = EXCLUDED.status,
                created_at = EXCLUDED.created_at
            """,
            (
                payment["payment_id"],
                payment["order_id"],
                Decimal(payment["amount"]),
                payment["status"],
                payment["created_at"],
            ),
        )


def upsert_inventory(inventory):
    with get_connection() as connection:
        connection.execute(
            """
            INSERT INTO inventory (
                product_id,
                stock_quantity,
                updated_at
            )
            VALUES (%s, %s, %s)
            ON CONFLICT (product_id)
            DO UPDATE SET
                stock_quantity = EXCLUDED.stock_quantity,
                updated_at = EXCLUDED.updated_at
            WHERE EXCLUDED.updated_at >= inventory.updated_at
            """,
            (
                inventory["product_id"],
                inventory["stock_quantity"],
                inventory["updated_at"],
            ),
        )