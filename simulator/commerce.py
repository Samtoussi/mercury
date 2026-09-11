from simulator.db import get_connection


def place_order(customer_id, product_id, quantity):
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    p.name,
                    p.price,
                    i.stock_quantity
                FROM products p
                JOIN inventory i
                    ON p.product_id = i.product_id
                WHERE p.product_id = %s
                """,
                (product_id,),
            )

            product = cursor.fetchone()

            if product is None:
                raise ValueError(f"Product {product_id} does not exist")

            name, price, stock_quantity = product

            if quantity <= 0:
                raise ValueError("Quantity must be greater than 0")

            if stock_quantity < quantity:
                raise ValueError(
                    f"Not enough stock for {name}. "
                    f"Requested: {quantity}, available: {stock_quantity}"
                )

            total_amount = price * quantity

            cursor.execute(
                """
                INSERT INTO orders (
                    customer_id,
                    total_amount
                )
                VALUES (%s, %s)
                RETURNING order_id
                """,
                (customer_id, total_amount),
            )

            order_id = cursor.fetchone()[0]

            cursor.execute(
                """
                INSERT INTO order_items (
                    order_id,
                    product_id,
                    quantity,
                    unit_price
                )
                VALUES (%s, %s, %s, %s)
                """,
                (
                    order_id,
                    product_id,
                    quantity,
                    price,
                ),
            )

            cursor.execute(
                """
                UPDATE inventory
                SET
                    stock_quantity = stock_quantity - %s,
                    updated_at = NOW()
                WHERE product_id = %s
                """,
                (
                    quantity,
                    product_id,
                ),
            )

            cursor.execute(
                """
                INSERT INTO payments (
                    order_id,
                    amount,
                    status
                )
                VALUES (%s, %s, 'SUCCESS')
                """,
                (
                    order_id,
                    total_amount,
                ),
            )

            cursor.execute(
                """
                UPDATE orders
                SET
                    status = 'PAID',
                    updated_at = NOW()
                WHERE order_id = %s
                """,
                (order_id,),
            )

            print(f"Product: {name}")
            print(f"Price: {price}")
            print(f"Quantity: {quantity}")
            print(f"Stock before: {stock_quantity}")
            print(f"Stock after: {stock_quantity - quantity}")
            print(f"Order total: {total_amount}")
            print(f"Payment status: SUCCESS")
            print(f"Order status: PAID")
            print(f"Created order: {order_id}")

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()

def restock_product(product_id, quantity):
    if quantity <= 0:
        raise ValueError("Restock quantity must be greater than 0")

    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE inventory
                SET
                    stock_quantity = stock_quantity + %s,
                    updated_at = NOW()
                WHERE product_id = %s
                RETURNING stock_quantity
                """,
                (
                    quantity,
                    product_id,
                ),
            )

            result = cursor.fetchone()

            if result is None:
                raise ValueError(f"Product {product_id} does not exist")

            new_stock = result[0]

        connection.commit()

        print(
            f"Restocked product {product_id} by {quantity}. "
            f"New stock: {new_stock}"
        )

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()

        