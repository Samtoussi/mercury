import time

from simulator.commerce import place_order, restock_product


def main():
    print("Mercury commerce simulator started.\n")

    while True:
        try:
            place_order(
                customer_id=1,
                product_id=1,
                quantity=1,
            )

        except ValueError as error:
            print(f"Business event rejected: {error}")

            if "Not enough stock" in str(error):
                restock_product(
                    product_id=1,
                    quantity=10,
                )

        except KeyboardInterrupt:
            print("\nSimulator stopped.")
            break

        print("Waiting 5 seconds...\n")
        time.sleep(5)


if __name__ == "__main__":
    main()