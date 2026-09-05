import csv
import random
from datetime import date, timedelta
from pathlib import Path


SYMBOLS = ["TCS", "INFY", "RELIANCE", "HDFCBANK", "ITC", "WIPRO", "SBIN", "AXISBANK", "MARUTI", "SUNPHARMA"]


def create_csv(row_count=100000):
    output_path = Path(__file__).with_name("stock_transactions_large.csv")
    random_generator = random.Random(42)
    start_date = date(2024, 1, 1)

    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["date", "symbol", "buy_price", "sell_price", "quantity"])
        for row_number in range(row_count):
            symbol = SYMBOLS[row_number % len(SYMBOLS)]
            buy_price = round(random_generator.uniform(500, 4000), 2)
            price_change = random_generator.uniform(-0.08, 0.12)
            sell_price = round(buy_price * (1 + price_change), 2)
            quantity = random_generator.randint(100, 5000)
            transaction_date = start_date + timedelta(days=row_number % 365)
            writer.writerow(
                [transaction_date.isoformat(), symbol, buy_price, sell_price, quantity]
            )

    print(f"Created {row_count:,} rows in {output_path.name}")


if __name__ == "__main__":
    create_csv()
