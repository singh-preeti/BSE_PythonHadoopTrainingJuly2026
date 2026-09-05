import os
import sys
import argparse
from pathlib import Path
from warnings import catch_warnings

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType
from pyspark.storagelevel import StorageLevel


# ---------------------------------------------------------
# 1. STOCK DATA FILE
# ---------------------------------------------------------

CSV_PATH = Path(__file__).with_name("stock_transactions_large.csv")


# ---------------------------------------------------------
# 2. LOAD STOCK TRANSACTIONS
# ---------------------------------------------------------

def load_stock_transactions(spark, csv_path):
    """
    Load stock transaction data from CSV.
    """

    stock_schema = StructType([
        StructField("date", DateType(), True),
        StructField("symbol", StringType(), True),
        StructField("buy_price", DoubleType(), True),
        StructField("sell_price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
    ])

    return (
        spark.read
        .option("header", True)
        .schema(stock_schema)
        .csv(str(csv_path))
    )


def count_stock_transactions(spark, csv_path):
    """Run one Spark action so its job is easy to inspect in the UI."""
    stock_df = load_stock_transactions(spark, csv_path)
    transaction_count = stock_df.count()
    print(f"Input file: {csv_path.name}")
    print(f"Total transactions: {transaction_count:,}")




# ---------------------------------------------------------
# 11. MAIN APPLICATION
# ---------------------------------------------------------

def main(csv_path=CSV_PATH, keep_ui=False):

    # Windows configuration
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = (
        SparkSession.builder
        .appName("StockMarketProfitAnalyzer")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:

        count_stock_transactions(
            spark,
            csv_path
        )

        if keep_ui:

            input(
                "\nSpark UI is running at "
                "http://localhost:4040\n"
                "Press Enter to exit..."
            )

    finally:

        spark.stop()


# ---------------------------------------------------------
# 12. APPLICATION ENTRY POINT
# ---------------------------------------------------------

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Stock Market Profit Analyzer using PySpark"
    )

    parser.add_argument(
        "--keep-ui",
        action="store_true",
        help="Keep Spark alive so Spark UI can be inspected."
    )

    parser.add_argument(
        "--csv",
        default=str(CSV_PATH),
        help="Path to stock transaction CSV file."
    )

    arguments = parser.parse_args()

    main(
        csv_path=Path(arguments.csv),
        keep_ui=arguments.keep_ui
    )
