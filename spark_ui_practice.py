import os
import sys
import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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

    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(csv_path))
    )


# ---------------------------------------------------------
# 3. CALCULATE PROFIT FOR EACH TRANSACTION
# ---------------------------------------------------------

def calculate_transaction_profit(stock_df):
    """
    Calculate profit for every stock transaction.

    Profit = (Sell Price - Buy Price) * Quantity
    """

    return stock_df.withColumn(
        "profit",
        (F.col("sell_price") - F.col("buy_price"))
        * F.col("quantity")
    )


# ---------------------------------------------------------
# 4. ANALYZE PROFIT BY STOCK
# ---------------------------------------------------------

def analyze_profit_by_stock(profit_df):
    """
    Calculate total profit for each stock symbol.

    groupBy() causes a shuffle because records
    belonging to the same stock symbol are grouped together.
    """

    print("\n--- Stock Profit Analysis ---")

    stock_profit = (
        profit_df
        .groupBy("symbol")
        .agg(
            F.sum("profit").alias("total_profit")
        )
        .orderBy(F.desc("total_profit"))
    )

    stock_profit.show()

    return stock_profit


# ---------------------------------------------------------
# 5. PREPARE REUSABLE PROFIT DATA
# ---------------------------------------------------------

def prepare_reusable_profit_data(profit_df):
    """
    Persist calculated profit data so that it can
    be reused by multiple operations.
    """

    print("\n--- Preparing Reusable Stock Profit Data ---")

    reusable_profit_df = profit_df.persist(
        StorageLevel.MEMORY_AND_DISK
    )

    print("Stock profit data persisted using MEMORY_AND_DISK.")

    # Action - materializes the persisted data
    reusable_profit_df.count()

    print("Profit data is now ready for repeated analysis.")

    # Example of repeated analysis
    reusable_profit_df \
        .groupBy("symbol") \
        .agg(
            F.sum("profit").alias("total_profit")
        ) \
        .orderBy(F.desc("total_profit")) \
        .show()

    reusable_profit_df.unpersist()

    print("Reusable profit data removed from cache.")


# ---------------------------------------------------------
# 6. INSPECT STOCK ANALYSIS PLAN
# ---------------------------------------------------------

def inspect_stock_analysis_plan(profit_df):
    """
    Display the Spark physical execution plan.
    Useful for understanding how Spark executes
    stock analysis operations.
    """

    print("\n--- Stock Analysis Execution Plan ---")

    (
        profit_df
        .groupBy("symbol")
        .agg(
            F.sum("profit").alias("total_profit")
        )
        .explain("formatted")
    )


# ---------------------------------------------------------
# 7. ANALYZE STOCK DATA BY PARTITION
# ---------------------------------------------------------

def analyze_stock_profit_by_partition(profit_df):
    """
    Repartition stock transactions by symbol and
    analyze profit.

    This helps demonstrate partitioning and shuffle behavior.
    """

    print("\n--- Stock Partition Analysis ---")

    print("Repartitioning stock transactions by symbol...")

    partitioned_stock_df = profit_df.repartition(
        200,
        "symbol"
    )

    stock_profit = (
        partitioned_stock_df
        .groupBy("symbol")
        .agg(
            F.sum("profit").alias("total_profit")
        )
        .orderBy(F.desc("total_profit"))
    )

    print("Execution plan for partitioned stock analysis:")

    stock_profit.explain("formatted")

    print("Stock profit after partitioning:")

    stock_profit.show()


# ---------------------------------------------------------
# 8. FIND MOST PROFITABLE STOCK
# ---------------------------------------------------------

def find_top_profitable_stocks(profit_df):
    """
    Find stocks with the highest total profit.
    """

    print("\n--- Top Profitable Stocks ---")

    stock_profit = (
        profit_df
        .groupBy("symbol")
        .agg(
            F.sum("profit").alias("total_profit")
        )
        .orderBy(
            F.desc("total_profit")
        )
    )

    print("Stocks ranked by total profit:")

    stock_profit.show(10)


# ---------------------------------------------------------
# 9. FIND TOTAL TRADING VOLUME
# ---------------------------------------------------------

def analyze_stock_trading_volume(profit_df):
    """
    Calculate the total quantity traded for each stock.
    """

    print("\n--- Stock Trading Volume ---")

    stock_volume = (
        profit_df
        .groupBy("symbol")
        .agg(
            F.sum("quantity").alias("total_volume")
        )
        .orderBy(
            F.desc("total_volume")
        )
    )

    stock_volume.show(10)


# ---------------------------------------------------------
# 10. RUN COMPLETE STOCK ANALYSIS
# ---------------------------------------------------------

def run_stock_profit_analysis(spark, csv_path):
    """
    Run the complete stock profit analysis application.
    """

    print("\n==========================================")
    print("      STOCK MARKET PROFIT ANALYZER")
    print("==========================================")

    print(f"\nInput file: {csv_path.name}")

    # Load stock transactions
    stock_df = load_stock_transactions(
        spark,
        csv_path
    )

    print(f"Total transactions: {stock_df.count():,}")

    # Display original data
    print("\n--- Stock Transactions ---")

    stock_df.show(5)

    # Calculate transaction-level profit
    profit_df = calculate_transaction_profit(
        stock_df
    )

    print("\n--- Transactions With Profit ---")

    profit_df.select(
        "symbol",
        "buy_price",
        "sell_price",
        "quantity",
        "profit"
    ).show(5)

    print(
        "\nProfit Formula:"
        " (sell_price - buy_price) * quantity"
    )

    # Stock-wise profit analysis
    analyze_profit_by_stock(
        profit_df
    )

    # Persist reusable data
    prepare_reusable_profit_data(
        profit_df
    )

    # Inspect Spark execution plan
    inspect_stock_analysis_plan(
        profit_df
    )

    # Partition analysis
    analyze_stock_profit_by_partition(
        profit_df
    )

    # Find top stocks
    find_top_profitable_stocks(
        profit_df
    )

    # Trading volume analysis
    analyze_stock_trading_volume(
        profit_df
    )

    print("\n==========================================")
    print("       STOCK ANALYSIS COMPLETED")
    print("==========================================")


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
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:

        run_stock_profit_analysis(
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
