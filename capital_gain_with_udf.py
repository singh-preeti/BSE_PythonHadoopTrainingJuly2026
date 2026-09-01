from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
import os
import time

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CapitalGainsOptimization") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.autoBroadcastJoinThreshold", 50 * 1024 * 1024) \
    .getOrCreate()

# Get the directory where CSV files are located
csv_dir = os.path.dirname(os.path.abspath(__file__))

print("\n" + "="*80)
print("CAPITAL GAINS OPTIMIZATION WITH PYSPARK")
print("Including WIDE and NARROW Transformation Analysis")
print("="*80)

# ============================================================================
# STEP 1: Load CSV Files (NARROW - No Shuffle)
# ============================================================================
print("\n[1/10] Loading CSV files...")
print("  [NARROW] FileScan: Reads data from CSV without shuffling")

transactions = spark.read.csv(
    os.path.join(csv_dir, "transactions.csv"),
    header=True,
    inferSchema=True
)

traders = spark.read.csv(
    os.path.join(csv_dir, "traders.csv"),
    header=True,
    inferSchema=True
)

stocks = spark.read.csv(
    os.path.join(csv_dir, "stocks.csv"),
    header=True,
    inferSchema=True
)

tax_rules = spark.read.csv(
    os.path.join(csv_dir, "tax_rules.csv"),
    header=True,
    inferSchema=True
)

market_prices = spark.read.csv(
    os.path.join(csv_dir, "market_prices.csv"),
    header=True,
    inferSchema=True
)

print(f"[OK] Transactions: {transactions.count()} records")
print(f"[OK] Traders: {traders.count()} records")
print(f"[OK] Stocks: {stocks.count()} records")
print(f"[OK] Tax Rules: {tax_rules.count()} records")
print(f"[OK] Market Prices: {market_prices.count()} records") 


import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType

# Regular (scalar) Python UDF - easy to read but slower at scale
@udf(StringType())
def classify_gain_py(total_gain):
    if total_gain is None:
        return None
    try:
        g = float(total_gain)
    except Exception:
        return None
    if g < 0:
        return "LOSS"
    if g < 100:
        return "SMALL_GAIN"
    if g < 1000:
        return "MEDIUM_GAIN"
    return "LARGE_GAIN"

  #classify_gain_udf = udf(classify_gain_py, StringType())




@pandas_udf(DoubleType())
def adjusted_gain_pandas_udf(total_gain: pd.Series) -> pd.Series:
    # Compute fee as max(1.0, 0.001 * abs(total_gain))
    # This models a per-trade commission or fee based on trade size
    fee = (total_gain.abs() * 0.001).fillna(0.0)
    final_fee = fee.clip(lower=1.0)
    return total_gain - final_fee


# ============================================================================
# STEP 2: Data Skewness Analysis (WIDE - GroupBy Shuffle)
# ============================================================================
print("\n[2/10] Analyzing data skewness...")
print("  [WIDE] groupBy + count: Requires shuffle to aggregate by ticker")

ticker_distribution = transactions.groupBy("ticker").count().orderBy(col("count").desc())
print("\nTicker Distribution:")
ticker_distribution.show()

skew_stats = ticker_distribution.agg(
    avg("count").alias("avg_count"),
    max("count").alias("max_count"),
    min("count").alias("min_count")
).collect()[0]

print(f"\nSkew Statistics:")
print(f"  Average count per ticker: {skew_stats['avg_count']:.0f}")
print(f"  Max count (hottest ticker): {skew_stats['max_count']}")
print(f"  Min count (coldest ticker): {skew_stats['min_count']}")
print(f"  Skew ratio (max/min): {skew_stats['max_count']/skew_stats['min_count']:.1f}x") 



# ============================================================================
# STEP 3: NARROW Transformations - Filter and Select
# ============================================================================
print("\n[3/10] Applying NARROW transformations (filter & select)...")
print("  [NARROW] filter: Removes rows locally, no shuffle")
print("  [NARROW] select: Projects columns, no shuffle")
print("  [NARROW] withColumn: Adds/modifies columns, can be pipelined")

# NARROW: Filter transactions locally
buys_filtered = transactions.filter(col("transaction_type") == "BUY")
sells_filtered = transactions.filter(col("transaction_type") == "SELL")

# NARROW: Select only needed columns (Projection Pruning)
buys = buys_filtered.select(
    col("ticker"),
    col("trader_id"),
    col("date"),
    col("price"),
    col("quantity")
)

sells = sells_filtered.select(
    col("ticker"),
    col("trader_id"),
    col("date"),
    col("price")
)

print(f"[OK] Buy transactions after filter+select: {buys.count()}")
print(f"[OK] Sell transactions after filter+select: {sells.count()}")


# ============================================================================
# STEP 4: NARROW + WIDE Transformations - FIFO Window Ordering
# ============================================================================
print("\n[4/10] Applying Window Functions...")
print("  [WIDE] Window with partitionBy: Shuffles to group by (ticker, trader_id)")
print("  [NARROW] Window with orderBy: Orders within each partition (no shuffle)")
print("  [NARROW] row_number(): Assigns numbers within groups")

# WIDE: Partition requires shuffle to group by ticker and trader_id
# NARROW: Order by date happens within each partition after shuffle
window_fifo = Window.partitionBy("ticker", "trader_id").orderBy("date")

buys_with_seq = buys.withColumn("buy_seq", row_number().over(window_fifo))
sells_with_seq = sells.withColumn("sell_seq", row_number().over(window_fifo))

print(f"[OK] FIFO sequences assigned")

# ============================================================================
# STEP 5: WIDE Transformation - Complex Join (FIFO Matching)
# ============================================================================
print("\n[5/10] Performing FIFO join...")
print("  [WIDE] join: Complex join requires shuffle (FIFO matching)")
print("  Strategy: Rename columns to avoid conflicts, then join on conditions")

# Rename columns to avoid conflicts
sells_renamed = sells_with_seq.select(
    col("ticker").alias("s_ticker"),
    col("trader_id").alias("s_trader_id"),
    col("date").alias("sell_date"),
    col("price").alias("sell_price"),
    col("sell_seq").alias("s_sell_seq")
)

buys_renamed = buys_with_seq.select(
    col("ticker").alias("b_ticker"),
    col("trader_id").alias("b_trader_id"),
    col("date").alias("buy_date"),
    col("price").alias("buy_price"),
    col("quantity").alias("buy_qty"),
    col("buy_seq").alias("b_buy_seq")
)

# WIDE: Join requires shuffle to match records across partitions
capital_gains = (sells_renamed
    .join(
        buys_renamed,
        on=[
            col("s_ticker") == col("b_ticker"),
            col("s_trader_id") == col("b_trader_id"),
            col("b_buy_seq") <= col("s_sell_seq")
        ],
        how="inner"
    )
    .select(
        col("s_ticker").alias("ticker"),
        col("s_trader_id").alias("trader_id"),
        col("buy_date"),
        col("sell_date"),
        col("buy_price"),
        col("sell_price"),
        col("buy_qty").alias("quantity"),
        (col("sell_price") - col("buy_price")).alias("gain_per_share"),
        ((col("sell_price") - col("buy_price")) * col("buy_qty")).alias("total_gain")
    )
)

print(f"[OK] Capital gain matches created: {capital_gains.count()}")
print("\nSample gains (first 10):")
capital_gains.show(10, truncate=False)


