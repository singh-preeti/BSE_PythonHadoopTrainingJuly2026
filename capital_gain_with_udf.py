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



