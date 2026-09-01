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



# ============================================================================
# STEP 6: NARROW + WIDE - Broadcast Joins vs Regular Joins
# ============================================================================
print("\n[6/10] Enriching data with broadcast joins...")
print("  [NARROW-ish] broadcast join: No shuffle for broadcasted table")
print("  Key insight: Small tables broadcasted to all executors")
print("  vs WIDE: Regular join would shuffle both large tables")

# Optimize by using broadcast for small dimension tables
result = (capital_gains
    .join(broadcast(traders.select("trader_id", "trader_name", "country")), "trader_id")
    .join(broadcast(stocks.select("ticker", "company_name", "sector")), "ticker")
    .join(broadcast(tax_rules.select("country", "long_term_rate", "short_term_rate")), "country")
)

print("[OK] Broadcast joins completed (no shuffle for small tables)")

# ============================================================================
# STEP 7: NARROW Transformations - Calculations with withColumn
# ============================================================================
print("\n[7/10] Calculating holding period and tax liability...")
print("  [NARROW] withColumn (datediff): Local calculation, no shuffle")
print("  [NARROW] when/otherwise: Conditional logic, no shuffle")
print("  [NARROW] column arithmetic: Math operations, can be pipelined")

holding_period_days = datediff(col("sell_date"), col("buy_date"))

result = result.withColumn(
    "holding_period_days",
    holding_period_days
).withColumn(
    "is_long_term",
    col("holding_period_days") >= 365
).withColumn(
    "tax_rate",
    when(
        col("holding_period_days") >= 365,
        col("long_term_rate")
    ).otherwise(
        col("short_term_rate")
    )
).withColumn(
    "tax_liability",
    col("total_gain") * col("tax_rate")
).withColumn(
    "net_gain",
    col("total_gain") - col("tax_liability")
)

print("[OK] Tax calculations completed (using native SQL functions, not UDFs)")






# ============================================================================
# USER-DEFINED FUNCTIONS: Apply UDFs to enrich the result
# ============================================================================
print("\n[USER UDFS] Applying UDFs: adjusted gain (vectorized if available) and gain category (Python UDF)")
# Try to use a native Spark expression for fee-adjusted gain to avoid Python worker timeouts.
# fee = greatest(1.0, 0.001 * abs(total_gain))
# adjusted_total_gain = total_gain - fee
adjusted_fee_expr = greatest(lit(1.0), (abs(col("total_gain")) * lit(0.001)))
result = result.withColumn(
    "adjusted_total_gain",
    (col("total_gain").cast(DoubleType()) - adjusted_fee_expr).cast(DoubleType())
).withColumn(
    "gain_category",
    classify_gain_py(col("total_gain"))
)



print("[OK] Applied adjustments using native Spark expression for adjusted_total_gain and native expression for gain_category")
# Replace Python UDF classification with native Spark expression to avoid Python worker overhead
result = result.withColumn(
    "gain_category",
    when(col("total_gain").isNull(), lit(None).cast(StringType()))
    .when(col("total_gain") < 0, lit("LOSS"))
    .when(col("total_gain") < 100, lit("SMALL_GAIN"))
    .when(col("total_gain") < 1000, lit("MEDIUM_GAIN"))
    .otherwise(lit("LARGE_GAIN"))
)

print("[OK] Gain category computed using native Spark expressions (no Python UDF)")
