"
Capital Gains Calculator using PySpark - Real Example
Demonstrates all optimization techniques from the guide
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
import os

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
print("="*80)

# ============================================================================
# STEP 1: Load CSV Files
# ============================================================================
print("\n[1/8] Loading CSV files...")

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

# ============================================================================
# STEP 2: Data Skewness Analysis
# ============================================================================
print("\n[2/8] Analyzing data skewness...")

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
# STEP 3: Separate Buys and Sells with FIFO Ordering
# ============================================================================
print("\n[3/8] Separating transactions and applying FIFO ordering...")

# Define FIFO window: partition by ticker and trader, order by date
# traid_id  ticker  transaction
# t1         tcs     buy
# t1         hdfc    sell
window_fifo = Window.partitionBy("ticker", "trader_id").orderBy("date")
#transactions.orderBy("ticker","trader_id","date")


# group by vs window
buys = (transactions
    .filter(col("transaction_type") == "BUY")
    .withColumn("buy_seq", row_number().over(window_fifo))
)

sells = (transactions
    .filter(col("transaction_type") == "SELL")
    .withColumn("sell_seq", row_number().over(window_fifo))
)

print(f"[OK] Buy transactions: {buys.count()}")
print(f"[OK] Sell transactions: {sells.count()}")

# ============================================================================
# STEP 4: FIFO Join (Match Sells to Buys)
# ============================================================================
print("\n[4/8] Performing FIFO join (matching sells to buys)...")

# This is the complex join from the guide
# Each sell is matched with ALL preceding buys for FIFO calculation
# Rename columns to avoid conflicts
sells_renamed = sells.select(
    col("ticker").alias("s_ticker"),
    col("trader_id").alias("s_trader_id"),
    col("date").alias("sell_date"),
    col("price").alias("sell_price"),
    col("sell_seq").alias("s_sell_seq")
)

buys_renamed = buys.select(
    col("ticker").alias("b_ticker"),
    col("trader_id").alias("b_trader_id"),
    col("date").alias("buy_date"),
    col("price").alias("buy_price"),
    col("quantity").alias("buy_qty"),
    col("buy_seq").alias("b_buy_seq")
)

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
# STEP 5: Enrich with Trader, Stock, and Tax Info (Using Broadcast Joins)
# ============================================================================
print("\n[5/8] Enriching data with broadcast joins...")

result = (capital_gains
    .join(broadcast(traders.select("trader_id", "trader_name", "country")), "trader_id")
    .join(broadcast(stocks.select("ticker", "company_name", "sector")), "ticker")
    .join(broadcast(tax_rules.select("country", "long_term_rate", "short_term_rate")), "country")
)
result.show();
print("[OK] Broadcast joins completed")

# ============================================================================
# STEP 6: Calculate Holding Period and Tax Liability (SQL Functions - No UDFs!)
# ============================================================================
print("\n[6/8] Calculating holding period and tax liability...")

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




