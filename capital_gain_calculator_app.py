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
