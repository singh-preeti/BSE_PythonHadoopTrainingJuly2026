from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    lit,
    count,
    sum,
    avg,
    max,
    min,
    round
)

# ============================================================
# PYSPARK TRADE DATA PROCESSING - COMPLETE ASSIGNMENT
# ============================================================

# ------------------------------------------------------------
# 1. Create Spark Session
# ------------------------------------------------------------

spark = SparkSession.builder \
    .appName("TradeProcessingAssignment") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# ------------------------------------------------------------
# 2. Read CSV
# ------------------------------------------------------------

trades_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("trades.csv")

print("\n" + "=" * 70)
print("INPUT DATA")
print("=" * 70)

trades_df.show(truncate=False)

print("Input Schema:")
trades_df.printSchema()


# ------------------------------------------------------------
# 3. Add Validation Status
# ------------------------------------------------------------

validated_trades = trades_df.withColumn(
    "status",
    when(
        (col("quantity") > 0) &
        (col("price") > 0) &
        (col("trade_type").isin("BUY", "SELL")),
        "VALID"
    ).otherwise("INVALID")
)


# ------------------------------------------------------------
# 4. Display Invalid Trades
# ------------------------------------------------------------

invalid_trades = validated_trades.filter(
    col("status") == "INVALID"
)

print("\n" + "=" * 70)
print("INVALID TRADES")
print("=" * 70)

invalid_trades.show(truncate=False)


# ------------------------------------------------------------
# 5. Keep Only Valid Trades
# ------------------------------------------------------------

valid_trades = validated_trades.filter(
    col("status") == "VALID"
)


# ------------------------------------------------------------
# 6. Calculate Trade Value
# ------------------------------------------------------------

processed_trades = valid_trades.withColumn(
    "trade_value",
    col("quantity") * col("price")
)


# ------------------------------------------------------------
# 7. Categorize Trade Value
# ------------------------------------------------------------

processed_trades = processed_trades.withColumn(
    "trade_value_category",
    when(col("trade_value") < 10000, "LOW")
    .when(
        (col("trade_value") >= 10000) &
        (col("trade_value") <= 50000),
        "MEDIUM"
    )
    .otherwise("HIGH")
)


# ------------------------------------------------------------
# 8. Determine Risk Level
# ------------------------------------------------------------

processed_trades = processed_trades.withColumn(
    "risk_level",
    when(
        col("trade_value") > 50000,
        "HIGH"
    ).when(
        col("trade_value") >= 10000,
        "MEDIUM"
    ).otherwise("LOW")
)


# ------------------------------------------------------------
# 9. Calculate Transaction Fee
# BUY  -> 0.1%
# SELL -> 0.15%
# ------------------------------------------------------------

processed_trades = processed_trades.withColumn(
    "transaction_fee",
    when(
        col("trade_type") == "BUY",
        col("trade_value") * 0.001
    ).when(
        col("trade_type") == "SELL",
        col("trade_value") * 0.0015
    ).otherwise(0)
)

processed_trades = processed_trades.withColumn(
    "transaction_fee",
    round(col("transaction_fee"), 2)
)


# ------------------------------------------------------------
# 10. Calculate Net Trade Value
# BUY  -> trade_value + transaction_fee
# SELL -> trade_value - transaction_fee
# ------------------------------------------------------------

processed_trades = processed_trades.withColumn(
    "net_trade_value",
    when(
        col("trade_type") == "BUY",
        col("trade_value") + col("transaction_fee")
    ).when(
        col("trade_type") == "SELL",
        col("trade_value") - col("transaction_fee")
    ).otherwise(0)
)

processed_trades = processed_trades.withColumn(
    "net_trade_value",
    round(col("net_trade_value"), 2)
)


# ------------------------------------------------------------
# 11. Determine Action
# ------------------------------------------------------------

processed_trades = processed_trades.withColumn(
    "action",
    when(
        col("trade_type") == "BUY",
        "Add to holdings"
    ).when(
        col("trade_type") == "SELL",
        "Reduce holdings"
    ).otherwise("Unknown")
)


# ------------------------------------------------------------
# 12. Display Final Processed Trades
# ------------------------------------------------------------

print("\n" + "=" * 70)
print("FINAL PROCESSED TRADES")
print("=" * 70)

processed_trades.select(
    "trade_id",
    "symbol",
    "trade_type",
    "quantity",
    "price",
    "trade_value",
    "trade_value_category",
    "transaction_fee",
    "net_trade_value",
    "risk_level",
    "action"
).show(truncate=False)


# ------------------------------------------------------------
# 13. BUY vs SELL Analysis
# ------------------------------------------------------------

buy_sell_analysis = processed_trades.groupBy(
    "trade_type"
).agg(
    count("*").alias("total_trades"),
    sum("trade_value").alias("total_trade_value"),
    avg("trade_value").alias("average_trade_value")
).orderBy("trade_type")


print("\n" + "=" * 70)
print("BUY vs SELL ANALYSIS")
print("=" * 70)

buy_sell_analysis.show(truncate=False)


# ------------------------------------------------------------
# 14. Top 5 Most Valuable Trades
# ------------------------------------------------------------

top_5_trades = processed_trades.orderBy(
    col("trade_value").desc()
).limit(5)

print("\n" + "=" * 70)
print("TOP 5 MOST VALUABLE TRADES")
print("=" * 70)

top_5_trades.select(
    "trade_id",
    "symbol",
    "trade_type",
    "quantity",
    "price",
    "trade_value",
    "risk_level"
).show(truncate=False)


# ------------------------------------------------------------
# 15. Symbol-wise Analysis
# ------------------------------------------------------------

symbol_analysis = processed_trades.groupBy(
    "symbol"
).agg(
    sum("quantity").alias("total_quantity"),
    sum("trade_value").alias("total_trade_value"),
    count("*").alias("number_of_trades"),
    avg("trade_value").alias("average_trade_value")
).orderBy(
    col("total_trade_value").desc()
)


print("\n" + "=" * 70)
print("SYMBOL-WISE TRADE ANALYSIS")
print("=" * 70)

symbol_analysis.show(truncate=False)


# ------------------------------------------------------------
# 16. Highest Trading Symbol
# ------------------------------------------------------------

highest_trading_symbol = symbol_analysis.limit(1)

print("\n" + "=" * 70)
print("HIGHEST TRADING SYMBOL")
print("=" * 70)

highest_trading_symbol.show(truncate=False)


# ------------------------------------------------------------
# 17. Overall Trade Statistics
# ------------------------------------------------------------

overall_statistics = processed_trades.agg(
    count("*").alias("total_valid_records"),
    sum("trade_value").alias("total_trade_value"),
    avg("trade_value").alias("average_trade_value"),
    max("trade_value").alias("maximum_trade_value"),
    min("trade_value").alias("minimum_trade_value")
)

print("\n" + "=" * 70)
print("OVERALL TRADE STATISTICS")
print("=" * 70)

overall_statistics.show(truncate=False)


# ------------------------------------------------------------
# 18. Data Quality Report
# ------------------------------------------------------------

total_records = trades_df.count()
valid_records = validated_trades.filter(
    col("status") == "VALID"
).count()
invalid_records = validated_trades.filter(
    col("status") == "INVALID"
).count()

buy_records = processed_trades.filter(
    col("trade_type") == "BUY"
).count()

sell_records = processed_trades.filter(
    col("trade_type") == "SELL"
).count()

print("\n" + "=" * 70)
print("DATA QUALITY REPORT")
print("=" * 70)

print(f"Total Records       : {total_records}")
print(f"Valid Records       : {valid_records}")
print(f"Invalid Records     : {invalid_records}")
print(f"BUY Records         : {buy_records}")
print(f"SELL Records        : {sell_records}")


# ------------------------------------------------------------
# 19. Trade Value Category Analysis
# ------------------------------------------------------------

category_analysis = processed_trades.groupBy(
    "trade_value_category"
).agg(
    count("*").alias("number_of_trades"),
    sum("trade_value").alias("total_trade_value")
).orderBy(
    col("total_trade_value").desc()
)

print("\n" + "=" * 70)
print("TRADE VALUE CATEGORY ANALYSIS")
print("=" * 70)

category_analysis.show(truncate=False)


# ------------------------------------------------------------
# 20. Risk Level Analysis
# ------------------------------------------------------------

risk_analysis = processed_trades.groupBy(
    "risk_level"
).agg(
    count("*").alias("number_of_trades"),
    sum("trade_value").alias("total_trade_value")
).orderBy(
    col("total_trade_value").desc()
)

print("\n" + "=" * 70)
print("RISK LEVEL ANALYSIS")
print("=" * 70)

risk_analysis.show(truncate=False)


# ------------------------------------------------------------
# 21. Save Final Processed Data
# ------------------------------------------------------------

processed_trades.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("output/processed_trades")

print("\nFinal processed data saved to:")
print("output/processed_trades")


# ------------------------------------------------------------
# 22. Stop Spark
# ------------------------------------------------------------

spark.stop()

print("\n" + "=" * 70)
print("TRADE PROCESSING COMPLETED SUCCESSFULLY")
print("=" * 70)
