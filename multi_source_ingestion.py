from pyspark.sql import SparkSession


# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

spark = SparkSession.builder \
    .appName("MultiSourceIngestion") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\n========================================")
print("   MULTI-SOURCE INGESTION USING SPARK")
print("========================================\n")


# ============================================================
# 2. CREATE SAMPLE CSV DATA
# ============================================================

customers_data = [
    ("101", "Rahul", 28),
    ("102", "Amit", 32),
    ("103", "Priya", 25),
    ("104", "Neha", 30)
]

customers_df = spark.createDataFrame(
    customers_data,
    ["customerId", "name", "age"]
)

customers_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("data/customers")

print("CSV source created successfully.")


# ============================================================
# 3. CREATE SAMPLE JSON DATA
# ============================================================

orders_data = [
    (1, "101", 5000),
    (2, "102", 3500),
    (3, "101", 2500),
    (4, "103", 7000),
    (5, "104", 4500)
]

orders_df = spark.createDataFrame(
    orders_data,
    ["orderId", "customerId", "amount"]
)

orders_df.write \
    .mode("overwrite") \
    .json("data/orders")

print("JSON source created successfully.")

# ============================================================
# 4. CREATE SAMPLE PARQUET DATA
# ============================================================

payments_data = [
    (1, "101", 5000, "SUCCESS"),
    (2, "102", 3500, "SUCCESS"),
    (3, "101", 2500, "SUCCESS"),
    (4, "103", 7000, "SUCCESS"),
    (5, "104", 4500, "PENDING")
]

payments_df = spark.createDataFrame(
    payments_data,
    ["paymentId", "customerId", "amount", "status"]
)

payments_df.write \
    .mode("overwrite") \
    .parquet("data/payments")

print("Parquet source created successfully.")




