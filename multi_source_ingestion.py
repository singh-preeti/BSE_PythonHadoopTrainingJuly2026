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


# ============================================================
# 5. READ CSV SOURCE
# ============================================================

print("Reading CSV Data")

customers = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("data/customers")

customers.show()

customers.printSchema()


# ============================================================
# 6. READ JSON SOURCE
# ============================================================


print("Reading JSON Data")


orders = spark.read \
    .option("inferSchema", "true") \
    .json("data/orders")

orders.show()

orders.printSchema()


# ============================================================
# 7. READ PARQUET SOURCE
# ============================================================


print("Reading Parquet Data")


payments = spark.read \
    .parquet("data/payments")

payments.show()

payments.printSchema()


# ============================================================
# 8. REGISTER DATAFRAMES AS SQL TABLES
# ============================================================

customers.createOrReplaceTempView("customers")
orders.createOrReplaceTempView("orders")
payments.createOrReplaceTempView("payments") 

# ============================================================
# 9. BASIC SPARK SQL QUERY
# ============================================================


print(" BASIC SPARK SQL QUERY")


result = spark.sql("""
    SELECT
        customerId,
        name,
        age
    FROM customers
    WHERE age >= 30
""")

result.show()



print("\nSQL temporary views created successfully.")


# ============================================================
# 10. JOIN CUSTOMER + ORDER DATA
# ============================================================

print("       CUSTOMER + ORDER JOIN")


customer_orders = spark.sql("""
    SELECT
        c.customerId,
        c.name,
        o.orderId,
        o.amount
    FROM customers c
    INNER JOIN orders o
        ON c.customerId = o.customerId
""")

customer_orders.show()




