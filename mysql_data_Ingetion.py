from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("ECommerceSQLIngestion") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


jdbc_url = "jdbc:mysql://localhost:3306/ecommerce"

db_properties = {
    "user": "root",
    "password": "root",
    "driver": "com.mysql.cj.jdbc.Driver"
}

customers_df = spark.read \
    .jdbc(
        url=jdbc_url,
        table="customers",
        properties=db_properties
    )

print("\n CUSTOMERS ")
customers_df.show()
customers_df.printSchema()


products_df = spark.read \
    .jdbc(
        url=jdbc_url,
        table="products",
        properties=db_properties
    )

print("\n PRODUCTS ")
products_df.show()
products_df.printSchema()



orders_df = spark.read \
    .jdbc(
        url=jdbc_url,
        table="orders",
        properties=db_properties
    )

print("\n ORDERS ")
orders_df.show()
orders_df.printSchema()

order_items_df = spark.read \
    .jdbc(
        url=jdbc_url,
        table="order_items",
        properties=db_properties
    )

print("\nORDER ITEMS ")
order_items_df.show()
order_items_df.printSchema()



customers_df.createOrReplaceTempView("customers")
products_df.createOrReplaceTempView("products")
orders_df.createOrReplaceTempView("orders")
order_items_df.createOrReplaceTempView("order_items")



ecommerce_df = spark.sql("""
    SELECT
        o.order_id,
        o.order_date,
        c.customer_id,
        c.customer_name,
        p.product_id,
        p.product_name,
        oi.quantity,
        p.price,
        (oi.quantity * p.price) AS total_amount
    FROM orders o
    JOIN customers c
        ON o.customer_id = c.customer_id
    JOIN order_items oi
        ON o.order_id = oi.order_id
    JOIN products p
        ON oi.product_id = p.product_id
""")



print("\n FINAL E-COMMERCE DATA")

ecommerce_df.show(truncate=False)


ecommerce_df.write \
    .mode("overwrite") \
    .parquet("output/ecommerce_data")


spark.stop()
