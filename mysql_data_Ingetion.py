from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ECommerceCustomerIngestion") \
    .master("local[*]") \
    .config(
        "spark.jars",
        r"C:\Users\Prashil Singh\Downloads\mysql-connector-j-9.6.0\mysql-connector-j-9.6.0.jar"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# MySQL Database Connection
jdbc_url = "jdbc:mysql://localhost:3306/ecommerce"

db_properties = {
    "user": "mysql",
    "password": "mysql",
    "driver": "com.mysql.cj.jdbc.Driver"
}


# Ingest customer table from MySQL
customers_df = spark.read \
    .jdbc(
        url=jdbc_url,
        table="customer",
        properties=db_properties
    )


# Display data
print("\n===== CUSTOMER DATA =====")
customers_df.show()

print("\n===== CUSTOMER SCHEMA =====")
customers_df.printSchema()


# Stop Spark
spark.stop()
