import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

# Initialize Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("retail-fact-sales", {})

print("Reading processed orders data...")
orders_df = spark.read.parquet("s3://retail-dwh-processed/orders/")

print("Reading processed order_items data...")
order_items_df = spark.read.parquet("s3://retail-dwh-processed/order_items/")

# 🔗 Join Orders + Order Items
print("Joining orders and order_items...")
fact_sales_df = orders_df.join(order_items_df, "order_id")

# 🧹 Select required columns
fact_sales_df = fact_sales_df.select(
    col("order_id"),
    col("customer_id"),
    col("product_id"),
    col("quantity"),
    col("unit_price"),
    col("order_date")
)

# 💰 Calculate total amount
fact_sales_df = fact_sales_df.withColumn(
    "total_amount",
    col("quantity") * col("unit_price")
)

# 🧼 Remove duplicates (optional but good practice)
fact_sales_df = fact_sales_df.dropDuplicates()

# 📝 Write to Gold Layer
print("Writing fact_sales to Gold layer...")
fact_sales_df.write.mode("overwrite").parquet(
    "s3://retail-dwh-gold/fact_sales/"
)

print("fact_sales table created successfully!")

job.commit()
