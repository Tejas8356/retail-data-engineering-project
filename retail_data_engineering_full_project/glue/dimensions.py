import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("retail-dimensions-transform", {})

print("Starting Dimension Tables Creation...")

# =========================
# DIM CUSTOMERS
# =========================
print("Processing dim_customers...")
customers_df = spark.read.parquet("s3://retail-dwh-processed/customers/")

dim_customers = customers_df.select(
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "city",
    "country"
).dropDuplicates()

dim_customers.write.mode("overwrite").parquet(
    "s3://retail-dwh-gold/dim_customers/"
)

print("dim_customers created!")

# =========================
# DIM PRODUCTS
# =========================
print("Processing dim_products...")
products_df = spark.read.parquet("s3://retail-dwh-processed/products/")

dim_products = products_df.select(
    "product_id",
    "product_name",
    "category",
    "cost_price"
).dropDuplicates()

dim_products.write.mode("overwrite").parquet(
    "s3://retail-dwh-gold/dim_products/"
)

print("dim_products created!")

# =========================
# DIM STORES
# =========================
print("Processing dim_stores...")
stores_df = spark.read.parquet("s3://retail-dwh-processed/stores/")

dim_stores = stores_df.select(
    "store_id",
    "store_name",
    "city",
    "country"
).dropDuplicates()

dim_stores.write.mode("overwrite").parquet(
    "s3://retail-dwh-gold/dim_stores/"
)

print("dim_stores created!")

print("All dimension tables created successfully!")

job.commit()
