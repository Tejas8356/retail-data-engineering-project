
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("fact-sales", {})

orders = spark.read.parquet("s3://retail-dwh-processed/orders/")
items = spark.read.parquet("s3://retail-dwh-processed/order_items/")

fact = orders.join(items, "order_id")

fact = fact.withColumn("total_amount", col("quantity") * col("unit_price"))

fact.write.mode("overwrite").parquet("s3://retail-dwh-gold/fact_sales/")

job.commit()
