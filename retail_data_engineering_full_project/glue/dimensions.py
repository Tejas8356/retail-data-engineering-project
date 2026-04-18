
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("dimensions", {})

# customers
spark.read.parquet("s3://retail-dwh-processed/customers/")\
    .dropDuplicates()\
    .write.mode("overwrite").parquet("s3://retail-dwh-gold/dim_customers/")

# products
spark.read.parquet("s3://retail-dwh-processed/products/")\
    .dropDuplicates()\
    .write.mode("overwrite").parquet("s3://retail-dwh-gold/dim_products/")

# stores
spark.read.parquet("s3://retail-dwh-processed/stores/")\
    .dropDuplicates()\
    .write.mode("overwrite").parquet("s3://retail-dwh-gold/dim_stores/")

job.commit()
