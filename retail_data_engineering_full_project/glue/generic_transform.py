
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("retail-generic-transform", {})

database = "retail_dwh_db"
tables = spark.sql(f"SHOW TABLES IN {database}").collect()

for t in tables:
    name = t.tableName
    if not name.startswith("raw_"):
        continue

    df = glueContext.create_dynamic_frame.from_catalog(
        database=database,
        table_name=name
    ).toDF()

    df = df.dropDuplicates()
    target = name.replace("raw_", "")

    df.write.mode("overwrite").parquet(f"s3://retail-dwh-processed/{target}/")
    print(f"Processed {name}")

job.commit()
