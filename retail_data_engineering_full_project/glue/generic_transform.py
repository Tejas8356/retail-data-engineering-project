import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("retail-generic-transform", {})

database = "retail_dwh_db"

print("Fetching tables from Glue Catalog...")

tables = spark.sql(f"SHOW TABLES IN {database}").collect()

for table in tables:

    table_name = table.tableName

    # Process only raw tables
    if not table_name.startswith("raw_"):
        continue

    print(f"Processing {table_name}...")

    # Read from Glue Catalog
    datasource = glueContext.create_dynamic_frame.from_catalog(
        database=database,
        table_name=table_name
    )

    df = datasource.toDF()

    # =========================
    # Transformations
    # =========================

    # Remove duplicates
    df_clean = df.dropDuplicates()

    # Optional: Remove nulls (basic quality check)
    df_clean = df_clean.dropna(how="all")

    # =========================
    # Write to Processed Layer
    # =========================

    target_folder = table_name.replace("raw_", "")

    print(f"Writing to {target_folder}...")

    df_clean.write.mode("overwrite").parquet(
        f"s3://retail-dwh-processed/{target_folder}/"
    )

    print(f"{table_name} processed successfully!")

print("All tables processed successfully!")

job.commit()
