
import pandas as pd
import mysql.connector
from datetime import datetime
import os, json

print("Starting MySQL Incremental Extraction...")

base_path = os.path.dirname(os.path.dirname(__file__))
config_path = os.path.join(base_path, "config", "last_run.json")
data_path = os.path.join(base_path, "sample_data")

with open(config_path, "r") as f:
    config = json.load(f)

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="YOUR_PASSWORD",
    database="retail_product_db"
)

tables = ["products", "stores", "inventory"]

for table in tables:
    last_run = config["mysql"].get(table, "1900-01-01 00:00:00")
    print(f"Extracting {table} after {last_run}")

    query = f"SELECT * FROM {table} WHERE updated_at > '{last_run}'"
    df = pd.read_sql(query, conn)

    if not df.empty:
        file_name = f"{table}_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
        file_path = os.path.join(data_path, file_name)
        df.to_csv(file_path, index=False)
        config["mysql"][table] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"{table} extracted -> {file_path}")
    else:
        print(f"No new data for {table}")

with open(config_path, "w") as f:
    json.dump(config, f, indent=2)

conn.close()
print("MySQL Extraction Completed")
