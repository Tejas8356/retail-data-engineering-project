
import pandas as pd
import psycopg2
from datetime import datetime
import os, json

print("Starting PostgreSQL Incremental Extraction...")

base_path = os.path.dirname(os.path.dirname(__file__))
config_path = os.path.join(base_path, "config", "last_run.json")
data_path = os.path.join(base_path, "sample_data")

os.makedirs(data_path, exist_ok=True)

with open(config_path, "r") as f:
    config = json.load(f)

conn = psycopg2.connect(
    host="localhost",
    database="retail_db",
    user="postgres",
    password="YOUR_PASSWORD"
)

tables = ["customers", "orders", "order_items"]

for table in tables:
    last_run = config["postgres"].get(table, "1900-01-01 00:00:00")
    print(f"Extracting {table} after {last_run}")

    query = f"SELECT * FROM {table} WHERE updated_at > '{last_run}'"
    df = pd.read_sql(query, conn)

    if not df.empty:
        file_name = f"{table}_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
        file_path = os.path.join(data_path, file_name)
        df.to_csv(file_path, index=False)
        config["postgres"][table] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"{table} extracted -> {file_path}")
    else:
        print(f"No new data for {table}")

with open(config_path, "w") as f:
    json.dump(config, f, indent=2)

conn.close()
print("PostgreSQL Extraction Completed")
