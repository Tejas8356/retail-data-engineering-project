
import boto3, os, json

print("Starting S3 Upload...")

bucket = "retail-dwh-raw"
base_path = os.path.dirname(os.path.dirname(__file__))
data_path = os.path.join(base_path, "sample_data")
config_path = os.path.join(base_path, "config", "uploaded_files.json")

with open(config_path, "r") as f:
    uploaded = json.load(f)["uploaded_files"]

s3 = boto3.client("s3")

for file in os.listdir(data_path):
    if file in uploaded:
        continue

    file_path = os.path.join(data_path, file)

    if file.startswith("customers"):
        s3_key = f"postgres/customers/{file}"
    elif file.startswith("orders"):
        s3_key = f"postgres/orders/{file}"
    elif file.startswith("order_items"):
        s3_key = f"postgres/order_items/{file}"
    elif file.startswith("products"):
        s3_key = f"mysql/products/{file}"
    elif file.startswith("stores"):
        s3_key = f"mysql/stores/{file}"
    elif file.startswith("inventory"):
        s3_key = f"mysql/inventory/{file}"
    else:
        continue

    print(f"Uploading {file}...")
    s3.upload_file(file_path, bucket, s3_key)
    uploaded.append(file)

with open(config_path, "w") as f:
    json.dump({"uploaded_files": uploaded}, f, indent=2)

print("Upload Completed")
