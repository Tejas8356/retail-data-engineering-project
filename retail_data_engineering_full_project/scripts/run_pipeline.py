
import subprocess, os

print("Running Full Pipeline...")

base = os.path.dirname(__file__)

subprocess.run(["python", os.path.join(base, "extract_postgres.py")])
subprocess.run(["python", os.path.join(base, "extract_mysql.py")])
subprocess.run(["python", os.path.join(base, "upload_to_s3.py")])

print("Pipeline Completed")
