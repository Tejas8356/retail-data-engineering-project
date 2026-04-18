# 🚀 Retail Data Engineering Pipeline (AWS)

## 📌 Overview

This project demonstrates a complete **end-to-end data engineering pipeline** built using AWS services.

It ingests data from multiple sources, processes it using ETL pipelines, and builds a **star schema data warehouse** for analytics.

---

## 🧠 Architecture

```
PostgreSQL / MySQL / CSV
        ↓
Python Incremental Pipeline
        ↓
S3 Raw (Bronze)
        ↓
AWS Glue ETL
        ↓
S3 Processed (Silver)
        ↓
Glue Jobs (Fact + Dimensions)
        ↓
S3 Gold Layer
        ↓
Athena (Query Layer)
```

---

## ⚙️ Technologies Used

* Python (ETL scripts)
* AWS S3 (Data Lake)
* AWS Glue (ETL)
* AWS Glue Crawler (Schema detection)
* Athena (Query engine)
* PostgreSQL / MySQL

---

## 🔄 Key Features

* ✅ Multi-source data ingestion
* ✅ Incremental data loading
* ✅ Automated ETL pipelines
* ✅ Generic Glue job (scalable design)
* ✅ Star schema data warehouse
* ✅ Parquet-based optimization

---

## 🧱 Data Layers

### 🟤 Bronze (Raw)

* Stored in S3
* No transformation

### ⚪ Silver (Processed)

* Cleaned data
* Deduplicated
* Stored as Parquet

### 🟡 Gold (Warehouse)

* fact_sales
* dim_customers
* dim_products
* dim_stores

---

## ⭐ Star Schema

```
          dim_customers
                |
dim_products — fact_sales — dim_stores
```

---

## 📊 Sample Query (Athena)

```sql
SELECT SUM(total_amount) AS revenue
FROM fact_sales;
```

---

## 🚀 How to Run

1. Run extraction scripts:

```bash
python run_pipeline.py
```

2. Upload data to S3

3. Run Glue jobs

4. Query using Athena

---

## 🎯 Interview Talking Points

* Incremental ETL design
* Data lake architecture (S3)
* Glue vs Spark processing
* Star schema modeling
* Parquet optimization

---

## 📄 Documentation

See `/docs/project_documentation.docx` for full details.

---

## 👨‍💻 Author

Tejas Varma
