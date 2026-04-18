# 🚀 Retail Data Engineering Pipeline (AWS)

## 📌 Overview

This project demonstrates a complete **end-to-end data engineering pipeline** using AWS services.

It ingests data from multiple sources (PostgreSQL, MySQL, CSV), processes it using ETL pipelines, and builds a **star schema data warehouse** for analytics.

---

## 🧠 Architecture

```
PostgreSQL / MySQL / CSV
        ↓
Python Incremental ETL
        ↓
S3 Raw Layer (Bronze)
        ↓
AWS Glue (Transformation)
        ↓
S3 Processed Layer (Silver)
        ↓
Fact + Dimension Tables
        ↓
S3 Gold Layer
        ↓
Athena (Query Layer)
```


---

## ⚙️ Technologies Used

* Python
* AWS S3
* AWS Glue
* AWS Glue Crawler
* Amazon Athena
* PostgreSQL
* MySQL

---

## 🔄 Key Features

* ✅ Incremental data loading using timestamps
* ✅ Multi-source ingestion
* ✅ Generic Glue job (handles multiple tables)
* ✅ Parquet-based optimization
* ✅ Star schema design

---

## 🧱 Data Layers

### 🟤 Bronze (Raw)

* Stores raw data in S3

### ⚪ Silver (Processed)

* Cleaned and deduplicated data

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

## ▶️ How to Run

1. Update database credentials in scripts
2. Run pipeline:

```
python scripts/run_pipeline.py
```

3. Upload data to S3
4. Run Glue jobs
5. Query using Athena

---

## 📊 Sample Query

```sql
SELECT SUM(total_amount) AS revenue
FROM fact_sales;
```

---

## 🎯 Interview Talking Points

* Incremental ETL design
* Data lake architecture
* Glue vs traditional ETL
* Star schema modeling
* Cost optimization using Parquet

---

## 📄 Documentation

Detailed documentation available in `/docs/`

---

## 👨‍💻 Author

Tejas
