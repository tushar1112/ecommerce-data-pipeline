# E-Commerce Data Engineering Pipeline (v2.0) 🚀
![Python](https://img.shields.io/badge/Python-3.11-blue)
![PySpark](https://img.shields.io/badge/PySpark-Big%20Data-orange)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-Orchestration-green)
![Parquet](https://img.shields.io/badge/Storage-Parquet-purple)
![Power BI](https://img.shields.io/badge/Dashboard-Power%20BI-yellow)
![Architecture](https://img.shields.io/badge/Architecture-Bronze%20%7C%20Silver%20%7C%20Gold-brightgreen)

## 📌Project Overview

An end-to-end data pipeline built using Python, PySpark, Apache Airflow, Parquet, and Power BI applying the Medallion Architecture. This platform ingests raw e-commerce files, runs schemas through automated quarantine filters, executes stateful Change Data Capture (CDC) processing, builds an analytics Star Schema, and serves downstream executive reporting.

## ⭐ Key features
- Built an end-to-end Bronze → Silver → Gold Medallion Architecture pipeline
- Implemented full load and incremental CDC processing
- Designed reusable metadata-driven merge/upsert logic
- Implemented SCD Type 2 for customer and product dimensions
- Built incremental refresh logic for fact_sales and fact_payments
- Wrote rejected Silver records to a dedicated quarantine location
- Added referential integrity checks across business entities
- Added watermark-based batch tracking
- Added CDC metadata auditing for inserts, updates, and deletes
- Orchestrated the pipeline using Apache Airflow DAGs
- Built interactive Power BI dashboards for Sales, Product analytics


## 📐 Architecture



<img width="1400" height="700" alt="architecture" src="https://github.com/user-attachments/assets/d3258c05-9d3b-4341-8154-798ca9b59fca" />

<br>
<br>
The project follows a layered Medallion Architecture:

```text
Raw CSV Files
     ↓
🥉 Bronze Layer
     ↓
🥈 Silver Layer
     └── rejected records → quarantine location for backtracking
     ↓
🥇 Gold Layer
     ↓
📊 Power BI Dashboards
```
Apache Airflow orchestrates the pipeline layer by layer:
```text
Airflow DAG → Bronze → Silver → Gold → Power BI Export
```
<br>

## 🛠️ Technology Stack

| Area                | Tools / Technologies                |
| ------------------- | ----------------------------------- |
| Programming         | Python                              |
| Big Data Processing | PySpark                             |
| Storage Format      | Parquet                             |
| Configuration       | YAML                                |
| Data Modeling       | Star Schema, SCD Type 2             |
| Audit & Monitoring  | Watermark, DQ Summary, CDC Metadata |
| Dashboard           | Power BI                            |
| Version Control     | Git / GitHub                        |
| Orchestration       | Apache airflow                      |

<br>

## 📦 Data Sources

The pipeline uses synthetic e-commerce datasets representing real-world business entities.

### Master Data

* `customers.csv`
* `products.csv`
* `categories.csv`
* `suppliers.csv`
* `warehouses.csv`

### Transactional Data

* `orders.csv`
* `order_items.csv`
* `payments.csv`
* `inventory.csv`

### Incremental CDC Files

* `customers_increment.csv`
* `products_increment.csv`
* `orders_increment.csv`
* `order_items_increment.csv`
* `payments_increment.csv`

Each incremental file contains an `operation_type` column:

```text
INSERT
UPDATE
DELETE
```

---

## 🗂️ Project Structure

```text
E-commerce Project/
│
├── config/
│   └── app_config.yaml
│
├── data/
│   ├── raw_data/
│   │   ├── full_load/
│   │   └── increment_load/
│   │
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   ├── quarantine/
│   └── audit/
│       ├── watermark/
│       ├── dq_summary/
│       └── cdc_metadata/
│
├── data_generation/
│   └── generators_full_load/
│       ├── customers.py
│       ├── products.py
│       ├── orders_order_items_.py
│       ├── payments.py
|   └── generators_incremental_load/
│       ├── customers_incremental.py
│       ├── products_incremental.py
│       ├── orders_order_items_incremental.py
│       └── payments_incremental.py
│
├── src/
│   ├── common/
│   │   ├── config.py
│   │   ├── logger.py
│   │   ├── spark.py
│   │   ├── bootstrap.py
│   │   └── watermark.py
│   │
│   ├── bronze/
│   │   ├── ingestion.py
│   │   └── main.py
│   │
│   ├── silver/
│   │   ├── transformations.py
│   │   ├── customers.py
│   │   ├── products.py
│   │   ├── orders.py
│   │   ├── order_items.py
│   │   ├── payments.py
│   │   ├── inventory.py
│   │   ├── categories.py
│   │   ├── suppliers.py
│   │   ├── warehouses.py
│   │   └── common/
│   │       └── referential_integrity.py
│   │
│   └── gold/
│       ├── transformations.py
│       ├── dim_customer.py
│       ├── dim_product.py
│       ├── dim_date.py
│       ├── fact_sales.py
│       ├── fact_payments.py
│       └── main.py
│
├── screenshots/
│
│── test/
│   └── testing.py
├── requirements.txt
└── README.md
```
## 🌬️ Airflow Orchestration
Apache Airflow is used for layer-wise orchestration of the pipeline.
<img width="1600" height="600" alt="airflow" src="https://github.com/user-attachments/assets/d36cd294-a6f0-4bd9-b372-5139a91dca7e" />
Airflow Tasks
Task	Responsibility
- 🥉 Bronze Task: Ingest raw CSV files and write Parquet data.

- 🥈 Silver Task: Clean, validate, deduplicate, apply CDC, and route non-conforming records to the quarantine location.

- 🥇 Gold Task: Build SCD Type 2 dimensions and transactional fact tables.

- 🧾 Audit Task: Manage watermark thresholds, update data quality summaries, and record CDC operation metadata logs.

- 📊 Power BI Export Task: Serve analytics-ready Gold outputs to the dashboard plane for executive reporting.




---
## ⚙️ Pipeline Layers

### 🥉 Bronze Layer
Raw ingestion layer. Reads CSV files, adds audit metadata, and stores data as Parquet with minimal transformation.
Audit columns added:
```text
source_file
ingestion_timestamp
ingestion_date
batch_id
```
Load behavior:
```text
FULL        → overwrite Bronze table
INCREMENTAL → append CDC records
```
### 🥈 Silver Layer
Cleaned and validated business layer. Applies schema enforcement, type casting, deduplication, CDC merge/upsert logic, and referential integrity checks.
Bad records are not treated as a separate pipeline layer. They are written from Silver processing to:
```text
data/quarantine/silver/
```
Rejected records include:
```text
rejection_reason
processing_timestamp
```
### 🥇 Gold Layer
Analytics-ready layer for Power BI. Builds Star Schema tables from clean Silver data.
Dimensions:
```text
dim_customer    → SCD Type 2 customer dimension
dim_product     → SCD Type 2 product dimension
dim_date        → calendar dimension
```
Facts:
```text
fact_sales      → 1 row per order item
fact_payments   → 1 row per payment transaction
```
---


## 🔁 CDC & Incremental Processing

The project supports CDC-based incremental processing for:

```text
customers
products
orders
order_items
payments
```

### CDC Flow

```text
Incremental CSV
   ↓
Bronze Append
   ↓
Silver CDC Merge
   ↓
Gold SCD2 / Incremental Fact Refresh
```

CDC Operation Types

| Operation | Behavior                               |
| --------- | -------------------------------------- |
| INSERT    | Adds new record                        |
| UPDATE    | Updates current-state record           |
| DELETE    | Soft deletes record using deleted_flag |

### SCD Type 2 Implementation

SCD Type 2 is implemented for:

```text
dim_customer
dim_product
```
When a tracked attribute changes:
```text
1. Existing current row is expired
2. effective_end_date is updated
3. is_current becomes false
4. New version is inserted with is_current = true
```

Example:

| customer_id | city   | effective_start_date | effective_end_date | is_current |
| ----------- | ------ | -------------------- | ------------------ | ---------- |
| CUST001     | Pune   | 2025-01-01           | 2026-06-01         | false      |
| CUST001     | Mumbai | 2026-06-01           | 9999-12-31         | true       |

---

## 🧾 Audit & Monitoring

### 📍Watermark
Tracks the last successfully processed batch for each layer and table.
<br>
<img width="500" height="200" alt="watermark" src="https://github.com/user-attachments/assets/6675800e-913c-486f-bb15-c5ca864ffc42" />

### ✅ DQ Summary
Tracks Bronze count, Silver count, rejected count, rejected percentage, and processing status.
<br>
<img width="500" height="200" alt="dq_summary" src="https://github.com/user-attachments/assets/46c2be19-260f-433c-9b9d-e663a60d9bd1" />

### 🔄 CDC Metadata
Tracks insert, update, and delete counts for each incremental batch.
<br>
<img width="500" height="200" alt="cdc_metadata" src="https://github.com/user-attachments/assets/0e0e352b-37e5-4f8a-a22d-c1a8e6441687" />


---

## Power BI Dashboard (Basic)

### Sales Overview
<img width="1000" height="500" alt="image" src="https://github.com/user-attachments/assets/de9f376f-23d6-40d7-81a5-034deeeb20d0" />

### Product Performance (**different dataset)
<img width="1000" height="500" alt="image" src="https://github.com/user-attachments/assets/c4f59be0-40fa-44f2-8c9c-c16faef8dd0a" />



## How to Run

## 1. Generate Full Load Data

```bash
python data_generation/generators_full_load/customers.py
python data_generation/generators_full_load/products.py
python data_generation/generators_full_load/orders_order_items.py
python data_generation/generators_full_load/payments.py
etc
```

## 2. Run Bronze Full Load

Set config:

```yaml
load_type: FULL
```

Run:

```bash
python -m src.bronze.main
```

## 3. Run Silver Full Load

```bash
python -m src.silver.main
```

## 4. Run Gold Full Load

```bash
python -m src.gold.main
```

## 5. Generate Incremental Data

```bash
python data_generation/generator_incremental/customers_inc.py
python data_generation/generator_incremental/products_inc.py
python data_generation/generator_incremental/orders_order_items_inc.py
python data_generation/generator_incremental/payments_inc.py
```

## 6. Run Incremental Pipeline

Set config:

```yaml
load_type: INC
```

Then run:

```bash
python -m src.bronze.main
python -m src.silver.main
python -m src.gold.main
```

---

## Configuration Example

```yaml
load_type: INC

paths:
  raw_full: data/raw_data/full_load
  raw_inc: data/raw_data/increment_load
  bronze: data/bronze
  silver: data/silver
  gold: data/gold
  audit: data/audit
  rejected_data_silver: data/quarantine/silver
  
spark:
  master: local[*]
  shuffle_partitions: 8
  log_level:  WARN
  driver_memory : 4g

files:
  - customers.csv
  - categories.csv
  - suppliers.csv
  - warehouses.csv
  - products.csv

files_inc:
  customers:
      joining_key: customer_id
  products:
      joining_key: product_id
  orders:
    joining_key: order_id
```
---

## Future Enhancements

* Add Delta Lake MERGE instead of Parquet overwrite
* Deploy pipeline on Databricks or AWS EMR
* Store Gold layer in Snowflake or Redshift
* Add Great Expectations for data quality checks
* Add CI/CD using GitHub Actions

---

## Production Notes

This project uses local Parquet files for learning and portfolio demonstration.

In a real production environment:

* Delta Lake or Apache Iceberg would be used for ACID-compliant merge operations
* Object storage such as AWS S3 would store Bronze/Silver/Gold data
* Snowflake/Redshift/BigQuery would serve analytics workloads
* Data quality checks would be automated using frameworks like Great Expectations

---

## Author

**D. Tushar**

Data Engineering Portfolio Project
