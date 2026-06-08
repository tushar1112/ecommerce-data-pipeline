# E-Commerce Data Engineering Pipeline

## Project Overview

This project is an end-to-end **E-Commerce Data Engineering Pipeline** built using **Python, PySpark, SQL, Parquet, and Medallion Architecture**.

The pipeline processes raw e-commerce data from multiple source files, applies data cleaning and validation, handles bad records through quarantine, performs CDC-based incremental processing, and builds analytics-ready Gold layer tables using dimensional modeling.

The project is designed to demonstrate production-style Data Engineering concepts such as:

* Full load and incremental load processing
* CDC merge/upsert logic
* Data quality validation
* Quarantine handling
* Referential integrity checks
* SCD Type 2 dimensions
* Incremental fact table refresh
* Watermark-based batch tracking
* CDC metadata auditing

---

## Architecture

![E-Commerce Data Pipeline Architecture](docs/architecture.png)

The project follows a layered Medallion Architecture:

```text
Raw Layer
   ↓
Bronze Layer
   ↓
Silver Layer
   ↓
Gold Layer
   ↓
Analytics Layer
```

---

## Tech Stack

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

---

## Data Sources

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

## Project Structure

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
│   └── generators/
│       ├── customers_generator.py
│       ├── products_generator.py
│       ├── orders_order_items_generator.py
│       ├── payments_generator.py
│       ├── customers_incremental_generator.py
│       ├── products_incremental_generator.py
│       ├── orders_order_items_incremental_generator.py
│       └── payments_incremental_generator.py
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
├── docs/
│   └── architecture.png
│
│── test/
│   └── testing.py
├── requirements.txt
└── README.md
```

---

## Pipeline Layers

## 1. Raw Layer

The Raw layer contains source CSV files.

It includes:

* Full load files
* Incremental CDC files
* Dirty data simulation for realistic validation scenarios

Examples of dirty data include:

* Invalid customer IDs
* Invalid product IDs
* Bad date formats
* Negative quantities
* Invalid prices
* Missing values
* Duplicate business keys

---

## 2. Bronze Layer

The Bronze layer stores raw ingested data in Parquet format.

### Key Responsibilities

* Read CSV source files
* Add audit columns
* Add batch ID
* Add source file information
* Add ingestion timestamp
* Preserve raw data without heavy transformations

### Audit Columns Added

```text
source_table/file
ingestion_timestamp
ingestion_date
batch_id
```

### Load Types

```text
FULL         → overwrite Bronze table
INCREMENTAL  → append CDC records to Bronze table
```

---

## 3. Silver Layer

The Silver layer contains cleaned, validated, and current-state business tables.

### Key Responsibilities

* Schema enforcement
* Type casting
* Data standardization
* Data quality checks
* Deduplication
* Quarantine handling
* Referential integrity validation
* CDC merge/upsert logic

### Silver CDC Handling

For incremental data:

```text
INSERT  → add new record
UPDATE  → replace current record
DELETE  → soft delete using deleted_flag = true
```

Silver stores the latest/current version of each business entity.

Example:

```text
customers
products
orders
order_items
payments
inventory
```

---

## Quarantine Layer

Invalid records are moved to the Quarantine layer instead of being silently dropped.

Examples:

* Invalid customer ID
* Invalid email
* Invalid phone number
* Invalid product reference
* Invalid order reference
* Duplicate business key
* Failed referential integrity check

Each rejected record contains a rejection reason and processing timestamp.

---

## Referential Integrity Checks

The pipeline validates parent-child relationships before writing final Silver tables.

Examples:

| Child Table            | Parent Table            |
| ---------------------- | ----------------------- |
| products.category_id   | categories.category_id  |
| products.supplier_id   | suppliers.supplier_id   |
| inventory.product_id   | products.product_id     |
| inventory.warehouse_id | warehouses.warehouse_id |
| orders.customer_id     | customers.customer_id   |
| order_items.order_id   | orders.order_id         |
| order_items.product_id | products.product_id     |
| payments.order_id      | orders.order_id         |

Invalid child records are moved to quarantine.

---

## 4. Gold Layer

The Gold layer contains analytics-ready tables modeled using a Star Schema.

## Dimension Tables

### dim_customer

Tracks customer attributes and supports SCD Type 2.

SCD2 columns include:

```text
city
state
customer_status
marketing_opt_in
```

SCD metadata columns:

```text
effective_start_date
effective_end_date
is_current
```

### dim_product

Tracks product attributes and supports SCD Type 2.

SCD2 columns include:

```text
category_id
supplier_id
mrp
selling_price
cost_price
product_status
```

### dim_date

Calendar dimension used for date-based analytics.

Includes:

```text
date_key
full_date
year
quarter
month
month_name
week_of_year
day_of_week
is_weekend
```

---

## Fact Tables

### fact_sales

Grain:

```text
1 row per order item
```

Measures:

```text
quantity
unit_price
gross_amount
discount_amount
final_price
estimated_shipping_cost
```

Supports incremental refresh by rebuilding only affected `order_id` records.

### fact_payments

Grain:

```text
1 row per payment transaction
```

Measures:

```text
payment_amount
refund_amount
```

Supports incremental refresh by rebuilding only affected `payment_id` records.

---

## CDC and Incremental Processing

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

### CDC Operation Types

| Operation | Behavior                               |
| --------- | -------------------------------------- |
| INSERT    | Adds new record                        |
| UPDATE    | Updates current-state record           |
| DELETE    | Soft deletes record using deleted_flag |

---

## SCD Type 2 Implementation

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

## Audit and Monitoring

## Watermark Table

Tracks the last successfully processed batch for each layer and table.

Columns:

```text
table_name
layer
last_batch_id
status
last_processed_timestamp
updated_at
```
![Watermark](screenshots/watermark.png)

Used for:

* Batch tracking
* Failure recovery
* Preventing duplicate processing
* End-to-end lineage

---

## DQ Summary

Tracks data quality results for Silver processing.

Columns:

```text
batch_id
table_name
bronze_count
silver_count
quarantine_count
rejected_percentage
status
```

![DQ Summary](screenshots/dq_summary.png)

Used for:

* Data quality monitoring
* Reconciliation
* Identifying bad data trends

---

## CDC Metadata

Tracks CDC operation counts for incremental batches.

Columns:

```text
batch_id
table_name
insert_count
update_count
delete_count
processed_timestamp
```
![CDC Metadata](screenshots/cdc_metadata.png)

Used for:

* CDC auditing
* Incremental load monitoring
* Understanding daily data changes

---

## Power BI Dashboard

### Sales Overview
![Sales Overview](screenshots/powerbi_sales_overview.png)

### Product Performance
![Product Performance](screenshots/powerbi_product_performance.png)

### Payment Analysis
![Payment Analysis](screenshots/powerbi_payment_analysis.png)


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

## Key Project Highlights

* Built an end-to-end Medallion Architecture pipeline
* Implemented full load and incremental CDC processing
* Designed reusable metadata-driven incremental merge logic
* Implemented SCD Type 2 for customer and product dimensions
* Built incremental fact refresh for sales and payments
* Added quarantine handling for rejected records
* Added referential integrity checks across business entities
* Added watermark-based batch tracking
* Added CDC metadata for insert/update/delete auditing
* Created analytics-ready Gold layer using Star Schema

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
