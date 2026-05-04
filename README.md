# 🛒 End-to-End E-commerce Data Engineering Pipeline

## 📌 Overview

This project demonstrates a **production-style data engineering pipeline** built using PySpark, Apache Airflow, and Power BI.

It ingests raw CSV data, processes it through multi-layer architecture (Bronze, Silver, Gold), performs data quality checks, and serves curated datasets for business dashboards.

## 🏗️ Architecture



**Flow:**
Raw Data → Bronze Layer → Silver Layer → Data Quality → Gold Layer → Power BI Dashboard

---

## 🔄 Data Pipeline Flow

![Pipeline Flow](images/pipeline_flow.png)

---

## ⚙️ Tech Stack

| Layer         | Technology Used       |
| ------------- | --------------------- |
| Ingestion     | Python, PySpark       |
| Processing    | PySpark               |
| Orchestration | Apache Airflow        |
| Storage       | CSV (Data Lake style) |
| Visualization | Power BI              |
| Environment   | WSL (Linux)           |

---

## 📂 Dataset Details

The project simulates an e-commerce platform with following datasets:

* Customers
* Products
* Orders
* Sellers
* Payments
* Reviews

Each dataset is processed and transformed across multiple layers.

---

## 🧱 Data Architecture

### 🔹 Bronze Layer

* Raw data ingestion
* Schema enforcement
* Minimal transformations

### 🔹 Silver Layer

* Data cleaning
* Handling nulls and duplicates
* Standardization

### 🔹 Data Quality Layer

* Validation checks
* Data consistency rules

### 🔹 Gold Layer

* Business-ready tables
* Aggregations and KPIs
* Used for dashboarding

---

## 📊 Dashboard

![Dashboard](images/dashboard.png)

### Key Metrics:

* Total Revenue
* Total Orders
* Units Sold
* Average Order Value

### Insights:

* Month-wise revenue trends
* Category-wise performance
* Top brands by revenue

---

## ⚡ Orchestration (Airflow)

* DAG-based pipeline execution
* Layer-wise task separation:

  * Bronze → Silver → Quality → Gold
* Daily scheduling enabled
* Automatic failure handling

---

## 📁 Project Structure

```text
ecommerce-data-pipeline/
│
├── airflow/
│   └── dags/
│       └── ecommerce_pipeline.py
│
├── config/
│   └── app_config.yaml
│
├── data/
│   └── sample/
│
├── dashboard/
│   └── ecommerce_dashboard.pbix
│
├── images/
│
├── src/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   ├── quality_layer/
│   └── common/
│
├── test/
│
├── main.py
├── requirements.txt
├── README.md
└── .gitignore
```

---

## ▶️ How to Run

### 1. Setup Environment

```bash
python3 -m venv airflow_env
source airflow_env/bin/activate
pip install -r requirements.txt
```

---

### 2. Start Airflow

```bash
airflow webserver --port 8080
airflow scheduler
```

---

### 3. Trigger Pipeline

* Open Airflow UI → http://localhost:8080
* Trigger DAG: `ecommerce_pipeline`

---

## 💡 Key Features

* End-to-end ETL pipeline
* Layered data architecture
* Automated orchestration
* Power BI integration
* Optimized Spark transformations
* Single CSV output for BI compatibility

---

## 🧠 Key Learnings

* Designed scalable data pipeline architecture
* Implemented orchestration using Airflow
* Handled Spark multi-file output issue
* Built business-ready analytical datasets
* Integrated data engineering with BI tools

---

## 📬 Contact

D Tushar
tusharsahu3627@gmail.com
