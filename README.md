
# 🏈 E2E Sport Sales Data Pipeline with Airflow

An end-to-end data pipeline built with 💪 **Apache Airflow**, designed to simulate real-time sales and customer activity in an American football gear store — from raw CSVs to transformed data and production-ready PostgreSQL tables.

> 🎯 **Goal**: Automate the full lifecycle of data — ingestion, validation, transformation, and storage — using a modern orchestration tool and real cloud-like infrastructure (MinIO, Postgres).

---

## 🚀 Features

✅ Periodic **data simulation** using historical datasets  
✅ Modular, production-grade **ETL with Airflow DAGs**  
✅ File-based ingestion via **MinIO** (S3-compatible)  
✅ Data deduplication, normalization, and transformation  
✅ Clean database schema with **dimension/fact tables**  
✅ Scalable structure for **future sales analytics & dashboards**

---

## 🧩 Tech Stack

| Layer        | Tool / Technology |
|-------------|-------------------|
| Orchestration | 🛫 Apache Airflow |
| Storage      | 🪣 MinIO (S3 API) |
| Database     | 🐘 PostgreSQL     |
| Containers   | 🐳 Docker / Docker Compose |
| Language     | 🐍 Python         |

---

## 🗺️ Architecture Overview

```text
                        +---------------------+
                        |   Historical CSVs   |
                        +----------+----------+
                                   |
                        (Airflow Task: download_data)
                                   |
                        +----------v----------+
                        |     MinIO (S3)      |
                        |  - raw_data bucket  |
                        |  - temp-files       |
                        |  - processed-files  |
                        +----------+----------+
                                   |
             +---------------------+---------------------+
             |                                           |
+------------v-----------+                +--------------v-----------+
|  transform_customers() |                |   transform_sales()      |
|  - Clean/normalize     |                |  - Add currency rates    |
|  - Deduplicate records |                |  - Join customers        |
+------------+-----------+                +--------------+-----------+
             |                                           |
   +---------v----------+                      +---------v----------+
   | upsert_customers() |                      |  upsert_sales()    |
   |  (PostgreSQL DB)   |                      |  (PostgreSQL DB)   |
   +--------------------+                      +--------------------+
```

---

## 📁 Folder Structure

```
.
├── dags/
│   └── main_etl_dag.py        # Main DAG definition
├── scripts/                   # Bootstrap & infra helpers
├── etl/
│   ├── customers/             # Customer ETL logic
│   └── sales/                 # Sales ETL logic
├── configs/                   # Env vars, S3 config, schemas
├── docker-compose.yml
└── README.md
```

---

## 🛠️ Setup & Run

### 1. 🔧 Clone the project

```bash
git clone https://github.com/ATanskiy/e2e_sport_sales_pipeline_with_airflow.git
cd e2e_sport_sales_pipeline_with_airflow
```

### 2. 🐳 Start all containers

```bash
docker-compose up -d
```

This brings up:
- PostgreSQL (with 3 schemas: `raw_data`, `prod`, `playground`)
- MinIO with buckets for raw and processed data
- Apache Airflow (webserver, scheduler)

### 3. 🌍 Access Airflow UI

Navigate to: [http://localhost:8080](http://localhost:8080)  


## 🔁 DAG Lifecycle

```
    A[collect_new_files] --> B[transform_customers]
    B --> C[upsert_customers]
    A --> D[transform_sales]
    D --> E[upsert_sales]
    C --> F[move_to_processed]
    E --> F
```

## 🧠 Why This Project Rocks

✅ Simulates real production workloads  
✅ Modular, testable Python code  
✅ Follows data engineering best practices  
✅ Ready for cloud migration (GCS, AWS, Azure)  
✅ Clean, reproducible local development via Docker

---

## ✨ Author

Created with ❤️ by [@ATanskiy](https://github.com/ATanskiy)

If you find this helpful — ⭐ the repo and share the ⚡ knowledge!

---