# 📊 Video Game Sales Data Pipeline (Airflow + Docker)

## 📌 Overview

This project implements a **production-like data pipeline** that processes video game sales data using **Apache Airflow** for workflow orchestration.

The pipeline follows a modular **ETL architecture (Extract, Transform, Load)** with **data quality validations** before persisting the data into a **PostgreSQL database**.

The goal of this project is to simulate a real-world data engineering workflow using tools commonly found in professional environments.

---

## 🧱 Architecture

```text
CSV File
   ↓
Airflow DAG
   ↓
load_clean_data
   ↓
validate_clean_table
   ↓
aggregate_sales
   ↓
validate_aggregated_table
   ↓
PostgreSQL Database
```

The project separates Airflow's internal metadata database from the project database used to store processed data.

```text
Airflow metadata DB  → internal Airflow state
sales_pipeline DB    → project data
```

---

## ⚙️ Tech Stack

- Python
- Pandas
- PostgreSQL
- Docker
- Apache Airflow
- SQLAlchemy
- psycopg2
- python-dotenv

---

## 📂 Project Structure

```text
airflow_local/
├── dags/
│   ├── video_game_sales_pipeline_dag.py
│   └── sales_data_pipeline/
│       ├── src/
│       │   ├── extract/       # Data ingestion
│       │   ├── transform/     # Data cleaning and aggregation
│       │   ├── validate/      # Data quality checks
│       │   ├── load/          # Database read/write operations
│       │   └── utils/         # Database connection helpers
│       └── data/
│           └── raw/           # Source dataset
├── logs/
├── plugins/
├── config/
├── docker-compose.yaml
└── .env
```

---

## 🔄 Pipeline Flow

The Airflow DAG is split into multiple tasks:

```text
load_clean_data
→ validate_clean_table
→ aggregate_sales
→ validate_aggregated_table
```

### 1. `load_clean_data`

- Loads raw CSV data
- Cleans and transforms the dataset
- Stores the cleaned data in PostgreSQL as `games_clean`

### 2. `validate_clean_table`

- Reads `games_clean` from PostgreSQL
- Runs data quality validations before continuing

### 3. `aggregate_sales`

- Reads `games_clean`
- Aggregates sales by platform and year
- Stores the result as `sales_by_platform_year`

### 4. `validate_aggregated_table`

- Reads `sales_by_platform_year`
- Validates the final analytical table

---

## ✅ Data Quality Checks

### Clean Data (`games_clean`)

The pipeline validates that:

- `Year` does not contain null values
- `Year` is an integer type
- `Total_Sales` does not contain negative values

### Aggregated Data (`sales_by_platform_year`)

The pipeline validates that:

- There are no duplicate `Platform + Year` combinations
- `Total_Sales` does not contain negative values

If any validation fails, the task raises an exception and Airflow marks the task as **FAILED**.

---

## 🗄️ Database Design

The pipeline writes data into a dedicated PostgreSQL database:

```text
sales_pipeline
```

### Tables

| Table | Description |
|---|---|
| `games_clean` | Cleaned dataset after basic transformations |
| `sales_by_platform_year` | Aggregated sales by platform and year |

This separation avoids mixing project data with Airflow internal metadata.

---

## 🔁 Error Handling and Resilience

Airflow is responsible for task failure handling.

The DAG can be configured with retries:

```python
from datetime import timedelta

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}
```

This allows the pipeline to retry tasks when temporary failures occur, such as database connection issues.

Data quality issues are handled through explicit exceptions, allowing Airflow to stop the pipeline when invalid data is detected.

---

## 🐳 How to Run

### 1. Start Docker Desktop

Make sure Docker Desktop is running before executing Docker commands.

### 2. Start Airflow

From the `airflow_local` directory:

```bash
docker compose up -d
```

### 3. Access Airflow UI

Open:

```text
http://localhost:8080
```

Default credentials:

```text
airflow / airflow
```

### 4. Trigger the DAG

In the Airflow UI:

1. Enable `video_game_sales_pipeline`
2. Click **Trigger DAG**
3. Check that all tasks finish successfully

---

## 🔍 Validate Database Results

Enter the PostgreSQL container:

```bash
docker compose exec postgres psql -U airflow
```

Connect to the project database:

```sql
\c sales_pipeline
```

List tables:

```sql
\dt
```

Preview data:

```sql
SELECT * FROM games_clean LIMIT 5;
SELECT * FROM sales_by_platform_year LIMIT 5;
```

Exit PostgreSQL:

```sql
\q
```

---

## 📈 Project Evolution

The first version of the project was a simple Python script:

```text
CSV → Python script → CSV / PostgreSQL
```

The project was later refactored into an Airflow-orchestrated pipeline:

```text
CSV → Airflow DAG → PostgreSQL staging table → validation → analytical table
```

### Key improvements

- Removed unnecessary CSV intermediate steps
- Added orchestration with Apache Airflow
- Split the pipeline into independent tasks
- Added data quality validations
- Used PostgreSQL as an intermediate and final storage layer
- Separated Airflow metadata from project data

---

## 💡 Key Learnings

This project helped practice:

- Building modular ETL pipelines
- Using Airflow DAGs and PythonOperator
- Working with Dockerized services
- Connecting Python to PostgreSQL
- Persisting DataFrames with SQLAlchemy
- Validating data before persistence
- Debugging container paths, environment variables, and database connections
- Designing a more realistic data engineering workflow

---

## 🚀 Future Improvements

- Add scheduled execution (`@daily`)
- Add more robust logging per task
- Add unit tests for transformation and validation functions
- Implement incremental loads instead of full table replacement
- Add a separate PostgreSQL service dedicated only to project data
- Add monitoring or alerting for failed DAG runs
- Move raw data ingestion to an external source such as an API or cloud storage

---

## 📄 Portfolio / Interview Notes

This project demonstrates:

- End-to-end data pipeline design
- Workflow orchestration with Apache Airflow
- Docker-based local development
- PostgreSQL integration
- Data quality validation
- Separation of pipeline logic into extract, transform, validate, and load layers
- A realistic approach to data engineering project structure

Designed as a portfolio project to practice and demonstrate junior data engineering skills.
