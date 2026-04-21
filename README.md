# E-Commerce ETL Pipeline

**Author:** Chereddy Prasantha Reddy  
**Stack:** Apache Airflow | AWS S3 | AWS Glue | AWS Athena | Python | PySpark | Docker

---

## Project Overview

An end-to-end cloud data engineering pipeline that generates realistic Indian e-commerce data, orchestrates the full ETL workflow using Apache Airflow, cleans and transforms data with AWS Glue, and enables analytics via AWS Athena.

The pipeline generates **100,000 order items** across 5 relational tables with **~3% intentional dirty data** to demonstrate real-world data cleaning capabilities.

---

## Pipeline Architecture

```
[Airflow DAG]
    Task 1: data_generator.py  -->  generates 5 CSV tables  -->  saves to S3 raw/
    Task 2: Glue Crawler       -->  scans raw/  -->  registers tables in Glue catalog
    Task 3: Glue ETL Job       -->  cleans data  -->  writes Parquet to S3 clean/
    Task 4: Glue Crawler       -->  scans clean/  -->  updates Glue catalog
    Task 5: Athena Query       -->  runs revenue analytics SQL  -->  results to S3
```

---

## Dataset

| Table       | Rows    | Description                              |
|-------------|---------|------------------------------------------|
| categories  | 7       | Static product categories (Electronics, Clothing, etc.) |
| products    | 49      | 7 products per category with Indian market prices |
| customers   | 14,285  | Indian locale -- names, cities, phone numbers |
| orders      | 50,000  | Linked to customers, weighted status distribution |
| order_items | 100,000 | Core fact table -- ~3% intentional dirty data |

### Dirty Data Injected (for Glue cleaning demo)
- **Invalid FK** -- product_id 9999 does not exist in products table
- **Negative quantity** -- physically impossible values
- **Future order dates** -- orders placed 10 days ahead

---

## Tech Stack

| Layer         | Technology                        |
|---------------|-----------------------------------|
| Orchestration | Apache Airflow 2.9.0 (Docker)     |
| Data Generation | Python, Faker, Pandas           |
| Cloud Storage | AWS S3                            |
| Cataloging    | AWS Glue Crawler                  |
| Transformation| AWS Glue ETL Job (PySpark)        |
| Analytics     | AWS Athena (serverless SQL)       |
| Infrastructure| Docker, docker-compose            |

---

## Folder Structure

```
ecommerce-etl-pipeline/
|-- dags/
|   |-- data_generator.py        # Generates all 5 CSV tables
|   |-- ecommerce_pipeline.py    # Airflow DAG -- full pipeline
|   `-- output/                  # CSVs appear here after DAG runs
|-- logs/                        # Airflow task logs (auto-generated)
|-- plugins/                     # Empty (required by Airflow)
|-- docs/
|   |-- data_generator_explanation.pdf
|   |-- dag_explanation.pdf
|   |-- airflow_setup_guide.pdf
|   `-- error_resolution.pdf
|-- docker-compose.yml           # Airflow services configuration
|-- requirements.txt             # Python package versions
`-- README.md
```

---

## How to Run

### Prerequisites
- Docker Desktop installed and running (4GB+ RAM allocated)
- AWS account with S3, Glue, Athena access
- S3 bucket created (update BUCKET constant in ecommerce_pipeline.py)
- Glue Crawlers and Glue ETL Job created in AWS console

### Step 1 -- Start Airflow

```bash
# First time only -- initialise database
docker compose up airflow-init

# Start all services
docker compose up -d

# Check all containers are running
docker compose ps
```

### Step 2 -- Open Airflow UI

```
URL:      http://localhost:8080
Username: airflow
Password: airflow
```

### Step 3 -- Add AWS Connection

```
Admin -> Connections -> + (Add)
Connection Id:   aws_default
Connection Type: Amazon Web Services
AWS Access Key ID:     your_access_key
AWS Secret Access Key: your_secret_key
Extra:           {"region_name": "ap-south-1"}
```

### Step 4 -- Trigger the Pipeline

1. Find **ecommerce_etl_pipeline** in the DAGs list
2. Toggle it from paused to active
3. Click the Trigger button
4. Monitor each task in the Graph view

---

## Key Design Decisions

| Decision | Reason |
|----------|--------|
| Faker with en_IN locale | Realistic Indian market data |
| No random seed | Fresh data every pipeline run |
| Weighted order status (70% completed) | Mimics real e-commerce distribution |
| ~3% dirty data injection | Tests Glue cleaning logic end-to-end |
| created_at on every table | Enables future incremental load via watermark |
| Operator + Sensor pattern | Separate trigger and wait for clear debugging |
| mode=reschedule on sensors | Releases worker between polls -- efficient |
| XCom for inter-task handoff | DAG 1 output path -> DAG 2 upload |

---

## Analytics Query (Athena)

```sql
SELECT
    c.category_name,
    COUNT(DISTINCT o.order_id)                  AS total_orders,
    SUM(oi.quantity)                            AS total_units_sold,
    ROUND(SUM(oi.final_price * oi.quantity), 2) AS total_revenue_inr
FROM order_items oi
JOIN orders     o ON oi.order_id  = o.order_id
JOIN products   p ON oi.product_id = p.product_id
JOIN categories c ON p.category_id = c.category_id
WHERE o.order_status = 'completed'
GROUP BY c.category_name
ORDER BY total_revenue_inr DESC
```

---

## Skills Demonstrated

- **Python** -- OOP, type hints, logging, error handling, data generation
- **Apache Airflow** -- DAG design, PythonOperator, GlueOperator, Sensors, XCom
- **AWS S3** -- bucket structure, prefix design, programmatic upload via S3Hook
- **AWS Glue** -- crawler configuration, ETL job with PySpark, Data Catalog
- **AWS Athena** -- serverless SQL, Glue catalog integration
- **Docker** -- multi-service compose, volume mounting, environment variables
- **Data Engineering** -- dirty data patterns, schema design, pipeline orchestration

---

## Target Role

Systems Engineer transitioning to **Data Engineer**  
Target: 10-14 LPA | Companies: Mu Sigma, Fractal Analytics, LTIMindtree, BMW TechWorks India
