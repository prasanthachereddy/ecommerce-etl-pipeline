# E-Commerce ETL Pipeline — AWS Data Engineering Project

End-to-end ETL pipeline processing 85,000+ synthetic e-commerce records  
through AWS Glue, S3, and Athena — built as a portfolio project.

---

## Architecture
```mermaid
flowchart LR
    A[Python Generator] --> B[Raw CSV Files]
    B --> C[AWS S3 raw/]
    C --> D[Glue Crawler]
    D --> E[Data Catalog]
    E --> F[PySpark Glue Job]
    F --> G[S3 processed/ Parquet]
    G --> H[Glue Crawler]
    H --> I[AWS Athena]
    I --> J[SQL Analytics]
```

---

## Problem Statement

Built a production-style data pipeline to simulate real-world ETL challenges:
- Raw messy data across 5 related tables
- Intentional dirty data injected (~3% error rows)
- PySpark cleaning job to fix and validate
- SQL analytics on clean processed data

---

## Tech Stack

| Tool | Purpose |
|---|---|
| Python + Faker | Synthetic e-commerce data generation |
| AWS S3 | Raw and processed data storage |
| AWS Glue Crawler | Schema inference + Data Catalog |
| PySpark (AWS Glue) | Data cleaning and transformation |
| AWS Athena | SQL analytics on Parquet files |
| Git + GitHub | Version control and portfolio |

---

## Dataset

| Table | Raw Rows | Clean Rows | Removed |
|---|---|---|---|
| categories | 7 | 7 | 0 |
| products | 49 | 49 | 0 |
| customers | 21,428 | 21,419 | 9 |
| orders | 37,500 | 36,694 | 806 |
| order_items | 85,478 | 83,729 | 1,749 |

---

## Dirty Data Cleaned

| Issue | Count | Fix Applied |
|---|---|---|
| Future order dates | 806 | Removed |
| Invalid product FK (id=9999) | 856 | Removed |
| Negative quantity | 893 | Removed |
| final_price miscalculated | 893 | Recomputed |

---

## Pipeline Steps

1. Generate 85,000+ synthetic records with Indian locale (Faker en_IN)
2. Inject ~3% error rows — invalid FK, negative qty, future dates
3. Upload raw CSVs to S3 `raw/` using AWS CLI
4. Run Glue Crawler on `raw/` to infer schema and update Data Catalog
5. Run PySpark Glue job to clean all 5 tables
6. Write clean data as Parquet (snappy compressed) to S3 `processed/`
7. Run Glue Crawler on `processed/` to update Data Catalog
8. Query clean data in AWS Athena for business insights

---

## Key Insights from Athena

- Top revenue category: **Electronics**
- Most used payment method: **Card**
- Pipeline removed **2,555 dirty rows** — 3% of total dataset
- Clean order items: **83,729** ready for analytics

---

## Project Structure
```
ecommerce-etl-pipeline/
│
├── README.md
├── requirements.txt
├── .gitignore
│
├── scripts/
│   ├── data_generator.py          ← generates synthetic data
│   └── glue_clean_ecommerce.py    ← PySpark Glue cleaning job
│
├── sql/
│   └── analytics_queries.sql      ← Athena analytics queries
│
└── docs/
    └── screenshots/               ← Athena query result screenshots
```

---

## Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Generate data
python scripts/data_generator.py

# Sync to S3
aws s3 sync data/raw/ s3://your-bucket/raw/

# Then run Glue Crawler → Glue Job → Glue Crawler → Athena
```

---

## Author

**Chereddy Prasantha Reddy**  
Systems Engineer @ TCS | Targeting Data Engineer roles  
📍 Bangalore, India  
📧 prasanthareddy94@gmail.com
