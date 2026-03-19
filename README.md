

# AI Adoption & Workforce Impact — Data Pipeline

A end-to-end data engineering project that ingests, transforms,
tests, and analyzes global AI adoption data across industries and countries.

---
## Presentation

The executive presentation slides are available here:
[📊 View Presentation](docs/presentation/AI_Adoption_Pipeline_Presentation.pdf)

---
## Architecture Overview
```
CSV Files (Kaggle)
      │
      ▼
Python Ingestion Script
      │
      ▼
BigQuery (ai_adoption_raw)
      │
      ▼
dbt Staging Models (views)
      │
      ▼
dbt Mart Models (star schema tables)
      │
      ├── Data Quality Tests
      │     ├── dbt tests (schema.yml)
      │     └── Custom SQL + Great Expectations (notebook)
      │
      ▼
EDA Analysis (Jupyter Notebook)
```

---

## Project Structure
```
ai-adoption-pipeline/
├── data/                        # CSVs not included (see download instructions)
│   └── README.md
├── ingestion/
│   └── load_to_bigquery.py      # CSV → BigQuery raw tables
├── dbt_project/
│   ├── models/
│   │   ├── staging/             # Raw → cleaned views
│   │   └── marts/               # Star schema tables
│   ├── dbt_project.yml
│   └── packages.yml
├── notebooks/
│   ├── data_quality_testing.ipynb
│   └── eda_analysis.ipynb
├── docs/
│   ├── architecture_diagram.png
│   └── data_lineage.md
├── .env.example
├── requirements.txt
└── README.md
```

---

## Dataset

**Global AI Adoption & Workforce Impact Dataset**
Source: [Kaggle](https://www.kaggle.com/datasets/mohankrishnathalla/global-ai-adoption-and-workforce-impact-dataset?resource=download)

| File | Description | Rows |
|------|-------------|------|
| ai_company_adoption.csv | Company-level AI adoption metrics | ~100,000 |
| ai_industry_summary.csv | Industry-level aggregated summary | ~20 |
| country_ai_index.csv | Country-level AI readiness index | ~50 |

> CSV files are not committed to Git due to file size.
> See `data/README.md` for download instructions.

---

## Tech Stack

| Layer | Tool | Reason |
|-------|------|--------|
| Data Warehouse | BigQuery | Scalable, serverless, SQL-native |
| Transformation | dbt | Version-controlled, testable SQL models |
| Ingestion | Python + pandas | Flexible, handles chunked uploads |
| Data Quality | dbt tests + Custom SQL + Great Expectations | Multi-layer validation |
| Analysis | Jupyter + pandas + matplotlib | Accessible, reproducible |
| Version Control | Git + GitHub | Collaboration and code history |

---

## Star Schema Design
```
                    DimDate
                       │
DimCompany ──── FactAISurvey ──── DimAITool
                       │
              ┌────────┼────────┐
              │        │        │
        DimAIUseCase  DimAI    DimSurvey
                    Adoption    Source
                     Stage
```

### Why Star Schema?
- Simple structure that supports fast analytical queries
- Easy to understand for BI analysts and business stakeholders
- Dimension tables can be reused across multiple fact tables
- Optimized for GROUP BY and aggregation queries in BigQuery

### Derived Columns (dbt)
| Column | Table | Formula |
|--------|-------|---------|
| net_jobs_change | fact_ai_survey | jobs_created - jobs_displaced |
| ai_roi_index | fact_ai_survey | (revenue_growth + cost_reduction) / ai_budget_pct |
| year_quarter | dim_date | concat(survey_year, '-', quarter) |
| is_latest_year | dim_date | survey_year = current year |
| stage_order | dim_ai_adoption_stage | mapped from raw stage name |
| stage_label | dim_ai_adoption_stage | formatted stage description |
| tool_vendor_category | dim_ai_tool | grouped from ai_primary_tool |
| usecase_category | dim_ai_usecase | grouped from ai_use_case |

---

## Setup Instructions

### 1. Clone the repository
```bash
git clone https://github.com/YOUR_USERNAME/ai-adoption-pipeline.git
cd ai-adoption-pipeline
```

### 2. Create virtual environment
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Configure environment variables
```bash
cp .env.example .env
# Edit .env with your actual values
```

### 4. Download data
```
Follow instructions in data/README.md
Place CSV files in the data/ folder
```

### 5. Run ingestion
```bash
python ingestion/load_to_bigquery.py
```

### 6. Run dbt models
```bash
cd dbt_project
dbt deps
dbt run
dbt test
```

### 7. Open notebooks
```bash
cd ..
jupyter notebook
```

---

## Data Quality Testing

### dbt Tests (schema.yml)
- `unique` and `not_null` on all primary keys
- `relationships` for all foreign keys in fact table
- `accepted_values` for categorical fields

### Custom SQL Tests (notebook)
| Category | Tests |
|----------|-------|
| Null checks | 6 tests across all tables |
| Duplicate checks | 5 tests on all PKs |
| Referential integrity | 6 FK tests |
| Business logic | 8 value range tests |

### Great Expectations
- survey_key not null
- task_automation_rate between 0 and 100
- jobs_displaced >= 0

---

## Key Findings

1. **Quarterly Trends** — Productivity and revenue growth
   trend upward as AI adoption matures over time
2. **Top Industries** — Industries with highest AI ROI
   show significantly higher automation rates
3. **Adoption Stage Segmentation** — Companies in later
   stages show higher ROI and positive net jobs change
4. **Workforce Impact** — Large enterprises displace more
   jobs but also create more; SMEs show better net ratio
5. **Regional Analysis** — Regional digital maturity
   correlates strongly with AI ROI index

---

## Environment Variables

Create a `.env` file based on `.env.example`:
```
GOOGLE_APPLICATION_CREDENTIALS=service_account.json
BQ_PROJECT_ID=your-project-id
BQ_RAW_DATASET=ai_adoption_raw
BQ_DW_DATASET=ai_adoption_dw
DATA_DIR=data
```

> Never commit `.env` or `service_account.json` to Git.