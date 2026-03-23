

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

## Key Findings

1. **Quarterly Trends** — Productivity grew from ~8.4% to ~10.4%
   and task automation rose from ~17% to ~22% between 2023–2026

2. **Industry Analysis** — Logistics leads AI ROI (~1.3) while
   Technology, despite highest productivity gains, shows lowest
   ROI (~0.9) due to diminishing returns

3. **Adoption Stage Segmentation** — Transforming companies achieve
   6–8× higher productivity gains than Exploring companies;
   Scaling stage drives the strongest net job creation

4. **Workforce Impact** — Enterprise companies drive almost all
   net job creation; higher automation correlates with job growth,
   not destruction

5. **Regional Analysis** — Oceania and North America lead in ROI;
   Asia dominates net job creation due to scale

6. **AI Maturity vs Outcomes** — Maturity is the strongest
   predictor of productivity (r = 0.73) and revenue growth
   (r = 0.41); employment effects remain near-neutral

7. **Strategic Insight** — More concurrent AI projects show
   short-term drag on performance; quality of implementation
   matters more than quantity

The full exploratory data analysis notebook is available here:
[📈 View EDA Analysis Notebook](notebooks/eda_analysis1.ipynb)

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

📄 **[View dbt Test Results](docs/dbt_test_results.txt)**


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

[🔬 View Notebook](notebooks/data_quality_testing.ipynb)

The full data quality report is availabe here: 📋 **[View Full Data Quality Report](docs/data_quality_summary.md)**

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