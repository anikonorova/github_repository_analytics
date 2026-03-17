# GitHub Repository Analytics

Take-home assessment for the Senior Analytics Engineer role.

End-to-end pipeline that extracts GitHub activity from `pandas-dev/pandas`, transforms it with dbt, and surfaces three engineering KPIs. The pipeline is repo-agnostic - swap `REPO_OWNER` and `REPO_NAME` in `extraction/config.py` to point it at any public GitHub repo.

**[Full project summary, KPI results, and design decisions → `analysis/summary.ipynb`](https://github.com/anikonorova/github_repository_analytics/blob/main/analysis/summary.ipynb)**

---

## Tech Stack

| Component | Tool |
|-----------|------|
| Database | DuckDB |
| Transformation | dbt-core + dbt-duckdb |
| Extraction | Python · GitHub REST API |
| Analysis | Jupyter · pandas · seaborn |

---

## Quick Start

**1. Install dependencies**
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

**2. Set up environment**
```bash
cp .env.example .env
# add your GITHUB_TOKEN https://github.com/settings/tokens
```

**3. Initialise the database**
```bash
python database/init_db.py
```
Creates `database/analytics.db` and the `raw_data` schema with tables.

**4. Configure dbt** - run `dbt init` to create `~/.dbt/profiles.yml`:
```bash
cd dbt_project
dbt init          # creates ~/.dbt/profiles.yml interactively
```


Open the file `~/.dbt/profiles.yml`. 
Copy and paste the configuration below into this file, and update the path to match the absolute path to your newly created `database/analytics.db`.
```yaml
dbt_project:
  outputs:
    dev:
      type: duckdb
      path: /absolute/path/to/database/analytics.db
      schema: main
      threads: 4
  target: dev
```

**5. (Optional) Adjust extraction window**

> Edit `extraction/config.py` to change the target repo or data extraction window.  
> The default value is `SINCE_DATE = "2025-01-01T00:00:00Z"`; full extraction takes over an hour.   
> Set it to a more recent date (e.g. a few days ago) for a quick test run.  

**6. Run the pipeline**
```bash
python run_pipeline.py   # extract & load raw data into DuckDB

cd dbt_project
dbt run                  # build all models
```

**7. Open the notebook**
```bash
jupyter notebook analysis/summary_presentation.ipynb
```

---

## Data Model

| Layer | Model | Grain |
|-------|-------|-------|
| Staging | `stg_pull_requests` | one row per PR |
| Staging | `stg_reviews` | one row per review |
| Staging | `stg_issues` | one row per issue |
| Staging | `stg_commits` | one row per commit |
| Intermediate | `int_github__contributors_spine` | one row per contributor |
| Mart | `fct_github__pull_requests` | one row per PR |
| Mart | `fct_github__issues` | one row per issue |
| Mart | `dim_github__contributors` | one row per contributor |
| KPI | `kpi_cycle_time` | month × pr_size |
| KPI | `kpi_time_to_first_review` | month |
| KPI | `kpi_bug_resolution_time` | quarter |
| KPI | `kpi_bug_resolution_buckets` | week × resolution bucket |

Staging models are views. Mart and KPI models are materialized as tables.

---

## KPIs

| KPI | Definition | Grain |
|-----|-----------|-------|
| **PR Cycle Time** | Median hours from PR opened → merged | month × pr_size |
| **Time to First Review** | Median hours from PR opened → first human review | month |
| **Bug Resolution Time** | Median hours from bug issue opened → closed | quarter |

For full KPI definitions, exclusion rules, threshold choices, and results see the [summary notebook](https://github.com/anikonorova/github_repository_analytics/blob/main/analysis/summary.ipynb).
