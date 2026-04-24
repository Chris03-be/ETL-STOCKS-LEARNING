# 🏗️ Architecture - Undervalued Stocks Analyzer

## System Overview

```
┌─────────────────────────────────────────────────────────────────┐
│           UNDERVALUED STOCKS ANALYZER ARCHITECTURE              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│ LAYER 1: INGESTION (APIs + DLT)                                 │
│ ├─ Yahoo Finance API (OHLCV data)                              │
│ ├─ Alpha Vantage (Technical indicators)                        │
│ ├─ IEX Cloud (Fundamentals)                                    │
│ └─ DLT (Automated loading)                                     │
│      └─ Fetch → Validate → Load Bronze                        │
│                                                                 │
│        10 Companies: INTC, CI, F, ADBE, MO, SIE, NESTE,       │
│                     CMCSA, GDMK, 601318.SS                    │
│                                                                 │
│              ↓                                                  │
│                                                                 │
│ LAYER 2: STORAGE (PostgreSQL)                                   │
│                                                                 │
│ BRONZE SCHEMA                                                   │
│ ├─ raw_stock_prices              (OHLCV data, 365+ days)      │
│ ├─ raw_fundamentals              (P/E, Dividend, etc)         │
│ ├─ raw_technical_indicators      (RSI, MACD, etc)            │
│ └─ raw_company_metrics           (ROE, ROA, Margins, etc)     │
│                                                                 │
│              ↓                                                  │
│                                                                 │
│ SILVER SCHEMA                                                   │
│ ├─ stg_stock_prices_clean        (Deduplicated, validated)    │
│ ├─ stg_fundamentals_clean        (Standardized)               │
│ ├─ stg_technical_indicators_clean (Computed)                 │
│ └─ stg_metrics_clean             (Normalized)                 │
│                                                                 │
│              ↓                                                  │
│                                                                 │
│ GOLD SCHEMA (Analytics)                                         │
│ ├─ fct_valuation_metrics         (P/E, P/B, PEG, Div Yield) │
│ ├─ fct_technical_analysis        (RSI, MACD, Bollinger, MA)  │
│ ├─ fct_undervaluation_scoring    (Score 0-100, Grade A-F)   │
│ ├─ fct_peer_comparison           (Industry benchmarks)        │
│ └─ dim_companies                 (10 companies master)         │
│                                                                 │
│              ↓                                                  │
│                                                                 │
│ LAYER 3: TRANSFORMATION (DBT)                                   │
│ ├─ SQL models for each layer                                   │
│ ├─ Data quality tests                                          │
│ ├─ Documentation & lineage                                     │
│ └─ Materialization: views & tables                            │
│                                                                 │
│              ↓                                                  │
│                                                                 │
│ LAYER 4: VISUALIZATION (Power BI)                              │
│ ├─ Executive Dashboard           (Top opportunities)           │
│ ├─ Valuation Analysis            (P/E, P/B, Div, FCF)        │
│ ├─ Technical Analysis            (Charts, indicators)          │
│ ├─ Undervaluation Scoring        (Rankings, grades)           │
│ └─ Peer Comparison               (Heatmaps, comparisons)      │
│                                                                 │
│              ↓                                                  │
│                                                                 │
│ LAYER 5: ORCHESTRATION (APScheduler)                           │
│ ├─ Daily 4 PM UTC trigger                                      │
│ ├─ Sequential task execution:                                  │
│ │   1. DLT ingestion                                          │
│ │   2. DBT transformations                                    │
│ │   3. Data quality tests                                     │
│ │   4. Error handling & logging                               │
│ └─ Auto-refresh Power BI                                       │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Data Flow

```
┌──────────────────┐
│  APIs (Daily)    │
│  Yahoo Finance   │
│  Alpha Vantage   │
│  IEX Cloud       │
└────────┬─────────┘
         │
         ↓
┌──────────────────────┐
│  DLT Pipeline        │
│  ├─ Fetch data       │
│  ├─ Validate schema  │
│  ├─ Standardize      │
│  └─ Load Bronze      │
└────────┬─────────────┘
         │
         ↓
┌──────────────────────┐
│  PostgreSQL Bronze   │
│  ├─ raw_prices       │
│  ├─ raw_fundamentals │
│  ├─ raw_technical    │
│  └─ raw_metrics      │
└────────┬─────────────┘
         │
         ↓ (DBT: dbt run)
┌──────────────────────┐
│  PostgreSQL Silver   │
│  ├─ stg_prices_clean │
│  ├─ stg_fund_clean   │
│  ├─ stg_tech_clean   │
│  └─ stg_metrics_clean│
└────────┬─────────────┘
         │
         ↓ (DBT: fct_* models)
┌──────────────────────┐
│  PostgreSQL Gold     │
│  ├─ fct_valuation    │
│  ├─ fct_technical    │
│  ├─ fct_scoring      │
│  ├─ fct_peer         │
│  └─ dim_companies    │
└────────┬─────────────┘
         │
         ↓ (DBT: dbt test)
┌──────────────────────┐
│  Data Quality Tests  │
│  ├─ Relationships    │
│  ├─ Not null checks  │
│  ├─ Value ranges     │
│  └─ Custom tests     │
└────────┬─────────────┘
         │
         ↓
┌──────────────────────┐
│  Power BI            │
│  ├─ Dashboards       │
│  ├─ Reports          │
│  ├─ KPI Cards        │
│  └─ Visualizations   │
└──────────────────────┘
```

---

## Component Details

### 1. Ingestion Layer (src/ingestion/)

**Files:**
- `dlt_pipeline.py` - Main DLT pipeline orchestrator
- `fetchers.py` - API client implementations
- `validators.py` - Data validation logic

**Responsibilities:**
- Fetch data from multiple APIs
- Validate data integrity
- Handle API errors & retries
- Load to Bronze layer
- Maintain state for incremental loads

**Flow:**
```python
for ticker in [INTC, CI, F, ...]:
    price_data = yfinance.Ticker(ticker).history()
    fundamental_data = alpha_vantage.get_fundamentals(ticker)
    validate(price_data, fundamental_data)
    dlt_load_to_bronze(price_data, fundamental_data)
```

---

### 2. Storage Layer (PostgreSQL)

**Database Structure:**

```sql
-- Bronze (Raw)
CREATE SCHEMA bronze;

CREATE TABLE bronze.raw_stock_prices (
    ticker VARCHAR(10),
    date DATE,
    open DECIMAL,
    high DECIMAL,
    low DECIMAL,
    close DECIMAL,
    volume BIGINT,
    adj_close DECIMAL,
    fetched_at TIMESTAMP
);

CREATE TABLE bronze.raw_fundamentals (
    ticker VARCHAR(10),
    pe_ratio DECIMAL,
    pb_ratio DECIMAL,
    dividend_yield DECIMAL,
    peg_ratio DECIMAL,
    roe DECIMAL,
    roa DECIMAL,
    fetched_at TIMESTAMP
);

-- Silver (Cleaned)
CREATE SCHEMA silver;

CREATE TABLE silver.stg_stock_prices_clean AS
SELECT DISTINCT * FROM bronze.raw_stock_prices;

-- Gold (Analytics)
CREATE SCHEMA gold;

CREATE TABLE gold.fct_valuation_metrics (
    ticker VARCHAR(10),
    date DATE,
    pe_ratio DECIMAL,
    pb_ratio DECIMAL,
    peg_ratio DECIMAL,
    dividend_yield DECIMAL,
    fcf_yield DECIMAL,
    price_to_sales DECIMAL,
    created_at TIMESTAMP
);

CREATE TABLE gold.fct_undervaluation_scoring (
    ticker VARCHAR(10),
    date DATE,
    composite_score INT,
    grade VARCHAR(1),
    pe_score INT,
    peg_score INT,
    dividend_score INT,
    technical_score INT,
    undervaluation_pct DECIMAL,
    recommendation VARCHAR(20),
    created_at TIMESTAMP
);
```

---

### 3. Transformation Layer (DBT)

**Model Structure:**

```
dbt_project/models/
├── bronze/
│   └── stg_api_raw.sql                (Just rename columns)
│
├── silver/
│   ├── stg_stock_prices.sql           (Deduplicate, validate)
│   ├── stg_fundamentals.sql
│   ├── stg_technical_indicators.sql
│   └── stg_company_metrics.sql
│
└── gold/
    ├── fct_valuation_metrics.sql      (P/E, P/B, PEG, etc)
    ├── fct_technical_analysis.sql     (RSI, MACD, MAs)
    ├── fct_undervaluation_scoring.sql (Composite 0-100)
    ├── fct_peer_comparison.sql        (Industry benchmarks)
    └── dim_companies.sql              (Master dimension)
```

**Example Model: Undervaluation Scoring**

```sql
-- fct_undervaluation_scoring.sql

with valuation as (
    select
        ticker,
        date,
        pe_ratio,
        peg_ratio,
        dividend_yield,
        intrinsic_value,
        current_price
    from {{ ref('stg_fundamentals') }}
),

technical as (
    select
        ticker,
        date,
        rsi
    from {{ ref('stg_technical_indicators') }}
),

scoring as (
    select
        v.ticker,
        v.date,
        v.current_price,
        v.intrinsic_value,
        
        -- Composite Score (0-100)
        (
            case when v.pe_ratio < 10 then 25 else 0 end +
            case when v.peg_ratio < 1 then 25 else 0 end +
            case when v.dividend_yield > 2 then 25 else 0 end +
            case when t.rsi < 30 then 25 else 0 end
        ) as composite_score
        
    from valuation v
    left join technical t
        on v.ticker = t.ticker
        and v.date = t.date
)

select
    *,
    case
        when composite_score >= 80 then 'A'
        when composite_score >= 60 then 'B'
        when composite_score >= 40 then 'C'
        when composite_score >= 20 then 'D'
        else 'F'
    end as grade,
    case
        when composite_score >= 80 then 'STRONG BUY'
        when composite_score >= 60 then 'BUY'
        when composite_score >= 40 then 'HOLD'
        when composite_score >= 20 then 'SELL'
        else 'STRONG SELL'
    end as recommendation
from scoring
```

---

### 4. Visualization Layer (Power BI)

**Dashboards:**

1. **Executive Summary**
   - KPI cards (Top 3 undervalued, Top 3 overvalued)
   - Key metrics overview
   - 10-company ranking

2. **Valuation Analysis**
   - P/E ratio comparison chart
   - Dividend yield matrix
   - P/B and PEG rankings
   - FCF analysis

3. **Technical Analysis**
   - Price charts (1-year)
   - RSI indicators
   - MACD charts
   - Bollinger Bands

4. **Undervaluation Scoring**
   - Score distribution
   - Grade breakdown
   - Recommendations
   - Risk ratings

5. **Peer Comparison**
   - Sector benchmarks
   - Heatmap (P/E by sector)
   - Relative valuation
   - Individual stock details

---

### 5. Orchestration Layer (APScheduler)

**Schedule:**

```python
# Daily at 4 PM UTC (after market close)
@scheduler.scheduled_job('cron', hour=16, minute=0)
def daily_pipeline():
    # 1. Run DLT (fetch + load Bronze)
    subprocess.run(['python', 'src/ingestion/dlt_pipeline.py'])
    
    # 2. Run DBT (transform)
    subprocess.run(['dbt', 'run', '--project-dir', 'src/transformation/dbt_project'])
    
    # 3. Run tests (validate)
    subprocess.run(['dbt', 'test', '--project-dir', 'src/transformation/dbt_project'])
    
    # 4. Log results
    log_pipeline_status()
    
    # 5. Auto-refresh Power BI (if cloud)
    # (For desktop version, manual refresh)
```

---

## Technology Stack

```
INGESTION
├─ Python 3.8+
├─ yfinance (Yahoo Finance API)
├─ Alpha Vantage (Technical data)
├─ dlt[postgres] (Data Load Tool)
└─ requests (HTTP client)

STORAGE
├─ PostgreSQL 13+
├─ psycopg2 (Python connector)
└─ SQLAlchemy (ORM)

TRANSFORMATION
├─ DBT Core 1.7+
├─ DBT Postgres adapter
└─ Jinja2 (templating)

VISUALIZATION
├─ Power BI Desktop (free)
└─ DAX (calculations)

ORCHESTRATION
├─ APScheduler (Python)
└─ Logging (Python)

TESTING
└─ pytest
```

---

## Data Quality Assurance

**Bronze Layer:**
- Schema validation on ingestion
- Type checking
- Null validation
- API error handling

**Silver Layer:**
- Deduplication
- Range validation (prices > 0)
- Date validation
- Standardization

**Gold Layer:**
- Referential integrity (FK checks)
- Calculated field validation
- Range checks (scores 0-100)
- Business rule validation

**DBT Tests:**
```yaml
# models/gold/schema.yml

models:
  - name: fct_undervaluation_scoring
    columns:
      - name: composite_score
        tests:
          - not_null
          - accepted_values:
              values: [0, 10, 20, ..., 100]
          
      - name: grade
        tests:
          - not_null
          - accepted_values:
              values: ['A', 'B', 'C', 'D', 'F']
```

---

## Performance Considerations

**Indexes:**
```sql
-- Fast lookups by ticker and date
CREATE INDEX idx_bronze_prices_ticker_date 
ON bronze.raw_stock_prices(ticker, date);

-- Fast aggregations
CREATE INDEX idx_gold_scoring_composite
ON gold.fct_undervaluation_scoring(composite_score DESC);
```

**Materialization:**
- Bronze: Views (raw data, no aggregation)
- Silver: Tables (cleaned, deduplicated)
- Gold: Tables (aggregated, indexed)

---

## Monitoring & Logging

**Scheduler Logs:**
```
/data/logs/scheduler_2026_04_24.log

[2026-04-24 16:00:00] Starting daily pipeline
[2026-04-24 16:00:05] DLT: Fetching 10 companies
[2026-04-24 16:00:30] DLT: Loaded 5,240 rows to Bronze
[2026-04-24 16:00:35] DBT: Running transformations
[2026-04-24 16:00:45] DBT: 15 models completed
[2026-04-24 16:00:50] Tests: 42 tests passed
[2026-04-24 16:00:55] Pipeline completed successfully ✅
```

---

## Scalability Path

**Current (10 companies):**
- ~5,000 rows/day
- PostgreSQL single-server
- Power BI Desktop

**Future (100+ companies):**
- Add data warehouse (Snowflake, BigQuery)
- Implement cloud dbt (dbt Cloud)
- Migrate to Power BI Premium
- Add real-time streaming (Kafka)
- Add ML models (undervaluation prediction)

---

**Architecture is production-ready and scalable! 🚀**