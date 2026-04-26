# 🚀 ETL-STOCKS-PREDICT - Complete Architecture

## Overview

A production-ready **ETL + Machine Learning pipeline** for analyzing undervalued stocks (US & Europe) with automated daily execution.

```
Yahoo Finance API
        ↓
   DLT Pipeline (Python)
        ↓
PostgreSQL Bronze Layer (Raw)
        ↓
   DBT Transformation
        ↓
PostgreSQL Silver Layer (Cleaned)
        ↓
PostgreSQL Gold Layer (Analytics)
        ↓
  PyCaret ML Training
        ↓
Price Predictions (7-day)
        ↓
  APScheduler Automation
        ↓
  Power BI Dashboards
```

---

## 📊 Database Schema

### Bronze Layer (Raw Data)
```
bronze.raw_stock_prices
├─ ticker, date, open, high, low, close, volume, adj_close

bronze.raw_fundamentals
├─ ticker, date, pe_ratio, pb_ratio, dividend_yield, market_cap, eps

bronze.raw_metadata
├─ ticker, company_name, sector, country, exchange
```

### Silver Layer (Cleaned Data)
```
silver.stg_stock_prices_clean
├─ Deduplicated, validated prices

silver.stg_fundamentals_clean
├─ Validated fundamental metrics
```

### Gold Layer (Analytics)
```
gold.fct_market_analysis ⭐
├─ ticker, date, close_price, volume
├─ ma_50, ma_200                      (Technical)
├─ volatility_30d, rsi_14             (Risk)
├─ pe_ratio, pb_ratio, dividend_yield (Valuation)
├─ is_undervalued, undervaluation_score

gold.ai_forecast ⭐
├─ ticker, forecast_date, actual_date
├─ predicted_price, predicted_direction
├─ prediction_confidence, prediction_error
├─ model_version, training_date

gold.dim_companies
├─ Master dimension table

gold.data_quality_checks
├─ Validation results and metrics

gold.pipeline_logs
├─ Execution logs and monitoring
```

---

## 🔄 Pipeline Stages

### Stage 1: Ingestion (DLT)
**File**: `src/ingestion/dlt_pipeline.py`

✅ Fetches historical prices from Yahoo Finance  
✅ Retrieves fundamentals (P/E, Dividend, etc)  
✅ Loads to PostgreSQL Bronze layer  
✅ Handles retries and errors gracefully  

**Output**: 
- 365+ days of OHLCV data
- Fundamental metrics

### Stage 2: Validation
**File**: `src/ingestion/validators.py`

✅ Schema validation (columns & types)  
✅ Not null checks  
✅ Range validation (price > 0, volume ≥ 0)  
✅ Uniqueness checks (no duplicates)  
✅ Outlier detection (Z-Score)  
✅ Data freshness checks  
✅ Gap detection (trading days)  

### Stage 3: Transformation (DBT)
**Directory**: `src/transformation/dbt_project/`

✅ Bronze → Silver: Clean & deduplicate  
✅ Silver → Gold: Calculate metrics  
- 50-day moving average
- 200-day moving average
- 30-day volatility
- 14-day RSI
- Undervaluation scoring (0-100)

✅ Data quality tests  
✅ Generate documentation  

### Stage 4: Machine Learning (PyCaret)
**File**: `src/ml_layer/predictive_model.py`

✅ Feature engineering (lag, momentum, technical)  
✅ Automated model selection (RF, XGBoost, GBR, LR)  
✅ 7-day price prediction  
✅ Confidence scoring  
✅ Save predictions to PostgreSQL  
✅ MLflow experiment tracking  

**Output**:
- Predicted prices for 7 days ahead
- Direction predictions (UP/DOWN/NEUTRAL)
- Confidence scores (0-100)
- Model metrics (RMSE, MAE, R²)

### Stage 5: Orchestration (APScheduler)
**File**: `src/orchestration/scheduler.py`

✅ Daily execution at 22:00 UTC  
✅ Sequential pipeline execution  
✅ Error handling and logging  
✅ Database logging  
✅ Manual or scheduled mode  

---

## 🎯 Key Features

### Data Quality
- ✅ 7-level validation suite
- ✅ Quality metrics in database
- ✅ Automated anomaly detection
- ✅ Data freshness monitoring

### Scalability
- ✅ Modular design (Ingestion, Transform, ML)
- ✅ Configurable via .env
- ✅ Database-centric architecture
- ✅ Parallel processing ready

### Security
- ✅ Environment variable management
- ✅ No hardcoded credentials
- ✅ Secure database connections
- ✅ Error handling without data leakage

### Monitoring
- ✅ Comprehensive logging
- ✅ Pipeline execution logs in database
- ✅ Data quality metrics
- ✅ ML model performance tracking

---

## 📈 Companies Analyzed

**US**: INTC, CI, F, ADBE  
**EU**: OR.PA (L'Oréal), SIE.DE (Siemens Energy)  

---

## 🚀 Quick Start

### 1. Setup
```bash
# Install dependencies
pip install -r requirements_complete.txt

# Setup PostgreSQL
psql -U postgres -f scripts/01_create_schema.sql

# Configure environment
cp .env.example .env
# Edit .env with your database credentials
```

### 2. Run Once
```bash
python main.py --once
```

### 3. Schedule Daily
```bash
# Daily at 22:00 UTC
python main.py --schedule 22:00

# Or use different time
python main.py --schedule 20:00
```

---

## 📊 Power BI Integration

### Essential Measures

1. **Price Variance**
   ```dax
   = SUM('Forecast'[predicted_price]) - SUM('Forecast'[actual_price])
   ```

2. **Price Variance %**
   ```dax
   = (SUM('Forecast'[predicted_price]) - SUM('Forecast'[actual_price])) 
   / SUM('Forecast'[actual_price])
   ```

3. **Undervaluation Score**
   ```dax
   = AVERAGE('Market Analysis'[undervaluation_score])
   ```

4. **Top 3 Undervalued (US)**
   ```dax
   = TOPN(3, FILTER('Companies', 'Companies'[region] = "US"), 
   'Market Analysis'[undervaluation_score])
   ```

5. **Top 3 Undervalued (EU)**
   ```dax
   = TOPN(3, FILTER('Companies', 'Companies'[region] = "EU"), 
   'Market Analysis'[undervaluation_score])
   ```

---

## 📋 File Structure

```
ETL-STOCKS-LEARN/
├── main.py                              # Entry point
├── requirements_complete.txt
├── .env.example
│
├── scripts/
│   └── 01_create_schema.sql            # Database setup
│
├── src/
│   ├── ingestion/
│   │   ├── dlt_pipeline.py             # DLT pipeline
│   │   ├── validators.py               # Data validation
│   │   └── fetchers.py                 # Yahoo Finance fetcher
│   │
│   ├── transformation/
│   │   ├── dbt_runner.py               # DBT executor
│   │   └── dbt_project/                # DBT models
│   │       ├── dbt_project.yml
│   │       ├── models/
│   │       └── tests/
│   │
│   ├── ml_layer/
│   │   ├── predictive_model.py         # PyCaret model
│   │   └── model_registry.py           # MLflow tracking
│   │
│   └── orchestration/
│       └── scheduler.py                # APScheduler
│
├── models/                              # Saved ML models
├── data/
│   └── logs/                           # Execution logs
└── config/
    └── companies_config.yaml           # Company configuration
```

---

## 🔍 Monitoring & Logging

### Pipeline Execution Log
```
2026-04-24 22:00:00 - Ingestion started
2026-04-24 22:05:30 - ✓ Loaded 2,340 records to Bronze
2026-04-24 22:06:00 - Transformation started (DBT run)
2026-04-24 22:08:45 - ✓ Transformed to Silver & Gold
2026-04-24 22:09:15 - ML Training started
2026-04-24 22:15:30 - ✓ Trained model (RMSE: 2.45, R²: 0.87)
2026-04-24 22:16:00 - ✓ Generated 6 predictions
2026-04-24 22:16:30 - Pipeline completed (Total: 16m 30s)
```

### Query Data Quality Metrics
```sql
SELECT 
    check_name, 
    check_type, 
    total_records, 
    failed_records, 
    pass_rate,
    check_date
FROM gold.data_quality_checks
ORDER BY check_date DESC
LIMIT 10;
```

### Query Model Performance
```sql
SELECT 
    ticker,
    predicted_price,
    actual_price,
    ABS(predicted_price - actual_price) as error,
    model_version,
    training_date
FROM gold.ai_forecast
WHERE actual_date <= CURRENT_DATE
ORDER BY forecast_date DESC;
```

---

## 🛠️ Troubleshooting

### Database Connection Failed
```bash
# Check PostgreSQL is running
psql -U etl_user -d etl_stocks -h localhost

# Verify .env credentials
cat .env | grep DB_
```

### DLT Pipeline Error
```bash
# Clear DLT state
rm -rf .dlt/

# Re-run
python src/ingestion/dlt_pipeline.py
```

### DBT Models Failed
```bash
# Test connection
cd src/transformation/dbt_project
dbt debug

# Run specific model
dbt run --select fct_market_analysis
```

### ML Training OOM Error
```python
# Reduce training data in predictive_model.py
df_raw = predictor.load_training_data(lookback_days=90)  # Instead of 365
```

---

## 📚 Next Steps

1. ✅ Run `python main.py --once` to test everything
2. ✅ Check `gold.pipeline_logs` for execution details
3. ✅ Query `gold.fct_market_analysis` for analysis
4. ✅ Review `gold.ai_forecast` for predictions
5. ✅ Create Power BI dashboard from Gold layer
6. ✅ Schedule daily runs: `python main.py --schedule 22:00`

---

**Production Ready! 🚀**