# 📊 Undervalued Stocks Analyzer - 10 Companies

> Production-ready ETL pipeline to identify undervalued stocks using fundamental and technical analysis.

![Status](https://img.shields.io/badge/Status-Production--Ready-green)
![Companies](https://img.shields.io/badge/Companies-10-blue)
![Python](https://img.shields.io/badge/Python-3.8%2B-green)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13%2B-lightblue)

---

## 🎯 Project Goal

Identify undervalued shares from 10 selected companies using:
- **Valuation Metrics**: P/E, P/B, PEG, Dividend Yield, FCF Yield
- **Technical Indicators**: RSI, MACD, Bollinger Bands, Moving Averages
- **Fundamental Analysis**: ROE, ROA, Debt ratios, Cash Flow
- **Composite Scoring**: 0-100 score + Grade (A-F) + Buy/Hold/Sell recommendation

---

## 🏢 10 Selected Companies

| # | Ticker | Company | Sector | Exchange |
|---|--------|---------|--------|----------|
| 1 | INTC | Intel | Semiconductors | NASDAQ |
| 2 | CI | Cigna Group | Healthcare/Insurance | NYSE |
| 3 | F | Ford | Automotive | NYSE |
| 4 | ADBE | Adobe | Software | NASDAQ |
| 5 | MO | Altria Group | Tobacco | NYSE |
| 6 | SIE | Siemens Energy | Energy/Industrial | XETRA |
| 7 | NESTE | Neste Oyj | Energy/Renewables | HEX |
| 8 | CMCSA | Comcast | Media/Telecom | NASDAQ |
| 9 | GDMK | Galderma Group | Pharmaceuticals | SIX |
| 10 | 601318.SS | Ping An Insurance | Insurance | Shanghai |

---

## 🏗️ Architecture

```
APIs (Yahoo Finance, Alpha Vantage)
    ↓
DLT (Data Load Tool) - Automated ingestion
    ↓
PostgreSQL (Bronze → Silver → Gold layers)
    ↓
DBT - Transformations & Valuation Scoring
    ↓
Power BI - Interactive dashboards
    ↓
APScheduler - Daily automation
```

---

## 📊 Data Layers

### Bronze Layer (Raw)
- Raw API responses from all sources
- No transformations
- Full historical data

### Silver Layer (Cleaned)
- Deduplicated, validated data
- Standardized formats
- Ready for analysis

### Gold Layer (Analytics)
- **fct_valuation_metrics**: P/E, P/B, PEG, Dividend Yield, FCF Yield
- **fct_technical_analysis**: RSI, MACD, Bollinger Bands, MAs
- **fct_undervaluation_scoring**: Composite score (0-100) + Grade + Recommendation
- **fct_peer_comparison**: Industry benchmarks
- **dim_companies**: Company dimensions

---

## 🚀 Quick Start

### 1. Setup Environment

```bash
# Clone repo
git clone https://github.com/Chris03-be/ETL-STOCKS-LEARNING.git
cd ETL-STOCKS-LEARNING

# Create virtual environment
python -m venv venv
source venv/bin/activate  # macOS/Linux
venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure

```bash
# Copy environment template
cp .env.example .env

# Edit .env with your PostgreSQL credentials
# DB_HOST=localhost
# DB_USER=postgres
# DB_PASSWORD=your_password
# DB_DATABASE=etl_stocks
```

### 3. Setup PostgreSQL

See [SETUP.md](SETUP.md)

### 4. Run Pipeline

```bash
# Test ingestion (fetch data for 10 companies)
python src/ingestion/dlt_pipeline.py

# Run DBT transformations
cd src/transformation/dbt_project
dbt run
dbt test

# Back to root
cd ../../..

# Start scheduler (daily 4 PM runs)
python src/orchestration/scheduler.py
```

### 5. Open Power BI Dashboard

```bash
Open: src/visualization/dashboards/undervalued_stocks.pbix
```

---

## 📋 Metrics Tracked

### Valuation Metrics
- **P/E Ratio** - Price-to-Earnings
- **P/B Ratio** - Price-to-Book
- **PEG Ratio** - P/E to Growth
- **Dividend Yield** - Annual dividend %
- **FCF Yield** - Free Cash Flow yield
- **Price-to-Sales** - P/S ratio

### Technical Indicators
- **RSI** - Relative Strength Index (0-100)
- **MACD** - Moving Average Convergence Divergence
- **Bollinger Bands** - Volatility bands
- **Moving Averages** - 50-day, 200-day
- **Support/Resistance** - Key price levels

### Fundamental Metrics
- **ROE** - Return on Equity
- **ROA** - Return on Assets
- **Debt-to-Equity** - Leverage ratio
- **Current Ratio** - Liquidity
- **Gross Margin** - Profitability
- **Net Margin** - Bottom line profitability
- **Free Cash Flow** - Cash generation
- **Earnings Growth** - Growth rate

---

## 🎯 Undervaluation Score

```
Composite Score (0-100):
├─ P/E Score (0-25)      → Lower P/E = higher score
├─ PEG Score (0-25)      → PEG < 1 = higher score
├─ Dividend Score (0-25) → Higher yield = higher score
└─ Technical Score (0-25)→ RSI < 30 = higher score

Grade:
├─ A (80-100) = STRONG BUY
├─ B (60-79)  = BUY
├─ C (40-59)  = HOLD
├─ D (20-39)  = SELL
└─ F (0-19)   = STRONG SELL
```

---

## 📊 Power BI Dashboards

### Executive Summary
- Top 3 most undervalued stocks
- Top 3 most overvalued stocks
- Portfolio health metrics

### Valuation Analysis
- P/E ratio comparison
- PEG ratio ranking
- Dividend yield matrix
- FCF analysis
- Intrinsic value vs current price

### Technical Analysis
- 1-year price charts with MAs
- RSI indicator overlay
- MACD charts
- Bollinger Bands
- Support/Resistance levels

### Undervaluation Scoring
- Composite score ranking (0-100)
- Undervaluation % distribution
- Risk-adjusted recommendations
- Buy/Hold/Sell signals

### Peer Comparison
- Sector metrics heatmap
- Relative valuation matrix
- Individual company deep-dive

---

## 🔄 Orchestration

**APScheduler** runs daily at **4 PM UTC** (after market close):

1. **DLT Pipeline** - Fetch latest data from all APIs
2. **Bronze Load** - Load raw data
3. **DBT Models** - Transform Silver → Gold layers
4. **Data Tests** - Validate data quality
5. **Logging** - Record execution details

---

## 📁 Project Structure

```
src/
├── ingestion/              # APIs + DLT
│   ├── dlt_pipeline.py
│   ├── fetchers.py
│   └── validators.py
│
├── transformation/         # DBT
│   ├── dbt_project/
│   │   ├── models/
│   │   │   ├── bronze/
│   │   │   ├── silver/
│   │   │   └── gold/
│   │   └── tests/
│   └── dbt_runner.py
│
├── visualization/          # Power BI
│   ├── dashboards/
│   └── queries/
│
and orchestration/          # APScheduler
    ├── scheduler.py
    └── tasks.py

config/
└── companies_config.yaml   # 10 companies config

data/
├── raw/
├── processed/
└── logs/
```

---

## 📚 Documentation

- [SETUP.md](SETUP.md) - Installation & PostgreSQL setup
- [ARCHITECTURE.md](ARCHITECTURE.md) - Complete system design
- [docs/](docs/) - Technical guides

---

## 🛠️ Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Python 3.8+ |
| APIs | yfinance, Alpha Vantage |
| ETL | DLT (Data Load Tool) |
| Database | PostgreSQL |
| Transformations | DBT |
| Visualization | Power BI Desktop |
| Orchestration | APScheduler |
| Testing | pytest |

---

## ✅ Features

- ✅ Automated daily data ingestion (DLT)
- ✅ Multi-layer data architecture (Bronze/Silver/Gold)
- ✅ Comprehensive valuation scoring (0-100)
- ✅ Technical + Fundamental analysis
- ✅ Professional Power BI dashboards
- ✅ Production-ready error handling
- ✅ Data quality tests (DBT)
- ✅ Scheduler for automation
- ✅ Logging & monitoring
- ✅ Portfolio-ready documentation

---

## 📈 Expected Output

Each day at 4 PM UTC:

```
✅ 10 companies data fetched
✅ 365+ days of historical data loaded to Bronze
✅ Data cleaned and deduplicated in Silver
✅ Valuation scores calculated (0-100)
✅ Technical indicators computed
✅ Peer comparisons generated
✅ Power BI dashboards auto-refreshed
✅ Rankings: Most undervalued → Most overvalued
✅ Buy/Hold/Sell recommendations generated
```

---

## 📞 Setup Questions?

See [SETUP.md](SETUP.md) for detailed instructions.

---

**Status**: 🟢 Production Ready  
**Last Updated**: April 24, 2026  
**License**: MIT