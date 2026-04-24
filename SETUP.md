# 🚀 Setup Guide - Undervalued Stocks Analyzer

## Prerequisites

- Python 3.8+
- PostgreSQL 13+
- Git
- VSCode (recommended)
- Power BI Desktop (for dashboards)

---

## 1. Clone & Environment Setup

```bash
# Clone repository
git clone https://github.com/Chris03-be/ETL-STOCKS-LEARNING.git
cd ETL-STOCKS-LEARNING

# Create virtual environment
python -m venv venv

# Activate (macOS/Linux)
source venv/bin/activate

# Activate (Windows)
venv\Scripts\activate

# Upgrade pip
pip install --upgrade pip

# Install dependencies
pip install -r requirements.txt
```

---

## 2. PostgreSQL Setup

### macOS (Homebrew)

```bash
# Install PostgreSQL
brew install postgresql

# Start service
brew services start postgresql

# Connect
psql -U postgres
```

### Linux (Ubuntu/Debian)

```bash
# Install
sudo apt-get install postgresql postgresql-contrib

# Start service
sudo systemctl start postgresql

# Connect
sudo -u postgres psql
```

### Windows

1. Download from: https://www.postgresql.org/download/windows/
2. Run installer
3. Remember the password you set for `postgres` user
4. Start PostgreSQL service from Services

---

## 3. Create Database & User

```sql
-- Connect as postgres
psql -U postgres

-- Create database
CREATE DATABASE etl_stocks;

-- Create user
CREATE USER etl_user WITH PASSWORD 'your_secure_password';

-- Grant privileges
ALTER ROLE etl_user SET client_encoding TO 'utf8';
ALTER ROLE etl_user SET default_transaction_isolation TO 'read committed';
ALTER ROLE etl_user SET default_transaction_deferrable TO on;
GRANT ALL PRIVILEGES ON DATABASE etl_stocks TO etl_user;

-- Exit
\q
```

---

## 4. Verify Database Connection

```bash
# Test connection
psql -U etl_user -d etl_stocks -h localhost -p 5432

# If successful, you'll see:
# etl_stocks=>  (ready for SQL commands)

# Exit
\q
```

---

## 5. Configure Environment Variables

```bash
# Copy template
cp .env.example .env

# Edit .env with your settings
# nano .env  (or use your editor)
```

Set these values:

```env
DB_HOST=localhost
DB_PORT=5432
DB_USER=etl_user
DB_PASSWORD=your_secure_password
DB_DATABASE=etl_stocks
DB_SCHEMA=public

# Optional: API keys (if using premium APIs)
ALPHA_VANTAGE_API_KEY=your_key
IEX_CLOUD_API_KEY=your_key
```

---

## 6. Test Connection from Python

```bash
# Create test script
cat > test_db.py << 'EOF'
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

try:
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_DATABASE'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )
    cursor = conn.cursor()
    cursor.execute('SELECT NOW()')
    result = cursor.fetchone()
    print(f"✅ Connection successful! Current time: {result[0]}")
    cursor.close()
    conn.close()
except Exception as e:
    print(f"❌ Connection failed: {e}")
EOF

# Run test
python test_db.py
```

---

## 7. Setup DLT

```bash
# Initialize DLT
dlt init etl_stocks

# This creates: ~/.dlt/secrets.toml
# Edit it with your PostgreSQL credentials
```

```toml
# ~/.dlt/secrets.toml
[destination.postgres]
database = "etl_stocks"
username = "etl_user"
password = "your_secure_password"
host = "localhost"
port = 5432
```

---

## 8. Setup DBT

```bash
# Initialize DBT project (if not already done)
cd src/transformation
dbt init dbt_project  # Skip if already exists
cd dbt_project

# Create profiles.yml
mkdir -p ~/.dbt

# Edit ~/.dbt/profiles.yml
```

```yaml
etl_stocks:
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: etl_user
      password: your_secure_password
      dbname: etl_stocks
      schema: public
      threads: 4
      keepalives_idle: 0
      
  target: dev
```

```bash
# Test DBT connection
dbt debug
```

---

## 9. Create Initial Database Schema

```bash
# Run DLT pipeline once to create Bronze layer
cd ETL-STOCKS-LEARNING
python src/ingestion/dlt_pipeline.py

# This will:
# - Create _dlt_loads table
# - Create raw API data tables
# - Load initial data for all 10 companies
```

---

## 10. Run DBT Models

```bash
# Navigate to DBT project
cd src/transformation/dbt_project

# Run all models
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve  # Opens browser on localhost:8000
```

---

## 11. Setup Power BI

1. **Download Power BI Desktop** (free): https://powerbi.microsoft.com/en-us/desktop/

2. **Open the dashboard**:
   ```
   src/visualization/dashboards/undervalued_stocks.pbix
   ```

3. **Configure data source**:
   - Click "Edit Queries"
   - Update PostgreSQL connection
   - Point to your `etl_stocks` database
   - Refresh data

4. **Dashboards available**:
   - Executive Summary
   - Valuation Analysis
   - Technical Analysis
   - Undervaluation Scoring
   - Peer Comparison

---

## 12. Setup Scheduler

```bash
# Run scheduler (daily 4 PM runs)
python src/orchestration/scheduler.py

# This will:
# - Start APScheduler in background
# - Run DLT pipeline daily at 4 PM UTC
# - Run DBT transformations
# - Log everything
# - Keep running until you stop it (Ctrl+C)
```

---

## ✅ Verification Checklist

- [ ] Python 3.8+ installed
- [ ] PostgreSQL running
- [ ] `etl_stocks` database created
- [ ] `etl_user` with permissions
- [ ] `.env` file configured
- [ ] Python database connection works
- [ ] DLT configured
- [ ] DBT configured & `dbt debug` passes
- [ ] Initial DLT pipeline run successful
- [ ] DBT models run successfully
- [ ] Power BI dashboards open
- [ ] Scheduler started

---

## 🐛 Troubleshooting

### PostgreSQL Connection Refused

```bash
# Check if PostgreSQL is running
psql --version

# macOS
brew services list
brew services start postgresql

# Linux
sudo systemctl status postgresql
sudo systemctl start postgresql
```

### DLT Issues

```bash
# Clear DLT state
rm -rf .dlt/

# Reinitialize
dlt init etl_stocks
```

### DBT Connection Failed

```bash
# Test connection
dbt debug

# Check ~/.dbt/profiles.yml
cat ~/.dbt/profiles.yml

# Verify PostgreSQL credentials
psql -U etl_user -d etl_stocks -h localhost
```

### Power BI Can't Connect

1. Verify PostgreSQL is running
2. Check firewall allows localhost:5432
3. Verify connection string in Power BI
4. Try: `psql -h localhost -U etl_user -d etl_stocks`

---

## 🚀 First Run

```bash
# 1. Activate venv
source venv/bin/activate

# 2. Run DLT pipeline
python src/ingestion/dlt_pipeline.py

# Expected output:
# ✅ Fetching INTC data...
# ✅ Fetching CI data...
# ... (all 10 companies)
# ✅ Pipeline completed

# 3. Run DBT
cd src/transformation/dbt_project
dbt run

# Expected output:
# Running with dbt 1.7.0
# Found 15 models, ...
# Completed successfully

# 4. Check data in PostgreSQL
psql -U etl_user -d etl_stocks

# SELECT COUNT(*) FROM public.raw_stock_prices;
# SELECT COUNT(*) FROM gold.fct_valuation_metrics;
```

---

## 📊 Next Steps

1. Run initial pipeline: `python src/ingestion/dlt_pipeline.py`
2. Review data in PostgreSQL
3. Check DBT models: `dbt docs serve`
4. Open Power BI dashboards
5. Start scheduler: `python src/orchestration/scheduler.py`

---

**Setup complete! Your pipeline is ready to run.** 🎉