-- ============================================================
-- ETAPE 1: CREATION DES SCHEMAS ET TABLES BRONZE/SILVER/GOLD
-- ============================================================

-- 1. Créer la base si elle n'existe pas (optionnel)
-- CREATE DATABASE etl_stocks;

-- 2. Créer les schémas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ============================================================
-- BRONZE LAYER: Raw Data (Données brutes de yfinance)
-- ============================================================

CREATE TABLE IF NOT EXISTS bronze.raw_stock_prices (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL,
    date DATE NOT NULL,
    open DECIMAL(12, 2),
    high DECIMAL(12, 2),
    low DECIMAL(12, 2),
    close DECIMAL(12, 2) NOT NULL,
    volume BIGINT NOT NULL,
    adj_close DECIMAL(12, 2),
    inserted_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    
    -- Clé unique pour éviter les doublons
    UNIQUE (ticker, date),
    INDEX idx_ticker_date (ticker, date),
    INDEX idx_date (date)
);

CREATE TABLE IF NOT EXISTS bronze.raw_fundamentals (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL,
    date DATE NOT NULL,
    pe_ratio DECIMAL(10, 2),
    pb_ratio DECIMAL(10, 2),
    dividend_yield DECIMAL(8, 4),
    market_cap BIGINT,
    eps DECIMAL(10, 2),
    revenue BIGINT,
    fifty_two_week_high DECIMAL(12, 2),
    fifty_two_week_low DECIMAL(12, 2),
    inserted_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    
    UNIQUE (ticker, date),
    INDEX idx_ticker_date (ticker, date)
);

CREATE TABLE IF NOT EXISTS bronze.raw_metadata (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL UNIQUE,
    company_name VARCHAR(255),
    sector VARCHAR(100),
    country VARCHAR(50),
    exchange VARCHAR(20),
    currency VARCHAR(5),
    inserted_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    INDEX idx_ticker (ticker)
);

-- ============================================================
-- SILVER LAYER: Cleaned and Validated Data
-- ============================================================

CREATE TABLE IF NOT EXISTS silver.stg_stock_prices_clean (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL,
    date DATE NOT NULL,
    open DECIMAL(12, 2),
    high DECIMAL(12, 2),
    low DECIMAL(12, 2),
    close DECIMAL(12, 2) NOT NULL,
    volume BIGINT NOT NULL,
    adj_close DECIMAL(12, 2),
    
    -- Data quality flags
    is_valid BOOLEAN DEFAULT TRUE,
    validation_errors TEXT,
    
    -- Tracking
    inserted_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    
    UNIQUE (ticker, date),
    INDEX idx_ticker_date (ticker, date),
    INDEX idx_valid (is_valid)
);

CREATE TABLE IF NOT EXISTS silver.stg_fundamentals_clean (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL,
    date DATE NOT NULL,
    pe_ratio DECIMAL(10, 2),
    pb_ratio DECIMAL(10, 2),
    dividend_yield DECIMAL(8, 4),
    market_cap BIGINT,
    eps DECIMAL(10, 2),
    
    is_valid BOOLEAN DEFAULT TRUE,
    validation_errors TEXT,
    
    inserted_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    
    UNIQUE (ticker, date),
    INDEX idx_ticker_date (ticker, date)
);

-- ============================================================
-- GOLD LAYER: Analytical Data (Ready for BI & ML)
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.fct_market_analysis (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL,
    date DATE NOT NULL,
    
    -- Price data
    open_price DECIMAL(12, 2),
    high_price DECIMAL(12, 2),
    low_price DECIMAL(12, 2),
    close_price DECIMAL(12, 2) NOT NULL,
    volume BIGINT NOT NULL,
    adj_close DECIMAL(12, 2),
    
    -- Technical indicators
    ma_50 DECIMAL(12, 2),          -- 50-day moving average
    ma_200 DECIMAL(12, 2),         -- 200-day moving average
    volatility_30d DECIMAL(8, 4),  -- 30-day volatility (std dev)
    rsi_14 DECIMAL(8, 2),          -- RSI 14-day
    
    -- Valuation metrics
    pe_ratio DECIMAL(10, 2),
    pb_ratio DECIMAL(10, 2),
    dividend_yield DECIMAL(8, 4),
    
    -- Undervaluation indicator
    is_undervalued BOOLEAN,
    undervaluation_score DECIMAL(5, 2),  -- 0-100
    
    -- Risk indicators
    zscore DECIMAL(8, 4),          -- Z-score for outlier detection
    
    -- Timestamps
    inserted_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    
    UNIQUE (ticker, date),
    INDEX idx_ticker_date (ticker, date),
    INDEX idx_undervalued (is_undervalued),
    INDEX idx_date (date)
);

CREATE TABLE IF NOT EXISTS gold.ai_forecast (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL,
    forecast_date DATE NOT NULL,   -- Date de la prédiction
    actual_date DATE NOT NULL,     -- Date réelle (à 7 jours)
    
    -- Predictions
    predicted_price DECIMAL(12, 2) NOT NULL,
    predicted_direction VARCHAR(10),  -- 'UP', 'DOWN', 'NEUTRAL'
    prediction_confidence DECIMAL(5, 2),  -- 0-100
    
    -- Actual vs Predicted
    actual_price DECIMAL(12, 2),
    prediction_error DECIMAL(12, 2),
    prediction_error_pct DECIMAL(8, 2),
    
    -- Model metadata
    model_version VARCHAR(50),
    model_name VARCHAR(100),
    training_date DATE,
    
    -- Timestamps
    inserted_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    
    INDEX idx_ticker_date (ticker, forecast_date),
    INDEX idx_actual_date (actual_date),
    INDEX idx_confidence (prediction_confidence DESC)
);

CREATE TABLE IF NOT EXISTS gold.dim_companies (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL UNIQUE,
    company_name VARCHAR(255),
    sector VARCHAR(100),
    country VARCHAR(50),
    region VARCHAR(50),  -- 'US' or 'EU'
    exchange VARCHAR(20),
    currency VARCHAR(5),
    market_cap BIGINT,
    
    inserted_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    
    INDEX idx_ticker (ticker),
    INDEX idx_region (region),
    INDEX idx_sector (sector)
);

-- ============================================================
-- DATA QUALITY TRACKING
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.data_quality_checks (
    id SERIAL PRIMARY KEY,
    check_name VARCHAR(100) NOT NULL,
    check_type VARCHAR(50),  -- 'schema', 'not_null', 'range', 'uniqueness', 'outlier', 'freshness'
    ticker VARCHAR(20),
    check_date DATE,
    total_records BIGINT,
    failed_records BIGINT,
    pass_rate DECIMAL(5, 2),  -- 0-100
    error_details TEXT,
    
    inserted_at TIMESTAMP DEFAULT NOW(),
    
    INDEX idx_check_date (check_date),
    INDEX idx_ticker (ticker)
);

CREATE TABLE IF NOT EXISTS gold.pipeline_logs (
    id SERIAL PRIMARY KEY,
    stage VARCHAR(50),  -- 'INGESTION', 'TRANSFORMATION', 'ML', 'VALIDATION'
    status VARCHAR(20),  -- 'SUCCESS', 'FAILED', 'WARNING'
    message TEXT,
    execution_time_seconds DECIMAL(10, 2),
    rows_processed BIGINT,
    error_details TEXT,
    
    executed_at TIMESTAMP DEFAULT NOW(),
    
    INDEX idx_stage (stage),
    INDEX idx_status (status),
    INDEX idx_executed_at (executed_at)
);

-- ============================================================
-- SAMPLE DATA FOR TESTING
-- ============================================================

INSERT INTO gold.dim_companies (ticker, company_name, sector, country, region, exchange, currency, market_cap)
VALUES
    ('INTC', 'Intel Corporation', 'Semiconductors', 'USA', 'US', 'NASDAQ', 'USD', 187500000000),
    ('CI', 'Cigna Group', 'Healthcare', 'USA', 'US', 'NYSE', 'USD', 46800000000),
    ('F', 'Ford Motor Company', 'Automotive', 'USA', 'US', 'NYSE', 'USD', 33200000000),
    ('ADBE', 'Adobe Inc.', 'Software', 'USA', 'US', 'NASDAQ', 'USD', 165400000000),
    ('OR.PA', 'L''Oréal S.A.', 'Consumer Goods', 'France', 'EU', 'EURONEXT', 'EUR', 180000000000),
    ('SIE.DE', 'Siemens Energy AG', 'Energy', 'Germany', 'EU', 'XETRA', 'EUR', 22300000000)
ON CONFLICT (ticker) DO NOTHING;

-- ============================================================
-- PERMISSIONS (Optional: Create read-only user for BI)
-- ============================================================

-- CREATE USER bi_user WITH PASSWORD 'bi_password';
-- GRANT CONNECT ON DATABASE etl_stocks TO bi_user;
-- GRANT USAGE ON SCHEMA gold TO bi_user;
-- GRANT SELECT ON ALL TABLES IN SCHEMA gold TO bi_user;
-- ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT ON TABLES TO bi_user;

-- ============================================================
-- VIEW FOR POWER BI (Optional)
-- ============================================================

CREATE OR REPLACE VIEW gold.v_market_analysis_summary AS
SELECT 
    ma.id,
    ma.ticker,
    dc.company_name,
    dc.sector,
    dc.region,
    ma.date,
    ma.close_price,
    ma.ma_50,
    ma.ma_200,
    ma.volatility_30d,
    ma.pe_ratio,
    ma.dividend_yield,
    ma.is_undervalued,
    ma.undervaluation_score,
    ma.inserted_at
FROM gold.fct_market_analysis ma
JOIN gold.dim_companies dc ON ma.ticker = dc.ticker
ORDER BY ma.date DESC, ma.ticker;

-- ============================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_gold_ma_undervalued ON gold.fct_market_analysis(is_undervalued, undervaluation_score DESC);
CREATE INDEX IF NOT EXISTS idx_gold_forecast_ticker_date ON gold.ai_forecast(ticker, forecast_date DESC);
CREATE INDEX IF NOT EXISTS idx_gold_forecast_confidence ON gold.ai_forecast(prediction_confidence DESC);

COMMIT;

SELECT 'Schema creation completed successfully!' AS status;