#!/usr/bin/env python3
"""
DLT Pipeline for Stock Data Ingestion
Recupère les données de yfinance et les charge dans PostgreSQL (Bronze Layer)
"""

import os

# --- FIX SYSTÉMIQUE POUR WINDOWS ET PYARROW ---
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
# ----------------------------------------------

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from pathlib import Path

import dlt
import yfinance as yf
import pandas as pd
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

# ============================================================
# Configuration
# ============================================================

load_dotenv()

# Database Configuration
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_USER = os.getenv('DB_USER', 'etl_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')
DB_NAME = os.getenv('DB_DATABASE', 'etl_stocks')

# Pipeline Configuration
TICKERS = ['INTC', 'CI', 'F', 'ADBE', 'OR.PA', 'SIE.DE']
HISTORICAL_DAYS = 365
INGESTION_BATCH_SIZE = 100

# Logging Configuration
LOG_DIR = Path('data/logs')
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"ingestion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# ============================================================
# DLT Configuration
# ============================================================

@dlt.resource(name="raw_stock_prices", write_disposition="merge", primary_key=["ticker", "date"])
def load_stock_prices(tickers: List[str]):
    """
    Récupère les données OHLCV de yfinance pour une liste de tickers.
    """
    logger.info(f"Starting price data ingestion for {len(tickers)} tickers")
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=HISTORICAL_DAYS)
    
    for ticker in tickers:
        try:
            logger.info(f"Fetching price data for {ticker}")
            
            data = yf.download(
                ticker,
                start=start_date,
                end=end_date,
                progress=False,
                interval='1d'
            )
            
            if data.empty:
                logger.warning(f"No data found for {ticker}")
                continue
            
            # 1. PURGE DU FUSEAU HORAIRE
            if isinstance(data.index, pd.DatetimeIndex) and data.index.tz is not None:
                data.index = data.index.tz_localize(None)
            
            # 2. NOMMAGE EXPLICITE DE L'INDEX
            data.index.name = 'date'
            data = data.reset_index()
            
            # 3. NETTOYAGE DES COLONNES (Minuscules, pas d'espaces, pas de tuples)
            new_cols = []
            for col in data.columns:
                col_name = col[0] if isinstance(col, tuple) else col
                new_cols.append(str(col_name).lower().replace(' ', '_'))
            data.columns = new_cols
            
            # 4. AJOUT DES MÉTADONNÉES
            data['ticker'] = ticker
            data['inserted_at'] = datetime.now()
            data['updated_at'] = datetime.now()
            
            # 5. FILTRE DES COLONNES
            columns_to_keep = [
                'ticker', 'date', 'open', 'high', 'low', 'close', 
                'volume', 'adj_close', 'inserted_at', 'updated_at'
            ]
            data = data[[col for col in columns_to_keep if col in data.columns]]
            
            # 6. FORMATAGE DE LA DATE (Maintenant que la colonne s'appelle bien 'date')
            data['date'] = pd.to_datetime(data['date']).dt.date
            
            # 7. BYPASS PYARROW (Extraction via dictionnaires natifs)
            for i in range(0, len(data), INGESTION_BATCH_SIZE):
                batch = data.iloc[i:i + INGESTION_BATCH_SIZE]
                logger.info(f"Yielding batch for {ticker}: {len(batch)} rows")
                yield batch.to_dict(orient='records')
                
            logger.info(f"Successfully fetched {len(data)} records for {ticker}")
            
        except Exception as e:
            logger.error(f"Error fetching data for {ticker}: {str(e)}")
            continue

@dlt.resource(name="raw_fundamentals", write_disposition="merge", primary_key=["ticker", "date"])
def load_fundamentals(tickers: List[str]):
    """
    Récupère les données fondamentales (P/E, Dividend, etc) de yfinance.
    """
    logger.info(f"Starting fundamentals data ingestion for {len(tickers)} tickers")
    fundamentals_data = []
    
    for ticker in tickers:
        try:
            logger.info(f"Fetching fundamentals for {ticker}")
            ticker_obj = yf.Ticker(ticker)
            info = ticker_obj.info
            
            fundamental_record = {
                'ticker': ticker,
                'date': datetime.now().date(),
                'pe_ratio': info.get('trailingPE', None),
                'pb_ratio': info.get('priceToBook', None),
                'dividend_yield': info.get('dividendYield', None),
                'market_cap': info.get('marketCap', None),
                'eps': info.get('trailingEps', None),
                'revenue': info.get('totalRevenue', None),
                'fifty_two_week_high': info.get('fiftyTwoWeekHigh', None),
                'fifty_two_week_low': info.get('fiftyTwoWeekLow', None),
                'inserted_at': datetime.now(),
                'updated_at': datetime.now()
            }
            fundamentals_data.append(fundamental_record)
            logger.info(f"Successfully fetched fundamentals for {ticker}")
            
        except Exception as e:
            logger.error(f"Error fetching fundamentals for {ticker}: {str(e)}")
            continue
    
    # BYPASS PYARROW POUR LES FONDAMENTAUX
    if fundamentals_data:
        yield fundamentals_data
    else:
        logger.warning("No fundamentals data retrieved")

def run_dlt_pipeline() -> Dict[str, any]:
    """
    Exécute le pipeline DLT complet.
    """
    logger.info("="*60)
    logger.info("Starting DLT Pipeline")
    logger.info("="*60)
    
    try:
        db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        
        pipeline = dlt.pipeline(
            pipeline_name='stock_ingestion_V2',
            destination=dlt.destinations.postgres(credentials=db_url),
            dataset_name='bronze'
        )
        
        logger.info("Loading price data...")
        pipeline.run(
            load_stock_prices(TICKERS),
            table_name='raw_stock_prices'
        )
        
        logger.info("Loading fundamentals data...")
        pipeline.run(
            load_fundamentals(TICKERS),
            table_name='raw_fundamentals'
        )
        
        logger.info("="*60)
        logger.info("DLT Pipeline completed successfully")
        logger.info("="*60)
        
        return {
            'status': 'SUCCESS',
            'tickers_processed': len(TICKERS),
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"DLT Pipeline failed: {str(e)}")
        return {
            'status': 'FAILED',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }

def log_pipeline_execution(result: Dict[str, any]) -> None:
    """
    Enregistre les résultats du pipeline dans la base de données.
    """
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=int(DB_PORT),
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        cursor = conn.cursor()
        
        cursor.execute("CREATE SCHEMA IF NOT EXISTS gold;")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS gold.pipeline_logs (
                id SERIAL PRIMARY KEY,
                stage VARCHAR(50),
                status VARCHAR(50),
                message TEXT,
                rows_processed INTEGER,
                executed_at TIMESTAMP
            );
        """)
        
        query = """
        INSERT INTO gold.pipeline_logs 
        (stage, status, message, rows_processed, executed_at)
        VALUES (%s, %s, %s, %s, NOW())
        """
        
        cursor.execute(
            query,
            (
                'INGESTION',
                result.get('status', 'UNKNOWN'),
                str(result),
                result.get('tickers_processed', 0)
            )
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("Pipeline execution logged successfully in gold.pipeline_logs")
        
    except Exception as e:
        logger.error(f"Failed to log pipeline execution: {str(e)}")

if __name__ == "__main__":
    result = run_dlt_pipeline()
    log_pipeline_execution(result)
    exit(0 if result['status'] == 'SUCCESS' else 1)