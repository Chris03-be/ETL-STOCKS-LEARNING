#!/usr/bin/env python3
"""
DLT Pipeline for Stock Data Ingestion
Recupère les données de yfinance et les charge dans PostgreSQL (Bronze Layer)
"""

import os
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

@dlt.resource(name="raw_stock_prices", write_disposition="merge", primary_key="id")
def load_stock_prices(tickers: List[str]) -> pd.DataFrame:
    """
    Récupère les données OHLCV de yfinance pour une liste de tickers.
    
    Args:
        tickers: Liste des symboles boursiers
        
    Yields:
        DataFrame avec les données de prix
    """
    logger.info(f"Starting price data ingestion for {len(tickers)} tickers")
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=HISTORICAL_DAYS)
    
    for ticker in tickers:
        try:
            logger.info(f"Fetching price data for {ticker}")
            
            # Fetch data from yfinance
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
            
            # Reset index to make date a column
            data = data.reset_index()
            data['ticker'] = ticker
            data['inserted_at'] = datetime.now()
            data['updated_at'] = datetime.now()
            
            # Rename columns to match schema
            data.columns = [
                col.lower().replace(' ', '_') if col not in ['Date', 'Adj Close']
                else col.lower().replace(' ', '_')
                for col in data.columns
            ]
            
            # Map yfinance columns to our schema
            data = data.rename(columns={
                'date': 'date',
                'open': 'open',
                'high': 'high',
                'low': 'low',
                'close': 'close',
                'volume': 'volume',
                'adj_close': 'adj_close',
                'ticker': 'ticker'
            })
            
            # Select only relevant columns
            columns_to_keep = [
                'ticker', 'date', 'open', 'high', 'low', 'close', 
                'volume', 'adj_close', 'inserted_at', 'updated_at'
            ]
            data = data[[col for col in columns_to_keep if col in data.columns]]
            
            # Convert date to datetime
            data['date'] = pd.to_datetime(data['date']).dt.date
            
            # Yield data in batches
            for i in range(0, len(data), INGESTION_BATCH_SIZE):
                batch = data.iloc[i:i + INGESTION_BATCH_SIZE]
                logger.info(f"Yielding batch for {ticker}: {len(batch)} rows")
                yield batch
                
            logger.info(f"Successfully fetched {len(data)} records for {ticker}")
            
        except Exception as e:
            logger.error(f"Error fetching data for {ticker}: {str(e)}")
            continue


@dlt.resource(name="raw_fundamentals", write_disposition="merge", primary_key="id")
def load_fundamentals(tickers: List[str]) -> pd.DataFrame:
    """
    Récupère les données fondamentales (P/E, Dividend, etc) de yfinance.
    
    Args:
        tickers: Liste des symboles boursiers
        
    Yields:
        DataFrame avec les données fondamentales
    """
    logger.info(f"Starting fundamentals data ingestion for {len(tickers)} tickers")
    
    fundamentals_data = []
    
    for ticker in tickers:
        try:
            logger.info(f"Fetching fundamentals for {ticker}")
            
            # Fetch ticker info
            ticker_obj = yf.Ticker(ticker)
            info = ticker_obj.info
            
            # Extract relevant fields
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
    
    if fundamentals_data:
        df = pd.DataFrame(fundamentals_data)
        yield df
    else:
        logger.warning("No fundamentals data retrieved")


def run_dlt_pipeline() -> Dict[str, any]:
    """
    Exécute le pipeline DLT complet.
    
    Returns:
        Dict avec les statistiques d'exécution
    """
    logger.info("="*60)
    logger.info("Starting DLT Pipeline")
    logger.info("="*60)
    
    try:
        # Configure DLT destination
        pipeline = dlt.pipeline(
            pipeline_name='stock_ingestion',
            destination=dlt.destinations.postgres(
                host=DB_HOST,
                port=int(DB_PORT),
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME,
                schema='bronze'
            )
        )
        
        # Load price data
        logger.info("Loading price data...")
        pipeline.run(
            load_stock_prices(TICKERS),
            table_name='raw_stock_prices'
        )
        
        # Load fundamentals data
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
    
    Args:
        result: Résultats d'exécution du pipeline
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
        logger.info("Pipeline execution logged successfully")
        
    except Exception as e:
        logger.error(f"Failed to log pipeline execution: {str(e)}")


if __name__ == "__main__":
    # Run the pipeline
    result = run_dlt_pipeline()
    
    # Log the result
    log_pipeline_execution(result)
    
    # Exit with appropriate code
    exit(0 if result['status'] == 'SUCCESS' else 1)