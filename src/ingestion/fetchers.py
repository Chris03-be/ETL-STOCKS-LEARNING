#!/usr/bin/env python3
"""
Data Fetchers Module
Récupère les données de yfinance avec gestion d'erreurs et retry logic
"""

import logging
from typing import Dict, Optional, Tuple, Any
from datetime import datetime, timedelta
import time

import yfinance as yf
import pandas as pd
from requests.exceptions import RequestException, Timeout, ConnectionError

logger = logging.getLogger(__name__)

# Configuration
MAX_RETRIES = 3
RETRY_DELAY = 2  # secondes
TIMEOUT = 30  # secondes


class YahooFinanceFetcher:
    """Classe pour récupérer les données de Yahoo Finance avec retry logic"""
    
    def __init__(self, max_retries: int = MAX_RETRIES, retry_delay: int = RETRY_DELAY):
        """
        Initialise le fetcher.
        
        Args:
            max_retries: Nombre maximum de tentatives
            retry_delay: Délai entre les tentatives (secondes)
        """
        self.max_retries = max_retries
        self.retry_delay = retry_delay
    
    def fetch_historical_prices(
        self,
        ticker: str,
        period: str = '1y',
        interval: str = '1d'
    ) -> Optional[pd.DataFrame]:
        """
        Récupère les données historiques de prix.
        
        Args:
            ticker: Symbole boursier (ex: 'INTC')
            period: Période ('1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max')
            interval: Intervalle ('1m', '5m', '15m', '30m', '60m', '1d', '1wk', '1mo')
            
        Returns:
            DataFrame avec colonnes [Date, Open, High, Low, Close, Volume, Adj Close]
        """
        logger.info(f"Fetching historical prices for {ticker} (period={period}, interval={interval})")
        
        for attempt in range(self.max_retries):
            try:
                data = yf.download(
                    ticker,
                    period=period,
                    interval=interval,
                    progress=False,
                    timeout=TIMEOUT
                )
                
                if data.empty:
                    logger.warning(f"No data returned for {ticker}")
                    return None
                
                logger.info(f"Successfully fetched {len(data)} records for {ticker}")
                data['ticker'] = ticker
                return data.reset_index()
                
            except (RequestException, Timeout, ConnectionError) as e:
                logger.warning(f"Attempt {attempt + 1}/{self.max_retries} failed for {ticker}: {str(e)}")
                
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    logger.error(f"Failed to fetch data for {ticker} after {self.max_retries} attempts")
                    return None
            
            except Exception as e:
                logger.error(f"Unexpected error fetching {ticker}: {str(e)}")
                return None
        
        return None
    
    def fetch_fundamentals(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Récupère les données fondamentales d'une entreprise.
        
        Args:
            ticker: Symbole boursier
            
        Returns:
            Dict avec les données fondamentales
        """
        logger.info(f"Fetching fundamentals for {ticker}")
        
        for attempt in range(self.max_retries):
            try:
                ticker_obj = yf.Ticker(ticker, session=None)
                info = ticker_obj.info
                
                if not info:
                    logger.warning(f"No fundamentals returned for {ticker}")
                    return None
                
                # Extract key metrics
                fundamentals = {
                    'ticker': ticker,
                    'date': datetime.now().date(),
                    'pe_ratio': info.get('trailingPE'),
                    'pb_ratio': info.get('priceToBook'),
                    'dividend_yield': info.get('dividendYield'),
                    'market_cap': info.get('marketCap'),
                    'eps': info.get('trailingEps'),
                    'revenue': info.get('totalRevenue'),
                    'fifty_two_week_high': info.get('fiftyTwoWeekHigh'),
                    'fifty_two_week_low': info.get('fiftyTwoWeekLow'),
                    'profit_margin': info.get('profitMargins'),
                    'operating_margin': info.get('operatingMargins'),
                    'roe': info.get('returnOnEquity'),
                    'debt_to_equity': info.get('debtToEquity'),
                    'current_ratio': info.get('currentRatio'),
                    'employees': info.get('fullTimeEmployees')
                }
                
                logger.info(f"Successfully fetched fundamentals for {ticker}")
                return fundamentals
                
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1}/{self.max_retries} failed for {ticker}: {str(e)}")
                
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    logger.error(f"Failed to fetch fundamentals for {ticker} after {self.max_retries} attempts")
                    return None
        
        return None
    
    def fetch_technical_indicators(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Récupère les indicateurs techniques calculés.
        
        Args:
            ticker: Symbole boursier
            
        Returns:
            Dict avec les indicateurs techniques
        """
        logger.info(f"Fetching technical indicators for {ticker}")
        
        try:
            # Fetch historical data
            hist = yf.download(ticker, period='1y', progress=False)
            
            if hist.empty:
                logger.warning(f"No historical data for technical indicators: {ticker}")
                return None
            
            # Calculate simple technical indicators
            close = hist['Close']
            
            # Moving averages
            ma_50 = close.rolling(window=50).mean().iloc[-1]
            ma_200 = close.rolling(window=200).mean().iloc[-1]
            
            # Volatility (30-day std dev)
            volatility_30d = close.pct_change().rolling(window=30).std().iloc[-1]
            
            # RSI (14-day)
            rsi_14 = self._calculate_rsi(close, 14)
            
            # Price change
            price_change_1d = close.pct_change().iloc[-1]
            price_change_1w = close.pct_change(5).iloc[-1]
            price_change_1m = close.pct_change(20).iloc[-1]
            
            indicators = {
                'ticker': ticker,
                'date': datetime.now().date(),
                'ma_50': ma_50,
                'ma_200': ma_200,
                'volatility_30d': volatility_30d,
                'rsi_14': rsi_14,
                'price_change_1d': price_change_1d,
                'price_change_1w': price_change_1w,
                'price_change_1m': price_change_1m,
                'current_price': close.iloc[-1]
            }
            
            logger.info(f"Successfully calculated technical indicators for {ticker}")
            return indicators
            
        except Exception as e:
            logger.error(f"Failed to calculate technical indicators for {ticker}: {str(e)}")
            return None
    
    @staticmethod
    def _calculate_rsi(prices: pd.Series, period: int = 14) -> float:
        """
        Calcule le RSI (Relative Strength Index).
        
        Args:
            prices: Série de prix de clôture
            period: Nombre de périodes (par défaut 14)
            
        Returns:
            Valeur RSI (0-100)
        """
        try:
            delta = prices.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
            
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            
            return float(rsi.iloc[-1]) if not pd.isna(rsi.iloc[-1]) else None
        
        except Exception as e:
            logger.error(f"Error calculating RSI: {str(e)}")
            return None
    
    def fetch_all_data(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Récupère toutes les données disponibles pour un ticker.
        
        Args:
            ticker: Symbole boursier
            
        Returns:
            Dict avec prices, fundamentals, et technical indicators
        """
        logger.info(f"Fetching all data for {ticker}")
        
        try:
            prices = self.fetch_historical_prices(ticker, period='1y')
            fundamentals = self.fetch_fundamentals(ticker)
            technical = self.fetch_technical_indicators(ticker)
            
            return {
                'ticker': ticker,
                'prices': prices,
                'fundamentals': fundamentals,
                'technical': technical,
                'fetch_timestamp': datetime.now()
            }
        
        except Exception as e:
            logger.error(f"Error fetching all data for {ticker}: {str(e)}")
            return None


def fetch_stock_data(ticker: str, data_type: str = 'all') -> Optional[Dict[str, Any]]:
    """
    Fonction helper pour récupérer les données d'une action.
    
    Args:
        ticker: Symbole boursier
        data_type: Type de données ('prices', 'fundamentals', 'technical', 'all')
        
    Returns:
        Dict avec les données demandées
    """
    fetcher = YahooFinanceFetcher()
    
    if data_type == 'prices':
        return {'prices': fetcher.fetch_historical_prices(ticker)}
    elif data_type == 'fundamentals':
        return {'fundamentals': fetcher.fetch_fundamentals(ticker)}
    elif data_type == 'technical':
        return {'technical': fetcher.fetch_technical_indicators(ticker)}
    else:  # 'all'
        return fetcher.fetch_all_data(ticker)