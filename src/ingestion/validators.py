#!/usr/bin/env python3
"""
Data Validation Module
Valide la qualité des données au niveau Bronze et Silver
"""

import logging
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
import os

import pandas as pd
import numpy as np
import psycopg2
from scipy import stats
from dotenv import load_dotenv

load_dotenv()

# Database Configuration
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_USER = os.getenv('DB_USER', 'etl_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')
DB_NAME = os.getenv('DB_DATABASE', 'etl_stocks')

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Résultat d'une validation"""
    check_name: str
    check_type: str
    ticker: str
    total_records: int
    failed_records: int
    pass_rate: float
    is_passed: bool
    error_details: Optional[str] = None
    check_date: datetime = None

    def __post_init__(self):
        if self.check_date is None:
            self.check_date = datetime.now().date()


class DataValidator:
    """Classe pour valider les données de prix et fondamentales"""
    
    def __init__(self, df: pd.DataFrame, ticker: str, check_type: str = 'prices'):
        """
        Initialise le validateur.
        
        Args:
            df: DataFrame à valider
            ticker: Symbole boursier
            check_type: Type de données ('prices' ou 'fundamentals')
        """
        self.df = df.copy()
        self.ticker = ticker
        self.check_type = check_type
        self.validation_results: List[ValidationResult] = []
    
    def validate_schema(self) -> ValidationResult:
        """
        Valide la structure du schéma (colonnes et types).
        
        Returns:
            ValidationResult
        """
        logger.info(f"Validating schema for {self.ticker}")
        
        required_columns = {
            'prices': ['ticker', 'date', 'close', 'volume'],
            'fundamentals': ['ticker', 'date', 'pe_ratio']
        }
        
        required = required_columns.get(self.check_type, [])
        missing_cols = [col for col in required if col not in self.df.columns]
        
        failed_records = len(missing_cols)
        is_passed = len(missing_cols) == 0
        
        result = ValidationResult(
            check_name='Schema Validation',
            check_type='schema',
            ticker=self.ticker,
            total_records=len(self.df),
            failed_records=failed_records,
            pass_rate=100 if is_passed else 0,
            is_passed=is_passed,
            error_details=f"Missing columns: {missing_cols}" if missing_cols else None
        )
        
        self.validation_results.append(result)
        return result
    
    def validate_not_null(self, columns: List[str]) -> ValidationResult:
        """
        Valide qu'aucune valeur NULL n'existe dans les colonnes critiques.
        
        Args:
            columns: Colonnes à vérifier
            
        Returns:
            ValidationResult
        """
        logger.info(f"Validating NOT NULL for {self.ticker}")
        
        null_mask = self.df[columns].isnull().any(axis=1)
        failed_records = null_mask.sum()
        pass_rate = ((len(self.df) - failed_records) / len(self.df)) * 100
        is_passed = failed_records == 0
        
        result = ValidationResult(
            check_name='Not Null Check',
            check_type='not_null',
            ticker=self.ticker,
            total_records=len(self.df),
            failed_records=failed_records,
            pass_rate=pass_rate,
            is_passed=is_passed,
            error_details=f"NULL values found in {columns}" if failed_records > 0 else None
        )
        
        self.validation_results.append(result)
        return result
    
    def validate_range(self, column: str, min_value: float = 0, max_value: Optional[float] = None) -> ValidationResult:
        """
        Valide que les valeurs sont dans une plage acceptable.
        
        Args:
            column: Colonne à valider
            min_value: Valeur minimale acceptable
            max_value: Valeur maximale acceptable
            
        Returns:
            ValidationResult
        """
        logger.info(f"Validating range for {column} ({self.ticker})")
        
        if column not in self.df.columns:
            return ValidationResult(
                check_name=f'Range Check ({column})',
                check_type='range',
                ticker=self.ticker,
                total_records=len(self.df),
                failed_records=len(self.df),
                pass_rate=0,
                is_passed=False,
                error_details=f"Column {column} not found"
            )
        
        out_of_range = (
            (self.df[column] < min_value) | 
            ((max_value is not None) & (self.df[column] > max_value))
        )
        
        failed_records = out_of_range.sum()
        pass_rate = ((len(self.df) - failed_records) / len(self.df)) * 100
        is_passed = failed_records == 0
        
        result = ValidationResult(
            check_name=f'Range Check ({column})',
            check_type='range',
            ticker=self.ticker,
            total_records=len(self.df),
            failed_records=failed_records,
            pass_rate=pass_rate,
            is_passed=is_passed,
            error_details=f"Values outside range [{min_value}, {max_value}]" if failed_records > 0 else None
        )
        
        self.validation_results.append(result)
        return result
    
    def validate_uniqueness(self, columns: List[str]) -> ValidationResult:
        """
        Valide qu'il n'y a pas de doublons sur les colonnes clés.
        
        Args:
            columns: Colonnes formant la clé unique
            
        Returns:
            ValidationResult
        """
        logger.info(f"Validating uniqueness for {self.ticker}")
        
        duplicates = self.df.duplicated(subset=columns, keep=False).sum()
        pass_rate = ((len(self.df) - duplicates) / len(self.df)) * 100 if len(self.df) > 0 else 0
        is_passed = duplicates == 0
        
        result = ValidationResult(
            check_name='Uniqueness Check',
            check_type='uniqueness',
            ticker=self.ticker,
            total_records=len(self.df),
            failed_records=duplicates,
            pass_rate=pass_rate,
            is_passed=is_passed,
            error_details=f"Duplicate records found on {columns}" if duplicates > 0 else None
        )
        
        self.validation_results.append(result)
        return result
    
    def validate_outliers(self, column: str, zscore_threshold: float = 3.0) -> ValidationResult:
        """
        Détecte les outliers en utilisant le Z-Score.
        
        Args:
            column: Colonne à analyser
            zscore_threshold: Seuil Z-Score (par défaut 3 sigma)
            
        Returns:
            ValidationResult
        """
        logger.info(f"Validating outliers for {column} ({self.ticker})")
        
        if column not in self.df.columns or len(self.df) == 0:
            return ValidationResult(
                check_name=f'Outlier Detection ({column})',
                check_type='outlier',
                ticker=self.ticker,
                total_records=len(self.df),
                failed_records=0,
                pass_rate=100,
                is_passed=True
            )
        
        # Calculate Z-Score
        numeric_data = pd.to_numeric(self.df[column], errors='coerce')
        zscores = np.abs(stats.zscore(numeric_data.dropna()))
        outliers = (zscores > zscore_threshold).sum()
        
        pass_rate = ((len(self.df) - outliers) / len(self.df)) * 100 if len(self.df) > 0 else 0
        is_passed = outliers == 0
        
        result = ValidationResult(
            check_name=f'Outlier Detection ({column})',
            check_type='outlier',
            ticker=self.ticker,
            total_records=len(self.df),
            failed_records=outliers,
            pass_rate=pass_rate,
            is_passed=is_passed,
            error_details=f"Outliers detected: {outliers} records exceed {zscore_threshold} sigma" if outliers > 0 else None
        )
        
        self.validation_results.append(result)
        return result
    
    def validate_freshness(self, max_days_old: int = 1) -> ValidationResult:
        """
        Valide que les données sont récentes (moins de max_days_old jours).
        
        Args:
            max_days_old: Nombre maximum de jours d'ancienneté
            
        Returns:
            ValidationResult
        """
        logger.info(f"Validating freshness for {self.ticker}")
        
        if 'date' not in self.df.columns:
            return ValidationResult(
                check_name='Freshness Check',
                check_type='freshness',
                ticker=self.ticker,
                total_records=len(self.df),
                failed_records=0,
                pass_rate=100,
                is_passed=True,
                error_details="Date column not found"
            )
        
        # Convert date to datetime if needed
        dates = pd.to_datetime(self.df['date'], errors='coerce')
        cutoff_date = datetime.now() - timedelta(days=max_days_old)
        
        stale_data = (dates < cutoff_date).sum()
        pass_rate = ((len(self.df) - stale_data) / len(self.df)) * 100 if len(self.df) > 0 else 0
        is_passed = stale_data == 0
        
        result = ValidationResult(
            check_name='Freshness Check',
            check_type='freshness',
            ticker=self.ticker,
            total_records=len(self.df),
            failed_records=stale_data,
            pass_rate=pass_rate,
            is_passed=is_passed,
            error_details=f"Stale data found: {stale_data} records older than {max_days_old} days" if stale_data > 0 else None
        )
        
        self.validation_results.append(result)
        return result
    
    def validate_gaps(self, date_column: str = 'date') -> ValidationResult:
        """
        Détecte les trous (gaps) entre les jours de bourse.
        
        Args:
            date_column: Colonne de date
            
        Returns:
            ValidationResult
        """
        logger.info(f"Validating gaps for {self.ticker}")
        
        if date_column not in self.df.columns or len(self.df) < 2:
            return ValidationResult(
                check_name='Gap Detection',
                check_type='gap',
                ticker=self.ticker,
                total_records=len(self.df),
                failed_records=0,
                pass_rate=100,
                is_passed=True
            )
        
        # Sort by date
        df_sorted = self.df.sort_values(date_column)
        dates = pd.to_datetime(df_sorted[date_column])
        
        # Calculate gaps (expected business days)
        date_diffs = dates.diff().dt.days
        gaps = (date_diffs > 3).sum()  # > 3 days = gap (weekends are 3 days)
        
        pass_rate = 100 if gaps == 0 else ((len(self.df) - gaps) / len(self.df)) * 100
        is_passed = gaps == 0
        
        result = ValidationResult(
            check_name='Gap Detection',
            check_type='gap',
            ticker=self.ticker,
            total_records=len(self.df),
            failed_records=gaps,
            pass_rate=pass_rate,
            is_passed=is_passed,
            error_details=f"Date gaps detected: {gaps} discontinuities" if gaps > 0 else None
        )
        
        self.validation_results.append(result)
        return result
    
    def validate_all(self) -> List[ValidationResult]:
        """
        Exécute tous les validateurs.
        
        Returns:
            Liste de tous les ValidationResults
        """
        logger.info(f"Running complete validation suite for {self.ticker}")
        
        # Schema validation
        self.validate_schema()
        
        # Not null validation
        if self.check_type == 'prices':
            self.validate_not_null(['ticker', 'date', 'close', 'volume'])
            self.validate_range('close', min_value=0)
            self.validate_range('volume', min_value=0)
            self.validate_uniqueness(['ticker', 'date'])
            self.validate_outliers('close')
            self.validate_gaps('date')
        else:  # fundamentals
            self.validate_not_null(['ticker', 'date'])
            self.validate_range('pe_ratio', min_value=0)
        
        # Freshness check
        self.validate_freshness(max_days_old=7)
        
        return self.validation_results
    
    def save_results_to_db(self) -> None:
        """
        Sauvegarde les résultats de validation dans la base de données.
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
            
            for result in self.validation_results:
                query = """
                INSERT INTO gold.data_quality_checks 
                (check_name, check_type, ticker, check_date, total_records, failed_records, pass_rate, error_details)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                cursor.execute(
                    query,
                    (
                        result.check_name,
                        result.check_type,
                        result.ticker,
                        result.check_date,
                        result.total_records,
                        result.failed_records,
                        result.pass_rate,
                        result.error_details
                    )
                )
            
            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"Saved {len(self.validation_results)} validation results to database")
            
        except Exception as e:
            logger.error(f"Failed to save validation results: {str(e)}")


def run_validation_suite(df: pd.DataFrame, ticker: str, data_type: str = 'prices') -> Tuple[bool, List[ValidationResult]]:
    """
    Fonction helper pour lancer une suite complète de validations.
    
    Args:
        df: DataFrame à valider
        ticker: Symbole boursier
        data_type: Type de données ('prices' ou 'fundamentals')
        
    Returns:
        Tuple (all_passed, results)
    """
    validator = DataValidator(df, ticker, data_type)
    results = validator.validate_all()
    validator.save_results_to_db()
    
    all_passed = all(r.is_passed for r in results)
    return all_passed, results