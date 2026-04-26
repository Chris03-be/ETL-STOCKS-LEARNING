#!/usr/bin/env python3
"""
ML Layer - Predictive Model using PyCaret
Entraîne un modèle de régression pour prédire le prix à 7 jours
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Dict, Tuple, Optional, Any
from pathlib import Path

import pandas as pd
import numpy as np
import psycopg2
from dotenv import load_dotenv

try:
    from pycaret.regression import setup, compare_models, create_model, finalize_model, predict_model
    PYCARET_AVAILABLE = True
except ImportError:
    PYCARET_AVAILABLE = False
    logging.warning("PyCaret not installed. Install with: pip install pycaret")

try:
    import mlflow
    import mlflow.sklearn
    MLFLOW_AVAILABLE = True
except ImportError:
    MLFLOW_AVAILABLE = False
    logging.warning("MLflow not installed. Install with: pip install mlflow")

load_dotenv()

# Database Configuration
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_USER = os.getenv('DB_USER', 'etl_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')
DB_NAME = os.getenv('DB_DATABASE', 'etl_stocks')

# ML Configuration
ML_MODEL_DIR = Path('models')
ML_MODEL_DIR.mkdir(parents=True, exist_ok=True)

# Logging
LOG_DIR = Path('data/logs')
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / f"ml_model_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


class StockPricePredictor:
    """Classe pour entraîner et prédire les prix des actions"""
    
    def __init__(self, model_name: str = f"stock_price_model_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
        """
        Initialise le prédicteur.
        
        Args:
            model_name: Nom du modèle
        """
        if not PYCARET_AVAILABLE:
            raise ImportError("PyCaret is required. Install with: pip install pycaret")
        
        self.model_name = model_name
        self.model = None
        self.model_metrics = None
        self.training_date = datetime.now().date()
    
    def load_training_data(self, lookback_days: int = 365) -> Optional[pd.DataFrame]:
        """
        Charge les données d'entraînement depuis PostgreSQL.
        
        Args:
            lookback_days: Nombre de jours d'historique
            
        Returns:
            DataFrame avec les features et target
        """
        logger.info(f"Loading training data (lookback: {lookback_days} days)")
        
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=int(DB_PORT),
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME
            )
            
            # Query to fetch data with features
            query = f"""
            SELECT 
                ticker,
                date,
                close_price,
                volume,
                ma_50,
                ma_200,
                volatility_30d,
                rsi_14,
                pe_ratio,
                dividend_yield
            FROM gold.fct_market_analysis
            WHERE date >= CURRENT_DATE - INTERVAL '{lookback_days} days'
            AND close_price IS NOT NULL
            AND volume IS NOT NULL
            ORDER BY date DESC
            """
            
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            if df.empty:
                logger.warning("No training data available")
                return None
            
            logger.info(f"Loaded {len(df)} training records")
            return df
        
        except Exception as e:
            logger.error(f"Error loading training data: {str(e)}")
            return None
    
    def engineer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Crée et transforme les features pour le ML.
        
        Args:
            df: DataFrame brut
            
        Returns:
            DataFrame avec features engineerées
        """
        logger.info("Engineering features")
        
        try:
            # Copy dataframe
            df_features = df.copy()
            
            # Sort by date to create time series features
            df_features = df_features.sort_values('date')
            
            # Group by ticker for lag features
            for ticker in df_features['ticker'].unique():
                ticker_data = df_features[df_features['ticker'] == ticker].copy()
                
                # Lag features (previous days)
                for lag in [1, 5, 10]:
                    ticker_data[f'close_lag_{lag}'] = ticker_data['close_price'].shift(lag)
                    ticker_data[f'volume_lag_{lag}'] = ticker_data['volume'].shift(lag)
                
                # Rate of change
                ticker_data['roc_5d'] = ticker_data['close_price'].pct_change(5)
                ticker_data['roc_10d'] = ticker_data['close_price'].pct_change(10)
                
                # Price momentum
                ticker_data['momentum_5d'] = ticker_data['close_price'] - ticker_data['close_price'].shift(5)
                ticker_data['momentum_10d'] = ticker_data['close_price'] - ticker_data['close_price'].shift(10)
                
                # Update main dataframe
                df_features.loc[ticker_data.index] = ticker_data
            
            # Fill NaN values created by lag features
            df_features = df_features.fillna(method='bfill').fillna(method='ffill')
            
            # Create target variable (close price 7 days ahead)
            df_features['target'] = df_features.groupby('ticker')['close_price'].shift(-7)
            
            # Remove rows where target is NaN
            df_features = df_features.dropna(subset=['target'])
            
            logger.info(f"Created {len(df_features.columns)} features")
            return df_features
        
        except Exception as e:
            logger.error(f"Error engineering features: {str(e)}")
            return None
    
    def train_model(self, df: pd.DataFrame, test_size: float = 0.2) -> Tuple[bool, Dict[str, Any]]:
        """
        Entraîne le modèle de régression avec PyCaret.
        
        Args:
            df: DataFrame avec features et target
            test_size: Pourcentage de données de test
            
        Returns:
            Tuple (success, metrics)
        """
        logger.info("Starting model training")
        
        try:
            # Prepare data for PyCaret
            # Remove non-numeric and categorical columns except ticker
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            
            if 'target' not in numeric_cols:
                logger.error("Target variable 'target' not found in numeric columns")
                return False, {}
            
            # Select features (exclude ticker, date, and target)
            features = [col for col in numeric_cols if col not in ['target']]
            
            df_model = df[features + ['target']].copy()
            df_model = df_model.dropna()
            
            logger.info(f"Training with {len(features)} features")
            
            # Setup PyCaret
            exp_clf101 = setup(
                data=df_model,
                target='target',
                train_size=1 - test_size,
                test_size=test_size,
                normalize=True,
                transformation=True,
                polynomial_features=False,
                session_id=42,
                verbose=False,
                log_experiment=False
            )
            
            # Compare models and select best
            logger.info("Comparing regression models")
            best_model = compare_models(include=['rf', 'xgboost', 'gbr', 'lr'], n_select=1)
            
            # Create final model
            logger.info("Creating final model")
            self.model = create_model(best_model)
            
            # Finalize model (train on full data)
            self.model = finalize_model(self.model)
            
            # Get predictions on test set to calculate metrics
            predictions = predict_model(self.model, data=exp_clf101)
            
            # Calculate metrics
            from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
            
            mse = mean_squared_error(predictions['target'], predictions['prediction_label'])
            rmse = np.sqrt(mse)
            mae = mean_absolute_error(predictions['target'], predictions['prediction_label'])
            r2 = r2_score(predictions['target'], predictions['prediction_label'])
            
            self.model_metrics = {
                'mse': float(mse),
                'rmse': float(rmse),
                'mae': float(mae),
                'r2': float(r2),
                'model_type': str(type(self.model).__name__),
                'training_samples': len(df_model)
            }
            
            logger.info(f"Model trained successfully")
            logger.info(f"Metrics: RMSE={rmse:.4f}, MAE={mae:.4f}, R2={r2:.4f}")
            
            return True, self.model_metrics
        
        except Exception as e:
            logger.error(f"Error training model: {str(e)}")
            return False, {}
    
    def make_predictions(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        Fait des prédictions sur les données de marché actuelles.
        
        Args:
            df: DataFrame avec features
            
        Returns:
            DataFrame avec prédictions
        """
        logger.info("Making predictions")
        
        if self.model is None:
            logger.error("Model not trained. Call train_model first.")
            return None
        
        try:
            # Prepare data
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            features = [col for col in numeric_cols if col not in ['target']]
            
            df_pred = df[['ticker', 'date'] + features].copy()
            df_pred = df_pred.dropna()
            
            # Make predictions
            predictions = predict_model(self.model, data=df_pred[features])
            
            # Add predictions to original data
            df_pred['predicted_price'] = predictions['prediction_label']
            df_pred['prediction_date'] = datetime.now().date()
            df_pred['actual_date'] = df_pred['date'] + timedelta(days=7)
            
            # Calculate prediction direction
            df_pred['price_change'] = df_pred['predicted_price'] - df_pred['close_price']
            df_pred['predicted_direction'] = df_pred['price_change'].apply(
                lambda x: 'UP' if x > 0 else ('DOWN' if x < 0 else 'NEUTRAL')
            )
            
            logger.info(f"Generated {len(df_pred)} predictions")
            return df_pred
        
        except Exception as e:
            logger.error(f"Error making predictions: {str(e)}")
            return None
    
    def save_predictions_to_db(self, predictions_df: pd.DataFrame) -> bool:
        """
        Sauvegarde les prédictions dans la base de données.
        
        Args:
            predictions_df: DataFrame avec prédictions
            
        Returns:
            Succès ou non
        """
        logger.info("Saving predictions to database")
        
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=int(DB_PORT),
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME
            )
            cursor = conn.cursor()
            
            for _, row in predictions_df.iterrows():
                query = """
                INSERT INTO gold.ai_forecast 
                (ticker, forecast_date, actual_date, predicted_price, predicted_direction, 
                 model_version, model_name, training_date, inserted_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (ticker, forecast_date) 
                DO UPDATE SET 
                    predicted_price = EXCLUDED.predicted_price,
                    predicted_direction = EXCLUDED.predicted_direction,
                    updated_at = NOW()
                """
                
                cursor.execute(
                    query,
                    (
                        row['ticker'],
                        row['prediction_date'],
                        row['actual_date'],
                        float(row['predicted_price']),
                        row['predicted_direction'],
                        '1.0',
                        self.model_name,
                        self.training_date
                    )
                )
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Saved {len(predictions_df)} predictions to database")
            return True
        
        except Exception as e:
            logger.error(f"Error saving predictions: {str(e)}")
            return False
    
    def save_model(self) -> str:
        """
        Sauvegarde le modèle entraîné.
        
        Returns:
            Chemin du fichier du modèle
        """
        import pickle
        
        try:
            model_path = ML_MODEL_DIR / f"{self.model_name}.pkl"
            with open(model_path, 'wb') as f:
                pickle.dump(self.model, f)
            
            logger.info(f"Model saved to {model_path}")
            return str(model_path)
        
        except Exception as e:
            logger.error(f"Error saving model: {str(e)}")
            return None


def run_ml_pipeline() -> Dict[str, Any]:
    """
    Exécute le pipeline ML complet.
    
    Returns:
        Dict avec les résultats d'exécution
    """
    logger.info("="*60)
    logger.info("Starting ML Pipeline")
    logger.info("="*60)
    
    try:
        # Initialize predictor
        predictor = StockPricePredictor()
        
        # Load training data
        df_raw = predictor.load_training_data(lookback_days=365)
        if df_raw is None or df_raw.empty:
            logger.error("Failed to load training data")
            return {'status': 'FAILED', 'error': 'No training data available'}
        
        # Engineer features
        df_features = predictor.engineer_features(df_raw)
        if df_features is None or df_features.empty:
            logger.error("Failed to engineer features")
            return {'status': 'FAILED', 'error': 'Feature engineering failed'}
        
        # Train model
        train_success, metrics = predictor.train_model(df_features, test_size=0.2)
        if not train_success:
            logger.error("Failed to train model")
            return {'status': 'FAILED', 'error': 'Model training failed'}
        
        # Make predictions
        predictions = predictor.make_predictions(df_features)
        if predictions is None or predictions.empty:
            logger.error("Failed to make predictions")
            return {'status': 'FAILED', 'error': 'Prediction failed'}
        
        # Save predictions to DB
        save_success = predictor.save_predictions_to_db(predictions)
        if not save_success:
            logger.error("Failed to save predictions")
            return {'status': 'FAILED', 'error': 'Failed to save predictions'}
        
        # Save model
        model_path = predictor.save_model()
        
        logger.info("="*60)
        logger.info("ML Pipeline completed successfully")
        logger.info("="*60)
        
        return {
            'status': 'SUCCESS',
            'model_metrics': metrics,
            'predictions_count': len(predictions),
            'model_path': model_path,
            'timestamp': datetime.now().isoformat()
        }
    
    except Exception as e:
        logger.error(f"ML Pipeline failed: {str(e)}")
        return {
            'status': 'FAILED',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }


def log_ml_execution(result: Dict[str, Any]) -> None:
    """
    Enregistre les résultats du ML dans la base de données.
    
    Args:
        result: Résultats d'exécution
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
                'ML_TRAINING',
                result.get('status', 'UNKNOWN'),
                str(result),
                result.get('predictions_count', 0)
            )
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("ML execution logged successfully")
    
    except Exception as e:
        logger.error(f"Failed to log ML execution: {str(e)}")


if __name__ == "__main__":
    result = run_ml_pipeline()
    log_ml_execution(result)
    exit(0 if result['status'] == 'SUCCESS' else 1)