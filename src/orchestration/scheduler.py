#!/usr/bin/env python3
"""
Orchestration Module - APScheduler
Orchestre l'exécution quotidienne du pipeline complet (DLT -> DBT -> ML)
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any
import traceback

import schedule
import time
from dotenv import load_dotenv
import psycopg2

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from ingestion.dlt_pipeline import run_dlt_pipeline, log_pipeline_execution as log_dlt_execution
from transformation.dbt_runner import DBTRunner, log_dbt_execution
from ml_layer.predictive_model import run_ml_pipeline, log_ml_execution

load_dotenv()

# Database Configuration
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_USER = os.getenv('DB_USER', 'etl_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')
DB_NAME = os.getenv('DB_DATABASE', 'etl_stocks')

# Logging Configuration
LOG_DIR = Path('data/logs')
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / f"orchestrator_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Classe pour orchestrer l'exécution du pipeline ETL + ML"""
    
    def __init__(self):
        """
        Initialise l'orchestrateur.
        """
        self.dbt_runner = DBTRunner()
        self.pipeline_execution_times = {}
    
    def run_complete_pipeline(self) -> Dict[str, Any]:
        """
        Exécute le pipeline complet: DLT -> DBT -> ML
        
        Returns:
            Dict avec les résultats de tous les stages
        """
        logger.info("="*80)
        logger.info("STARTING COMPLETE PIPELINE EXECUTION")
        logger.info(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("="*80)
        
        execution_results = {
            'ingestion': None,
            'transformation': None,
            'ml': None,
            'overall_status': 'UNKNOWN',
            'started_at': datetime.now().isoformat(),
            'completed_at': None,
            'total_duration_seconds': 0
        }
        
        start_time = time.time()
        
        try:
            # STAGE 1: Ingestion (DLT)
            logger.info("\n" + "="*80)
            logger.info("STAGE 1: DATA INGESTION (DLT)")
            logger.info("="*80)
            
            dlt_start = time.time()
            dlt_result = run_dlt_pipeline()
            dlt_duration = time.time() - dlt_start
            
            execution_results['ingestion'] = {
                **dlt_result,
                'duration_seconds': dlt_duration
            }
            self.pipeline_execution_times['ingestion'] = dlt_duration
            
            # Log DLT execution
            log_dlt_execution(dlt_result)
            
            if dlt_result.get('status') != 'SUCCESS':
                logger.error("DLT pipeline failed. Aborting remaining stages.")
                execution_results['overall_status'] = 'FAILED_AT_INGESTION'
                return execution_results
            
            logger.info(f"\n✓ Ingestion completed successfully (Duration: {dlt_duration:.2f}s)")
            
            # STAGE 2: Transformation (DBT)
            logger.info("\n" + "="*80)
            logger.info("STAGE 2: DATA TRANSFORMATION (DBT)")
            logger.info("="*80)
            
            dbt_start = time.time()
            dbt_success, dbt_result = self.dbt_runner.run_full_pipeline()
            dbt_duration = time.time() - dbt_start
            
            execution_results['transformation'] = {
                **dbt_result,
                'duration_seconds': dbt_duration,
                'success': dbt_success
            }
            self.pipeline_execution_times['transformation'] = dbt_duration
            
            # Log DBT execution
            log_dbt_execution(dbt_result)
            
            if not dbt_success:
                logger.error("DBT pipeline failed. Aborting ML stage.")
                execution_results['overall_status'] = 'FAILED_AT_TRANSFORMATION'
                return execution_results
            
            logger.info(f"\n✓ Transformation completed successfully (Duration: {dbt_duration:.2f}s)")
            
            # STAGE 3: Machine Learning
            logger.info("\n" + "="*80)
            logger.info("STAGE 3: MACHINE LEARNING (MODEL TRAINING & PREDICTION)")
            logger.info("="*80)
            
            ml_start = time.time()
            ml_result = run_ml_pipeline()
            ml_duration = time.time() - ml_start
            
            execution_results['ml'] = {
                **ml_result,
                'duration_seconds': ml_duration
            }
            self.pipeline_execution_times['ml'] = ml_duration
            
            # Log ML execution
            log_ml_execution(ml_result)
            
            if ml_result.get('status') != 'SUCCESS':
                logger.warning("ML pipeline failed, but previous stages succeeded.")
                execution_results['overall_status'] = 'PARTIAL_SUCCESS_AT_ML'
            else:
                logger.info(f"\n✓ ML Training completed successfully (Duration: {ml_duration:.2f}s)")
                execution_results['overall_status'] = 'SUCCESS'
            
        except Exception as e:
            logger.error(f"\nUnexpected error during pipeline execution: {str(e)}")
            logger.error(traceback.format_exc())
            execution_results['overall_status'] = 'FAILED_WITH_ERROR'
            execution_results['error'] = str(e)
        
        finally:
            # Calculate total duration
            total_duration = time.time() - start_time
            execution_results['total_duration_seconds'] = total_duration
            execution_results['completed_at'] = datetime.now().isoformat()
            
            # Log final summary
            logger.info("\n" + "="*80)
            logger.info("PIPELINE EXECUTION SUMMARY")
            logger.info("="*80)
            logger.info(f"Overall Status: {execution_results['overall_status']}")
            logger.info(f"Total Duration: {total_duration:.2f}s")
            logger.info(f"  - Ingestion: {self.pipeline_execution_times.get('ingestion', 0):.2f}s")
            logger.info(f"  - Transformation: {self.pipeline_execution_times.get('transformation', 0):.2f}s")
            logger.info(f"  - ML Training: {self.pipeline_execution_times.get('ml', 0):.2f}s")
            logger.info("="*80)
            
            # Log to database
            self._log_execution_summary(execution_results)
        
        return execution_results
    
    def _log_execution_summary(self, results: Dict[str, Any]) -> None:
        """
        Enregistre le résumé d'exécution dans la base de données.
        
        Args:
            results: Résultats d'exécution du pipeline
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
            (stage, status, message, execution_time_seconds, executed_at)
            VALUES (%s, %s, %s, %s, NOW())
            """
            
            cursor.execute(
                query,
                (
                    'ORCHESTRATION',
                    results.get('overall_status', 'UNKNOWN'),
                    str(results),
                    results.get('total_duration_seconds', 0)
                )
            )
            
            conn.commit()
            cursor.close()
            conn.close()
            logger.info("Pipeline execution summary logged to database")
        
        except Exception as e:
            logger.error(f"Failed to log execution summary: {str(e)}")
    
    def schedule_daily_run(self, scheduled_time: str = "22:00") -> None:
        """
        Programme l'exécution quotidienne du pipeline.
        
        Args:
            scheduled_time: Heure d'exécution (format HH:MM, ex: '22:00')
        """
        logger.info(f"Scheduling daily pipeline execution at {scheduled_time} UTC")
        
        schedule.every().day.at(scheduled_time).do(self.run_complete_pipeline)
        
        logger.info("Scheduler started. Press Ctrl+C to stop.")
        logger.info(f"Next execution: {schedule.idle_seconds():.0f}s")
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        
        except KeyboardInterrupt:
            logger.info("\nScheduler stopped by user")


def main():
    """
    Point d'entrée principal du script d'orchestration.
    """
    import argparse
    
    parser = argparse.ArgumentParser(
        description='ETL-STOCKS-PREDICT Pipeline Orchestrator'
    )
    parser.add_argument(
        '--once',
        action='store_true',
        help='Run pipeline once and exit'
    )
    parser.add_argument(
        '--schedule',
        type=str,
        default='22:00',
        help='Time to schedule daily runs (format HH:MM, default: 22:00)'
    )
    
    args = parser.parse_args()
    
    orchestrator = PipelineOrchestrator()
    
    if args.once:
        logger.info("Running pipeline once (no scheduling)")
        result = orchestrator.run_complete_pipeline()
        exit(0 if result['overall_status'] == 'SUCCESS' else 1)
    else:
        logger.info("Starting scheduler mode")
        orchestrator.schedule_daily_run(args.schedule)


if __name__ == "__main__":
    main()