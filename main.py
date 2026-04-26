#!/usr/bin/env python3
"""
ETL-STOCKS-PREDICT - Main Entry Point
Lance le pipeline ETL complet: Ingestion -> Transformation -> ML
"""

import os
import sys
import logging
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from orchestration.scheduler import PipelineOrchestrator

# Configure logging
LOG_DIR = Path('data/logs')
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / f"main_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def main():
    """
    Main function to run the pipeline.
    """
    logger.info("""
    ╔════════════════════════════════════════════════════════════════╗
    ║    ETL-STOCKS-PREDICT: Complete Data Pipeline                 ║
    ║    Ingestion → Transformation → Machine Learning              ║
    ╚════════════════════════════════════════════════════════════════╝
    """)
    
    import argparse
    
    parser = argparse.ArgumentParser(
        description='ETL-STOCKS-PREDICT Pipeline'
    )
    parser.add_argument(
        '--schedule',
        type=str,
        default=None,
        help='Schedule daily runs at specified time (HH:MM format, e.g., 22:00)'
    )
    parser.add_argument(
        '--once',
        action='store_true',
        help='Run pipeline once and exit'
    )
    
    args = parser.parse_args()
    
    try:
        orchestrator = PipelineOrchestrator()
        
        if args.once or (args.schedule is None):
            # Run once
            logger.info("Executing pipeline ONCE...")
            result = orchestrator.run_complete_pipeline()
            
            logger.info(f"\nFinal Status: {result['overall_status']}")
            logger.info(f"Duration: {result['total_duration_seconds']:.2f}s")
            
            exit(0 if result['overall_status'] in ['SUCCESS', 'PARTIAL_SUCCESS_AT_ML'] else 1)
        
        else:
            # Schedule daily
            logger.info(f"Starting scheduler - Daily runs at {args.schedule} UTC")
            orchestrator.schedule_daily_run(args.schedule)
    
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        exit(1)


if __name__ == "__main__":
    main()