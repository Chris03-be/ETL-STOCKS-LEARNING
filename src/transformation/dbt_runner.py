#!/usr/bin/env python3
"""
DBT Runner Module
Exécute les modèles DBT pour la transformation des données
"""

import os
import logging
import subprocess
from pathlib import Path
from typing import Dict, Tuple, Optional
from datetime import datetime

import psycopg2
from dotenv import load_dotenv

load_dotenv()

# Database Configuration
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_USER = os.getenv('DB_USER', 'etl_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')
DB_NAME = os.getenv('DB_DATABASE', 'etl_stocks')

# DBT Configuration
DBT_PROJECT_DIR = Path('src/transformation/dbt_project')
DBT_PROFILES_DIR = Path.home() / '.dbt'

logger = logging.getLogger(__name__)


class DBTRunner:
    """Classe pour exécuter les modèles DBT"""
    
    def __init__(self, project_dir: Path = DBT_PROJECT_DIR):
        """
        Initialise le runner DBT.
        
        Args:
            project_dir: Répertoire du projet DBT
        """
        self.project_dir = project_dir
        self.profiles_dir = DBT_PROFILES_DIR
    
    def run_dbt_models(self, select: Optional[str] = None, threads: int = 4) -> Tuple[bool, Dict[str, any]]:
        """
        Exécute les modèles DBT.
        
        Args:
            select: Sélection des modèles à exécuter (ex: 'silver.*')
            threads: Nombre de threads parallèles
            
        Returns:
            Tuple (success, result_dict)
        """
        logger.info("Starting DBT models execution")
        
        try:
            cmd = [
                'dbt',
                'run',
                '--project-dir', str(self.project_dir),
                '--profiles-dir', str(self.profiles_dir),
                '--threads', str(threads)
            ]
            
            if select:
                cmd.extend(['--select', select])
            
            logger.info(f"Running command: {' '.join(cmd)}")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=600  # 10 minutes timeout
            )
            
            if result.returncode == 0:
                logger.info("DBT models executed successfully")
                return True, {
                    'status': 'SUCCESS',
                    'stdout': result.stdout,
                    'timestamp': datetime.now().isoformat()
                }
            else:
                logger.error(f"DBT execution failed: {result.stderr}")
                return False, {
                    'status': 'FAILED',
                    'error': result.stderr,
                    'timestamp': datetime.now().isoformat()
                }
        
        except subprocess.TimeoutExpired:
            logger.error("DBT execution timeout (10 minutes exceeded)")
            return False, {
                'status': 'TIMEOUT',
                'error': 'Execution exceeded 10 minutes',
                'timestamp': datetime.now().isoformat()
            }
        
        except Exception as e:
            logger.error(f"Error executing DBT: {str(e)}")
            return False, {
                'status': 'ERROR',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def run_dbt_tests(self) -> Tuple[bool, Dict[str, any]]:
        """
        Exécute les tests DBT.
        
        Returns:
            Tuple (all_passed, result_dict)
        """
        logger.info("Starting DBT tests")
        
        try:
            cmd = [
                'dbt',
                'test',
                '--project-dir', str(self.project_dir),
                '--profiles-dir', str(self.profiles_dir)
            ]
            
            logger.info(f"Running command: {' '.join(cmd)}")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300  # 5 minutes timeout
            )
            
            if result.returncode == 0:
                logger.info("All DBT tests passed")
                return True, {
                    'status': 'SUCCESS',
                    'stdout': result.stdout,
                    'timestamp': datetime.now().isoformat()
                }
            else:
                logger.error(f"Some DBT tests failed: {result.stderr}")
                return False, {
                    'status': 'FAILED',
                    'error': result.stderr,
                    'stdout': result.stdout,
                    'timestamp': datetime.now().isoformat()
                }
        
        except subprocess.TimeoutExpired:
            logger.error("DBT tests timeout (5 minutes exceeded)")
            return False, {
                'status': 'TIMEOUT',
                'error': 'Tests exceeded 5 minutes',
                'timestamp': datetime.now().isoformat()
            }
        
        except Exception as e:
            logger.error(f"Error executing DBT tests: {str(e)}")
            return False, {
                'status': 'ERROR',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def generate_dbt_docs(self) -> Tuple[bool, Dict[str, any]]:
        """
        Génère la documentation DBT.
        
        Returns:
            Tuple (success, result_dict)
        """
        logger.info("Generating DBT documentation")
        
        try:
            cmd = [
                'dbt',
                'docs',
                'generate',
                '--project-dir', str(self.project_dir),
                '--profiles-dir', str(self.profiles_dir)
            ]
            
            logger.info(f"Running command: {' '.join(cmd)}")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300
            )
            
            if result.returncode == 0:
                logger.info("DBT documentation generated successfully")
                return True, {
                    'status': 'SUCCESS',
                    'stdout': result.stdout,
                    'timestamp': datetime.now().isoformat()
                }
            else:
                logger.error(f"Failed to generate documentation: {result.stderr}")
                return False, {
                    'status': 'FAILED',
                    'error': result.stderr,
                    'timestamp': datetime.now().isoformat()
                }
        
        except Exception as e:
            logger.error(f"Error generating documentation: {str(e)}")
            return False, {
                'status': 'ERROR',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def run_full_pipeline(self) -> Tuple[bool, Dict[str, any]]:
        """
        Exécute le pipeline DBT complet (run + tests + docs).
        
        Returns:
            Tuple (success, result_dict)
        """
        logger.info("Starting full DBT pipeline")
        
        results = {
            'run': None,
            'tests': None,
            'docs': None,
            'overall_status': 'UNKNOWN',
            'timestamp': datetime.now().isoformat()
        }
        
        # Step 1: Run models
        run_success, run_result = self.run_dbt_models(threads=4)
        results['run'] = run_result
        
        if not run_success:
            logger.error("DBT run failed, skipping tests and docs")
            results['overall_status'] = 'FAILED'
            return False, results
        
        # Step 2: Run tests
        test_success, test_result = self.run_dbt_tests()
        results['tests'] = test_result
        
        if not test_success:
            logger.warning("Some DBT tests failed")
            results['overall_status'] = 'TESTS_FAILED'
        
        # Step 3: Generate docs
        docs_success, docs_result = self.generate_dbt_docs()
        results['docs'] = docs_result
        
        # Determine overall status
        if run_success and test_success:
            results['overall_status'] = 'SUCCESS'
            overall_success = True
        elif run_success and not test_success:
            results['overall_status'] = 'PARTIAL_SUCCESS'
            overall_success = True
        else:
            results['overall_status'] = 'FAILED'
            overall_success = False
        
        logger.info(f"DBT pipeline completed with status: {results['overall_status']}")
        return overall_success, results


def log_dbt_execution(result: Dict[str, any]) -> None:
    """
    Enregistre les résultats d'exécution DBT dans la base de données.
    
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
        (stage, status, message, executed_at)
        VALUES (%s, %s, %s, NOW())
        """
        
        overall_status = result.get('overall_status', 'UNKNOWN')
        
        cursor.execute(
            query,
            (
                'TRANSFORMATION',
                overall_status,
                str(result)
            )
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("DBT execution logged successfully")
        
    except Exception as e:
        logger.error(f"Failed to log DBT execution: {str(e)}")


if __name__ == "__main__":
    runner = DBTRunner()
    success, result = runner.run_full_pipeline()
    log_dbt_execution(result)
    exit(0 if success else 1)