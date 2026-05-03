from apscheduler.schedulers.blocking import BlockingScheduler
import subprocess
import datetime
import sys

def job_pipeline_etl():
    """La fonction qui exécute les deux étapes dans le bon ordre"""
    heure_actuelle = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n[ {heure_actuelle} ] 🚀 DÉMARRAGE DU PIPELINE ETL")
    
    try:
        # Étape 1 : Bronze (DLT)
        print("1️⃣ Extraction (DLT)...")
        # On exécute le script Python d'ingestion
        subprocess.run(["python", "src/ingestion/dlt_pipeline.py"], check=True)
        
        # Étape 2 : Silver & Gold (dbt)
        print("2️⃣ Transformation (dbt)...")
        # On se déplace dans le dossier dbt pour lancer la commande
        subprocess.run(["dbt", "run"], cwd="src/transformation/dbt_project", check=True)
        
        heure_fin = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[ {heure_fin} ] ✅ PIPELINE TERMINÉ AVEC SUCCÈS\n")
        
    except subprocess.CalledProcessError as e:
        print(f"\n❌ ERREUR CRITIQUE dans le pipeline : {e}")

if __name__ == "__main__":
    scheduler = BlockingScheduler()
    
    # ---------------------------------------------------------
    # CONFIGURATION DE LA PLANIFICATION
    # ---------------------------------------------------------
    
    # Option 1 (Pour tester tout de suite) : Lancement toutes les 2 minutes
    # scheduler.add_job(job_pipeline_etl, 'interval', minutes=2)
    
    # Option 2 (Pour la production) : Lancement du Lundi au Vendredi à 23h00
    scheduler.add_job(job_pipeline_etl, 'cron', day_of_week='mon-fri', hour=23, minute=0)
    
    # ---------------------------------------------------------
    
    print("🏭 Orchestrateur Démarré.")
    print("En attente de la prochaine exécution... (Appuyez sur Ctrl+C pour quitter)")
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("\n🛑 Arrêt de l'orchestrateur.")