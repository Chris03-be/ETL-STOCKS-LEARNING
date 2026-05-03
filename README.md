


# 📈 ETL Stock Market Pipeline (Architecture Medallion & Orchestration)

Ce projet est un pipeline d'ingestion et de transformation de données financières (actions boursières) automatisé de bout en bout. Il implémente les meilleures pratiques du Data Engineering, notamment l'Architecture Medallion (Bronze, Silver, Gold) et un ordonnanceur de tâches.

## 🏗️ Architecture du Système

Le système est orchestré par **APScheduler** et divisé en trois couches de données :

*   ⏱️ **Orchestration (APScheduler) :** Planification automatisée du pipeline (ex: exécution post-fermeture des marchés), gérant les dépendances entre l'ingestion et la transformation.
*   🥉 **Couche Bronze (Ingestion) avec DLT (Data Load Tool) :** Extraction des données boursières brutes (Yahoo Finance) via API/Python et chargement automatisé et typé dans PostgreSQL.
*   🥈 **Couche Silver (Staging) avec dbt (Data Build Tool) :** Nettoyage, standardisation du nommage (ex: `ticker` vers `symbol`), typage strict et filtrage des valeurs nulles.
*   🥇 **Couche Gold (Marts/Analytique) avec dbt :** Calcul d'indicateurs financiers complexes prêts pour la BI (Variation journalière en pourcentage, Moyenne mobile sur 7 jours via fonctions de fenêtrage SQL).

## 🛠️ Stack Technologique
*   **Langage :** Python 3.11+
*   **Orchestration :** APScheduler
*   **Extraction & Chargement :** DLT (Data Load Tool)
*   **Transformation :** dbt (Data Build Tool) / SQL
*   **Base de données :** PostgreSQL

## 🚀 Installation & Exécution

### 1. Prérequis
*   Python 3.x installé
*   PostgreSQL installé et en cours d'exécution

### 2. Configuration
Créez un environnement virtuel et installez les dépendances :
```bash
python -m venv .venv
source .venv/Scripts/activate  # Sur Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Configurez les variables d'environnement. Créez un fichier `.env` à la racine :
```env
DESTINATION__POSTGRES__CREDENTIALS="postgresql://votre_user:votre_mdp@localhost:5432/etl_stocks"
```

Configurez le profil dbt en créant le fichier `~/.dbt/profiles.yml` :
```yaml
etl_stocks:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: votre_user
      pass: 'votre_mdp'
      port: 5432
      dbname: etl_stocks
      schema: silver
      threads: 1
```

### 3. Exécution du Pipeline

Vous pouvez lancer le pipeline de deux manières :

**Option A : Mode Orchestrateur Automatisé (Recommandé)**
Lance le planificateur de tâches en arrière-plan :
```bash
python run_scheduler.py
```

**Option B : Mode Manuel (Développement)**
```bash
# Étape 1 : Ingestion DLT
python src/ingestion/dlt_pipeline.py

# Étape 2 : Transformation dbt
cd src/transformation/dbt_project
dbt run
```

### 📊 Documentation des Données
Le projet inclut un catalogue de données interactif et un Lineage Graph générés automatiquement :
```bash
cd src/transformation/dbt_project
dbt docs generate
dbt docs serve
```
