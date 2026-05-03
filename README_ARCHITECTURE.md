# 🚀 ETL-STOCKS-LEARNING - Architecture Medallion & Orchestration

## 📋 Présentation

Un pipeline **Data Engineering de bout en bout** (ETL) conçu pour l'ingestion, le nettoyage et l'analyse de données boursières, automatisé quotidiennement.

```text
Yahoo Finance API
        ↓
   DLT Pipeline (Python)
        ↓
PostgreSQL Couche Bronze (Données Brutes)
        ↓
   DBT Transformation (Nettoyage)
        ↓
PostgreSQL Couche Silver (Données Standardisées)
        ↓
   DBT Transformation (Analytique)
        ↓
PostgreSQL Couche Gold (Indicateurs Métier)
        ↓
   APScheduler Orchestration
        ↓
   Prêt pour Power BI / Tableaux de bord
```

---

## 📊 Schéma de la Base de Données

### Couche Bronze (Raw Data)
```text
bronze.stock_prices
├─ date, open, high, low, close, volume, symbol
```

### Couche Silver (Cleaned Data)
```text
silver.stg_stock_prices
├─ Données dédoublonnées, typées proprement (numeric, date)
├─ Renommage standardisé (ex: ticker -> symbol)
```

### Couche Gold (Analytics) ⭐
```text
silver.gold_stock_indicators 
├─ symbol, date, current_price, volume
├─ daily_variation_pct (Variation journalière en %)
├─ moving_avg_7d (Moyenne mobile sur 7 jours)
```

---

## 🔄 Étapes du Pipeline

### Étape 1 : Ingestion (DLT)
**Fichier** : `src/ingestion/dlt_pipeline.py`

✅ Extraction des prix historiques via l'API Yahoo Finance  
✅ Gestion automatique des schémas de base de données (Schema Evolution)  
✅ Chargement dans la couche Bronze PostgreSQL  

### Étape 2 : Transformation (dbt)
**Dossier** : `src/transformation/dbt_project/`

✅ **Bronze → Silver** : Nettoyage, typage strict et standardisation  
✅ **Silver → Gold** : Calcul d'indicateurs financiers complexes via fonctions de fenêtrage SQL (Window Functions)  
✅ Génération automatique du catalogue de données et du graphe de dépendance (Lineage Graph)  

### Étape 3 : Orchestration (APScheduler)
**Fichier** : `src/run_scheduler.py`

✅ Exécution séquentielle automatisée (DLT d'abord, puis dbt)  
✅ Gestion des erreurs de processus  
✅ Planification flexible (ex: exécution post-fermeture des marchés)  

---

## 🎯 Fonctionnalités Clés

*   **Architecture Medallion :** Séparation stricte entre les données brutes, nettoyées et analytiques.
*   **Infrastructure as Code (Data) :** Utilisation de dbt pour versionner et tester les transformations SQL.
*   **Sécurité :** Gestion des identifiants de base de données via variables d'environnement (`.env`).
*   **Documentation Interactive :** Auto-génération du catalogue de données via `dbt docs`.

---

## 🚀 Démarrage Rapide

### 1. Installation
```bash
# Créer et activer l'environnement virtuel
python -m venv .venv
source .venv/Scripts/activate  # Sur Windows : .venv\Scripts\activate

# Installer les dépendances
pip install -r requirements.txt
```

### 2. Configuration
Créez un fichier `.env` à la racine :
```env
DESTINATION__POSTGRES__CREDENTIALS="postgresql://votre_user:votre_mdp@localhost:5432/etl_stocks"
```

Configurez votre profil dbt dans `~/.dbt/profiles.yml` :
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

### 3. Exécution

**Mode Automatique (Recommandé) :**
```bash
python run_scheduler.py
```

**Mode Manuel :**
```bash
python src/ingestion/dlt_pipeline.py
cd src/transformation/dbt_project
dbt run
```

---

## 📊 Intégration Power BI

La table `gold_stock_indicators` est optimisée pour la Business Intelligence. Voici quelques mesures DAX que vous pouvez brancher directement sur cette table :

1. **Volume Total Échangé**
   ```dax
   = SUM('gold_stock_indicators'[volume])
   ```

2. **Variation Moyenne Journalière**
   ```dax
   = AVERAGE('gold_stock_indicators'[daily_variation_pct])
   ```

---

## 📋 Structure du Projet
```text
ETL-STOCKS-LEARNING/
├── .env                           # Variables d'environnement
├── README.md                      # Documentation
├── requirements.txt               # Dépendances Python
├── run_scheduler.py               # Orchestrateur APScheduler
└── src/
    ├── ingestion/
    │   └── dlt_pipeline.py        # Pipeline d'extraction DLT
    └── transformation/
        └── dbt_project/
            ├── dbt_project.yml    # Configuration dbt
            └── models/
                ├── staging/       # Modèles Silver (Nettoyage)
                │   └── stg_stock_prices.sql
                └── gold/          # Modèles Gold (Analytique)
                    └── gold_stock_indicators.sql
```

---

## 📚 Prochaines Évolutions (Roadmap)
*   [ ] Intégration de tests de qualité de données stricts (dbt tests).
*   [ ] Ajout d'une couche Machine Learning (PyCaret) pour la prédiction de prix à 7 jours.
*   [ ] Déploiement de l'orchestrateur sur le Cloud.
```

***

### 💡 Le Mot du Data Engineer
Ce README correspond à 100% à la réalité de ton code d'aujourd'hui. Remplace le contenu de ton `README.md` actuel par celui-ci, refais un commit (`git add README.md`, `git commit -m "docs: refonte du README à l'image de la V1"`, `git push`), et ton portfolio sera d'une précision chirurgicale !