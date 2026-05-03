C'est une vision d'architecture absolument fantastique pour une V2 ! Le document que tu as généré montre que tu as parfaitement compris où ce projet peut aller (avec Alpha Vantage, le scoring complexe, les MACD, etc.). 

Cependant, comme pour le `README.md` général, si tu mets ce fichier `README_ARCHITECTURE.md` sur ton GitHub aujourd'hui, un recruteur technique va chercher les dossiers de "Scoring" ou l'intégration "Alpha Vantage" dans ton code et ne les trouvera pas. 

Il faut que ton architecture soit le **reflet exact et honnête** de ton code actuel (V1), tout en gardant tes superbes idées pour la section "Roadmap/Évolutions".

Voici ton fichier `README_ARCHITECTURE.md` réécrit pour coller **à 100% à la réalité de ton projet actuel**, tout en gardant ce format professionnel et visuel que tu as proposé :

***

```markdown
# 🏗️ Architecture du Système - ETL-STOCKS-LEARNING

Ce document détaille l'architecture technique, les flux de données et les schémas de base de données de la version actuelle (V1) du projet.

## 🗺️ Vue d'Ensemble du Système
```text
┌─────────────────────────────────────────────────────────────────┐
│           ETL-STOCKS-LEARNING : ARCHITECTURE MEDALLION          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│ LAYER 1: INGESTION (API + DLT)                                  │
│ ├─ Yahoo Finance API (Données OHLCV)                            │
│ └─ DLT (Data Load Tool)                                         │
│      └─ Extraction → Typage dynamique → Chargement Bronze       │
│                                                                 │
│              ↓                                                  │
│                                                                 │
│ LAYER 2: STORAGE (PostgreSQL)                                   │
│                                                                 │
│ BRONZE SCHEMA (Données Brutes)                                  │
│ └─ raw_stock_prices              (Données OHLCV historiques)    │
│                                                                 │
│              ↓                                                  │
│                                                                 │
│ SILVER SCHEMA (Données Nettoyées)                               │
│ └─ stg_stock_prices              (Dédoublonné, typé, casté)     │
│                                                                 │
│              ↓                                                  │
│                                                                 │
│ GOLD SCHEMA (Analytique / BI)                                   │
│ └─ gold_stock_indicators         (Moyennes mobiles, variations) │
│                                                                 │
│              ↓                                                  │
│                                                                 │
│ LAYER 3: TRANSFORMATION (DBT)                                   │
│ ├─ Modèles SQL (Staging & Marts)                                │
│ ├─ Génération du Lineage Graph (DAG)                            │
│ └─ Matérialisation en Vues (Views)                              │
│                                                                 │
│              ↓                                                  │
│                                                                 │
│ LAYER 4: ORCHESTRATION (APScheduler)                            │
│ ├─ Exécution planifiée (Script Python autonome)                 │
│ ├─ Séquence stricte : Ingestion DLT ➔ Attente ➔ Run dbt         │
│ └─ Gestion des erreurs et logs de base                          │
│                                                                 │
│              ↓                                                  │
│                                                                 │
│ LAYER 5: VISUALIZATION (Power BI - Prêt à l'emploi)             │
│ ├─ Analyse des volumes échangés                                 │
│ └─ Suivi de la volatilité et des tendances (Moyenne 7j)         │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🔄 Flux de Données Détaillé

### 1. Couche Ingestion (src/ingestion/)

**Fichier Principal :** `dlt_pipeline.py`

**Responsabilités :**
*   Extraction des données historiques via `yfinance`.
*   Gestion autonome du schéma de base de données (Schema Evolution) via DLT.
*   Chargement incrémental dans la couche Bronze PostgreSQL.

---

### 2. Couche Stockage (PostgreSQL)

**Structure des Schémas :**
```sql
-- Schema généré par DLT (Couche Bronze)
CREATE TABLE bronze.stock_prices (
    date TIMESTAMP,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume BIGINT,
    symbol VARCHAR,
    _dlt_load_id VARCHAR, -- Métadonnée DLT
    _dlt_id VARCHAR       -- Métadonnée DLT
);

-- Schema généré par dbt (Couche Silver)
CREATE VIEW silver.stg_stock_prices AS
    SELECT symbol, date, open, high, low, close, volume 
    FROM bronze.stock_prices;

-- Schema généré par dbt (Couche Gold)
CREATE VIEW silver.gold_stock_indicators AS
    -- Contient les calculs analytiques (Variation, Moyenne Mobile)
```

---

### 3. Couche Transformation (dbt)

**Structure du Projet :**
```text
dbt_project/models/
├── staging/
│   └── stg_stock_prices.sql       (Nettoyage et sélection Silver)
│
└── gold/
    └── gold_stock_indicators.sql  (Calculs analytiques métier)
```

**Exemple de Modèle (Couche Gold) :**
Utilisation des fonctions de fenêtrage SQL pour calculer les indicateurs de trading sans perdre la granularité temporelle.
```sql
with silver_data as (
    select * from {{ ref('stg_stock_prices') }}
),

analytical_indicators as (
    select
        symbol,
        date,
        close as current_price,
        volume,
        
        -- Variation journalière en pourcentage
        round(((close - open) / open * 100)::numeric, 2) as daily_variation_pct,

        -- Moyenne mobile sur 7 jours via Window Function
        round(avg(close) over (
            partition by symbol 
            order by date 
            rows between 6 preceding and current row
        )::numeric, 2) as moving_avg_7d

    from silver_data
)

select * from analytical_indicators
```

---

### 4. Couche Orchestration (APScheduler)

**Fichier Principal :** `run_scheduler.py`

**Stratégie :**
Le pipeline utilise `BlockingScheduler` pour s'exécuter de manière autonome, gérant le processus ETL complet de manière séquentielle.
```python
# Exemple de logique d'orchestration
def job_pipeline_etl():
    # 1. Run DLT (Ingestion vers Bronze)
    subprocess.run(["python", "src/ingestion/dlt_pipeline.py"])
    
    # 2. Run dbt (Transformation vers Silver puis Gold)
    subprocess.run(["dbt", "run"], cwd="src/transformation/dbt_project")
```

---

## 🛠️ Stack Technologique (V1)

*   **Ingestion :** Python 3.11, `yfinance`, `dlt[postgres]`
*   **Stockage :** PostgreSQL 13+
*   **Transformation :** `dbt-core`, `dbt-postgres`
*   **Orchestration :** `apscheduler`, `subprocess`
*   **Environnement :** `python-dotenv`

---

## 🚀 Évolutions Futures (Roadmap V2)

L'architecture a été conçue de manière modulaire (Medallion) pour permettre l'intégration future des éléments suivants :

1.  **Enrichissement des données sources :** Ajout des API *Alpha Vantage* ou *IEX Cloud* pour récupérer les données fondamentales (P/E ratio, dividendes).
2.  **Couche Gold Avancée :** Création d'un système de *Scoring d'Investissement* (Note de A à F basée sur des algorithmes financiers croisés).
3.  **Machine Learning :** Implémentation de `PyCaret` pour des prédictions de prix à 7 jours.
4.  **Tests dbt Stricts :** Ajout du fichier `schema.yml` pour valider l'intégrité de la donnée (tests `not_null`, `unique`, ranges de valeurs).
5.  **Dashboarding :** Connexion directe d'un rapport Power BI sur les tables Gold pour un suivi analytique en temps réel.
```
