import dlt
import yfinance as yf
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

# 1. Charger les variables d'environnement du fichier .env
load_dotenv()

# 2. Injection des identifiants Postgres pour DLT
os.environ["DESTINATION__POSTGRES__CREDENTIALS__DATABASE"] = os.getenv("DB_NAME")
os.environ["DESTINATION__POSTGRES__CREDENTIALS__PASSWORD"] = os.getenv("DB_PASS")
os.environ["DESTINATION__POSTGRES__CREDENTIALS__USERNAME"] = os.getenv("DB_USER")
os.environ["DESTINATION__POSTGRES__CREDENTIALS__HOST"] = os.getenv("DB_HOST")
os.environ["DESTINATION__POSTGRES__CREDENTIALS__PORT"] = os.getenv("DB_PORT")

# 3. Liste des Tickers
US_STOCKS = ['INTC', 'CI', 'F', 'ADBE', 'MO', 'CMCSA']
EU_STOCKS = ['OR.PA', 'SIE.DE', 'NESN.SW', 'ENI.MI']
ALL_TICKERS = US_STOCKS + EU_STOCKS

@dlt.resource(name="raw_stock_prices", write_disposition="append")
def fetch_market_data():
    for ticker in ALL_TICKERS:
        print(f" Extraction des données pour {ticker}...")
        stock = yf.Ticker(ticker)
        
        # Historique de 5 jours pour assurer la continuité (Gap Detection)
        df = stock.history(period="5d")
        
        if not df.empty:
            df = df.reset_index()
            
            # Standardisation : colonnes en minuscules sans espaces pour Postgres
            df.columns = [c.lower().replace(' ', '_') for c in df.columns]
            
            # Suppression du fuseau horaire pour éviter les erreurs d'insertion SQL
            if 'date' in df.columns:
                df['date'] = df['date'].dt.tz_localize(None)
            
            # Ajout des métadonnées
            df['ticker'] = ticker
            df['market_region'] = 'US' if ticker in US_STOCKS else 'EU'
            df['extracted_at'] = datetime.now()
            
            yield df.to_dict(orient="records")

if __name__ == "__main__":
    # 4. Configuration et lancement du pipeline DLT
    pipeline = dlt.pipeline(
        pipeline_name="yfinance_to_postgres",
        destination="postgres",
        dataset_name="bronze" # Les données iront dans le schéma bronze
    )
    
    print(" Lancement du pipeline d'ingestion...")
    load_info = pipeline.run(fetch_market_data())
    print(" Ingestion terminée avec succès !")
    print(load_info)
