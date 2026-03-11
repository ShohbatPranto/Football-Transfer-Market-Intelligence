import pandas as pd
from sqlalchemy import create_engine
import os

def ingest_to_postgres():
    print("🔌 Initializing Database Connection Protocol...\n")
    
    # --- UPDATE YOUR CREDENTIALS HERE ---
    DB_USER = 'postgres'
    DB_PASSWORD = 'YOUR_POSTGRES_PASSWORD', # <-- Update before running
    DB_HOST = 'localhost'
    DB_PORT = '5432'
    DB_NAME = 'transfer_market_intelligence'
    
    # Create the connection string
    engine_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    
    try:
        # Create the database engine
        engine = create_engine(engine_url)
        print("✅ Database connection established.")
        
        # 1. Load the Analytics Base Table we built in Phase 1
        file_path = "Current_Market_Analytics_Base.csv"
        if not os.path.exists(file_path):
            print(f"❌ Error: Cannot find {file_path}. Run Phase 1 ETL first.")
            return
            
        print("📥 Loading flat file into memory...")
        df = pd.read_csv(file_path)
        
        # 2. Push to PostgreSQL
        # We name the table 'ml_feature_base'. 
        # if_exists='replace' ensures we can run this script multiple times safely.
        table_name = 'ml_feature_base'
        print(f"🏗️ Building table '{table_name}' and inserting {len(df)} rows...")
        
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        
        print(f"🏆 SUCCESS: Data ingested into PostgreSQL table '{table_name}'.")
        
    except Exception as e:
        print(f"❌ Database Error: {e}")

if __name__ == "__main__":
    ingest_to_postgres()