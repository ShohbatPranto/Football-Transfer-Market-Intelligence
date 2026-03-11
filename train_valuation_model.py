import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score
from datetime import datetime
import logging

# Configure professional logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database Configuration (Best practice: use environment variables in the future)
DB_CONFIG = {
    'user': 'postgres',
    'password': 'YOUR_POSTGRES_PASSWORD', # <-- Update before running
    'host': 'localhost',
    'port': '5432',
    'dbname': 'transfer_market_intelligence'
}

def get_database_engine():
    """Initializes and returns the SQLAlchemy database engine."""
    url = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    return create_engine(url)

def extract_data(engine) -> pd.DataFrame:
    """Extracts the base feature matrix from the data warehouse."""
    logging.info("Extracting feature matrix from PostgreSQL...")
    return pd.read_sql("SELECT * FROM ml_feature_base;", engine)

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Applies domain-specific feature engineering for football asset valuation."""
    logging.info("Engineering advanced volume and consistency features...")
    
    # 1. Biological Age Calculation
    df['date_of_birth'] = pd.to_datetime(df['date_of_birth'])
    df['age'] = (datetime.now() - df['date_of_birth']).dt.days / 365.25
    
    # 2. Output Metrics (Rate & Volume)
    df['goal_contributions_p90'] = df['goals_per_90'] + df['assists_per_90']
    df['total_goal_contributions'] = df['goals'] + df['assists']
    df['proven_output_index'] = df['goal_contributions_p90'] * df['total_goal_contributions'] * (df['minutes_played'] / 90)
    
    # 3. Club Muscle & Prestige Aggregation
    club_metrics = df.groupby('club_name').agg(
        club_prestige_mean=('current_market_value_eur', 'mean'),
        club_muscle_sum=('current_market_value_eur', 'sum') 
    ).reset_index()
    df = pd.merge(df, club_metrics, on='club_name', how='left')
    
    # 4. Generational Outlier Detection
    elite_club_threshold = df['club_muscle_sum'].quantile(0.85)
    df['is_generational_teen'] = ((df['age'] <= 19) & 
                                  (df['minutes_played'] >= 1000) & 
                                  (df['club_muscle_sum'] >= elite_club_threshold)).astype(int)

    # 5. Position Encoding
    df = pd.get_dummies(df, columns=['position'], drop_first=False)
    
    return df

def train_and_predict(df: pd.DataFrame) -> pd.DataFrame:
    """Trains a weighted XGBoost model and generates theoretical market values."""
    logging.info("Preparing data matrix and sample weights...")
    
    features_to_drop = ['player_id', 'player_name', 'club_name', 'date_of_birth', 'current_market_value_eur']
    X = df.drop(columns=features_to_drop)
    y_log = np.log1p(df['current_market_value_eur']) 
    
    # Linear weighting to prioritize high-value financial assets
    sample_weights = df['current_market_value_eur'] / 1000000.0
    
    X_train, X_test, y_log_train, y_log_test, w_train, w_test = train_test_split(
        X, y_log, sample_weights, test_size=0.2, random_state=42
    )
    
    logging.info("Training XGBoost Regressor...")
    model = xgb.XGBRegressor(
        n_estimators=1000, 
        learning_rate=0.015, 
        max_depth=9, 
        min_child_weight=1, 
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42, 
        n_jobs=-1
    )
    
    model.fit(X_train, y_log_train, sample_weight=w_train)
    
    r2_log = r2_score(y_log_test, model.predict(X_test))
    logging.info(f"Model Validation - R-Squared (Log Scale): {r2_log:.3f}")
    
    logging.info("Generating predictions and calculating financial delta...")
    df['theoretical_value_eur'] = np.expm1(model.predict(X))
    df['value_delta_eur'] = df['theoretical_value_eur'] - df['current_market_value_eur']
    
    return df

def load_predictions(df: pd.DataFrame, engine):
    """Formats and loads the final intelligence table into the data warehouse."""
    logging.info("Formatting output for BI consumption...")
    
    final_cols = ['player_id', 'player_name', 'current_market_value_eur', 'theoretical_value_eur', 'value_delta_eur']
    final_predictions = df[final_cols].copy()
    
    final_predictions['theoretical_value_eur'] = final_predictions['theoretical_value_eur'].round(0)
    final_predictions['value_delta_eur'] = final_predictions['value_delta_eur'].round(0)
    
    logging.info("Pushing intelligence to PostgreSQL table 'predicted_player_valuations'...")
    final_predictions.to_sql('predicted_player_valuations', engine, if_exists='replace', index=False)
    logging.info("Pipeline execution completed successfully.")

def main():
    """Main execution block for the valuation pipeline."""
    try:
        engine = get_database_engine()
        raw_df = extract_data(engine)
        engineered_df = engineer_features(raw_df)
        scored_df = train_and_predict(engineered_df)
        load_predictions(scored_df, engine)
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")

if __name__ == "__main__":
    main()