# ⚽ Football Transfer Market Intelligence: An Arbitrage Scouting Engine

## 📌 Executive Summary
The football transfer market is highly volatile and often driven by emotion, media hype, and subjective scouting. This project introduces a purely objective, data-driven approach to player valuation. By treating players as financial assets, this end-to-end pipeline extracts performance metrics, warehouses the data in a relational database, and utilizes advanced Machine Learning to calculate a player's "Theoretical Market Value." 

The resulting Power BI dashboard acts as a Scouting Radar, instantly calculating the financial delta between a player's actual market price and their algorithmic value to flag **undervalued steals** and **overvalued risks**.

## 🏗️ System Architecture & Pipeline
This project is built on a robust, 4-stage enterprise data architecture:

1. **Data Extraction & Engineering (Python / Pandas):** - Ingests raw performance and financial data.
   - Engineers advanced metrics including *Volume-Weighted Output*, *Proven Consistency Indexes*, and *Effective Age* to handle real-world football nuances.
2. **Data Warehousing (PostgreSQL):** - Structured Star Schema database to securely house dimensions (Clubs, Players) and facts (Match Metrics, Valuations).
3. **Machine Learning Valuation Engine (XGBoost / Scikit-Learn):**
   - **Logarithmic Transformation:** Normalizes heavily right-skewed financial asset data.
   - **Linear Financial Weighting:** Forces the algorithm to prioritize accuracy on €100M+ world-class assets over average lower-league players.
   - **Z-Score Standardization:** Identifies top 1% Ballon d'Or candidates objectively, stripping away algorithmic bias against elite veterans.
4. **Business Intelligence (Power BI):**
   - Translates complex ML outputs into an interactive, dark-mode UI with conditional DAX formatting for executive-level decision-making.

## 🛠️ Tech Stack
* **Language:** Python 3.x
* **Data Engineering:** Pandas, NumPy, SQLAlchemy
* **Database:** PostgreSQL
* **Machine Learning:** XGBoost, Scikit-Learn
* **Data Visualization:** Microsoft Power BI, DAX

## 💡 Key Machine Learning Solutions
During model evaluation, standard Random Forest algorithms failed to accurately price absolute superstars (e.g., Lamine Yamal, Erling Haaland) and aggressively penalized world-class veterans (e.g., Harry Kane). 

To solve this, the final `XGBoost` model incorporates:
* **The "Generational Outlier" Flag:** Mathematical isolation for teenagers playing massive minutes for elite-prestige clubs.
* **Algorithmic Sample Weighting:** Custom loss-function weights directly tied to a player's market value, unchaining the decision trees to build highly specific rules for the top 0.1% of global talent.
* **Volume vs. Rate Architecture:** Multiplying `goals_per_90` by total volume and minutes played to eliminate the "small sample size" bias of bench players.

## 📊 The Dashboard
The final output is an interactive Power BI terminal allowing scouting departments to filter by age, position, and club to instantly visualize market inefficiencies. 
*(See the `dashboard_preview.png` file in this repository for a visual overview).*

## 📂 Data Source & Setup
To maintain repository performance and adhere to data engineering best practices, the raw dataset is not hosted in this repository. 

**The data is sourced from the official Transfermarkt dataset on Kaggle:**
🔗 [Football Data from Transfermarkt (Kaggle)](https://www.kaggle.com/datasets/davidcariboo/player-scores)

**Prerequisites for Reproducibility:**
1. Download the raw `.csv` files from the Kaggle link above.
2. Ensure you have a local instance of PostgreSQL installed and running.
3. You must run the initial Data Engineering ETL script (db_ingestion.py) (to ingest the Kaggle CSVs, clean the data, and build the relational Star Schema) to generate the `ml_feature_base` table before executing the Machine Learning pipeline.

## 🚀 How to Run the Pipeline
1. Clone this repository to your local machine.
2. Download the raw data from Kaggle and execute the initial ETL ingestion script to populate your local PostgreSQL database. 
3. Open `train_valuation_model.py` and update the `DB_PASSWORD` variable with your local PostgreSQL credentials.
4. Install the required Python dependencies:
   ```bash
   pip install -r requirements.txt

5. Execute the Machine Learning pipeline to generate valuations and calculate market arbitrage:
   ```bash
   python train_valuation_model.py

