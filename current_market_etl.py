import pandas as pd
import os

def build_current_market_base_table(raw_dir):
    print("🚀 Initiating Current Market ETL Pipeline...\n")
    
    try:
        # 1. Load the required datasets
        print("📥 Loading vendor data drops...")
        players = pd.read_csv(os.path.join(raw_dir, 'players.csv'))
        clubs = pd.read_csv(os.path.join(raw_dir, 'clubs.csv'))
        appearances = pd.read_csv(os.path.join(raw_dir, 'appearances.csv'))
        valuations = pd.read_csv(os.path.join(raw_dir, 'player_valuations.csv'))

        # 2. Filter Performance Data to CURRENT Season/Year
        print("⏱️ Isolating recent on-pitch performance...")
        appearances['date'] = pd.to_datetime(appearances['date'])
        # Filtering for matches from the start of the 23/24 season onwards
        recent_appearances = appearances[appearances['date'] >= '2023-08-01']
        
        # Group by player_id and sum up their recent stats
        player_stats = recent_appearances.groupby('player_id').agg({
            'minutes_played': 'sum',
            'goals': 'sum',
            'assists': 'sum',
            'yellow_cards': 'sum',
            'red_cards': 'sum'
        }).reset_index()

        # Filter out players with less than 500 minutes in the current window
        player_stats = player_stats[player_stats['minutes_played'] >= 500]

        # 3. Process Valuations (Get the exact CURRENT market value)
        print("💰 Processing current financial asset valuations...")
        valuations['date'] = pd.to_datetime(valuations['date'])
        
        # Sort by date descending, then drop duplicate players to keep only their latest row
        current_vals = valuations.sort_values('date', ascending=False).drop_duplicates('player_id')
        current_vals = current_vals[['player_id', 'market_value_in_eur']]
        current_vals.rename(columns={'market_value_in_eur': 'current_market_value_eur'}, inplace=True)

        # 4. Join Everything Together (The Analytics Base Table)
        print("🔗 Merging Dimensions, Facts, and Targets...")
        
        # Join Stats to Players
        abt = pd.merge(player_stats, players[['player_id', 'name', 'position', 'date_of_birth', 'current_club_id']], on='player_id', how='inner')
        
        # Join with Clubs to get the Club Name
        abt = pd.merge(abt, clubs[['club_id', 'name']], left_on='current_club_id', right_on='club_id', how='left', suffixes=('', '_club'))
        abt.rename(columns={'name_club': 'club_name', 'name': 'player_name'}, inplace=True)
        
        # Join with Current Valuations
        abt = pd.merge(abt, current_vals, on='player_id', how='inner')

        # Drop any players missing a current market valuation
        abt = abt.dropna(subset=['current_market_value_eur'])

        # 5. Feature Engineering (Creating KPI Ratios for the Dashboard)
        print("🧠 Engineering Business KPIs...")
        abt['goals_per_90'] = (abt['goals'] / abt['minutes_played']) * 90
        abt['assists_per_90'] = (abt['assists'] / abt['minutes_played']) * 90
        
        # Round the engineered features
        abt['goals_per_90'] = abt['goals_per_90'].round(2)
        abt['assists_per_90'] = abt['assists_per_90'].round(2)

        # Clean up final columns for the SQL Data Warehouse
        final_cols = [
            'player_id', 'player_name', 'position', 'club_name', 'date_of_birth',
            'minutes_played', 'goals', 'assists', 'goals_per_90', 'assists_per_90', 
            'current_market_value_eur'
        ]
        abt = abt[final_cols]

        print(f"\n✅ ETL Complete. Total viable players for analysis: {len(abt)}")
        print("\nPreview of Final Analytics Base Table:")
        print(abt.head())

        # Export for Phase 2 and 3
        output_file = "Current_Market_Analytics_Base.csv"
        abt.to_csv(output_file, index=False)
        print(f"\n📁 Master dataset exported successfully to {output_file}")
        print("🏆 PHASE 1 COMPLETE.")

    except Exception as e:
        print(f"❌ Error during ETL process: {e}")

if __name__ == "__main__":
    raw_data_folder = "data_raw"
    build_current_market_base_table(raw_data_folder)