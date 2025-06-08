import sqlite3
import pandas as pd
from datetime import datetime

HISTORICAL_DATABASE_NAME = "silver_bot/xagusd_15min_data.db"
HISTORICAL_TABLE_NAME = "xagusd_15min"

def fetch_data_for_date(target_date_str):
    """Fetches historical 15-min data for a specific date from the SQLite database."""
    conn = None
    try:
        conn = sqlite3.connect(HISTORICAL_DATABASE_NAME)
        # Timestamps are stored as TEXT in ISO format.
        # We need to select data where the date part of the timestamp matches target_date_str.
        query = f"SELECT timestamp, open, high, low, close FROM {HISTORICAL_TABLE_NAME} WHERE strftime('%Y-%m-%d', timestamp) = ? ORDER BY timestamp ASC"
        
        df = pd.read_sql_query(query, conn, params=(target_date_str,))
        
        if not df.empty:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            print(f"\n15-minute candle data for {target_date_str}:")
            with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 1000):
                print(df)
            
            daily_open = df['open'].iloc[0]
            min_low_after_open = df['low'].min() # Min low of the entire day
            
            print(f"\nSummary for {target_date_str}:")
            print(f"  Daily Open Price: {daily_open:.5f}")
            print(f"  Lowest Price of the Day: {min_low_after_open:.5f}")
            print(f"  Difference (Open - Lowest): {daily_open - min_low_after_open:.5f}")
            print(f"  Percentage Drop from Open to Lowest: {((daily_open - min_low_after_open) / daily_open) * 100:.3f}%")

        else:
            print(f"No data found for {target_date_str} in table '{HISTORICAL_TABLE_NAME}'.")
        
        return df

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    # Get the target date from the user or set it directly
    # For this specific request:
    date_to_analyze = "2025-04-03"
    fetch_data_for_date(date_to_analyze)
