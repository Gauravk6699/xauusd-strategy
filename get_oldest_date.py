import sqlite3
import pandas as pd

DATABASE_NAME = "silver_bot/xagusd_15min_data.db"
TABLE_NAME = "xagusd_15min"

def get_oldest_timestamp():
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        # Query for the minimum timestamp. Timestamps are stored as TEXT in ISO format.
        query = f"SELECT MIN(timestamp) FROM {TABLE_NAME};"
        cursor = conn.cursor()
        cursor.execute(query)
        oldest_timestamp_str = cursor.fetchone()[0]
        
        if oldest_timestamp_str:
            # Convert to datetime object for more readable print, though string is fine
            # oldest_dt = pd.to_datetime(oldest_timestamp_str)
            print(f"The oldest timestamp in the database '{DATABASE_NAME}' table '{TABLE_NAME}' is: {oldest_timestamp_str}")
        else:
            print(f"No data found in table '{TABLE_NAME}'.")
            
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    get_oldest_timestamp()
