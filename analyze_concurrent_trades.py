import sqlite3
import pandas as pd

# Configuration
DB_FILE_PATH = 'trade_log.db'
TABLE_NAME = 'trade_info'

def calculate_max_concurrent_trades():
    """
    Connects to the SQLite database, queries the trade_info table,
    and calculates the maximum number of concurrently open trades.
    """
    try:
        # Connect to SQLite database
        print(f"Connecting to SQLite database: {DB_FILE_PATH}...")
        conn = sqlite3.connect(DB_FILE_PATH)

        # Execute SQL query to fetch necessary columns
        query = f"SELECT entry_time, exit_time, status FROM {TABLE_NAME}" # Added status
        print(f"Executing query: {query}...")
        df = pd.read_sql_query(query, conn)
        print(f"Successfully fetched {len(df)} records from table '{TABLE_NAME}'.")

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        return
    except Exception as e:
        print(f"An unexpected error occurred during database operation: {e}")
        return
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print(f"Database connection to '{DB_FILE_PATH}' closed.")

    if df.empty:
        print("No trade data found. Max concurrent trades: 0")
        return

    # Data Preprocessing
    try:
        print("Converting time columns to datetime objects...")
        df['entry_time'] = pd.to_datetime(df['entry_time'])
        # Coerce errors for exit_time to handle 'STILL_OPEN'
        df['exit_time'] = pd.to_datetime(df['exit_time'], errors='coerce') 
        print("Initial time conversion successful.")

        # Determine the last known timestamp in the dataset
        # Consider both entry times and valid exit times
        valid_exit_times = df['exit_time'].dropna()
        if not valid_exit_times.empty:
            max_exit_time = valid_exit_times.max()
        else:
            max_exit_time = pd.NaT # No valid exit times

        max_entry_time = df['entry_time'].max()

        if pd.isna(max_exit_time):
            last_known_timestamp = max_entry_time
        elif pd.isna(max_entry_time): # Should not happen if there's data
             last_known_timestamp = max_exit_time
        else:
            last_known_timestamp = max(max_entry_time, max_exit_time)
            
        if pd.isna(last_known_timestamp):
            print("Error: Could not determine a valid last known timestamp for 'STILL_OPEN' trades.")
            # Fallback: if all trades are STILL_OPEN and no exit times, use the latest entry time.
            # Or, if no trades at all, this won't be reached due to df.empty check.
            # If there's at least one entry, max_entry_time should be valid.
            if not df['entry_time'].empty:
                 last_known_timestamp = df['entry_time'].max()
            else: # Should be caught by df.empty earlier
                 print("Critical error: No valid timestamps found in data.")
                 return


        # Fill NaT in exit_time (from 'STILL_OPEN') with the last known timestamp
        df['exit_time'] = df['exit_time'].fillna(last_known_timestamp)
        print(f"Filled 'STILL_OPEN' exit times with last known timestamp: {last_known_timestamp}")

    except Exception as e:
        print(f"Error during time conversion or 'STILL_OPEN' handling: {e}")
        return

    # Event Creation
    print("Creating trade events...")
    events = []
    for index, row in df.iterrows():
        events.append((row['entry_time'], 'entry'))
        events.append((row['exit_time'], 'exit'))
    
    # Sort events: Primary key timestamp, secondary key event type ('exit' before 'entry')
    # 'exit' corresponds to False (0), 'entry' corresponds to True (1) when comparing.
    # So, (timestamp, False) comes before (timestamp, True).
    events.sort(key=lambda x: (x[0], x[1] == 'entry'))
    print(f"Created and sorted {len(events)} events.")

    # Concurrency Calculation
    print("Calculating maximum concurrency...")
    max_concurrent_trades = 0
    current_concurrent_trades = 0
    for timestamp, event_type in events:
        if event_type == 'entry':
            current_concurrent_trades += 1
            if current_concurrent_trades > max_concurrent_trades:
                max_concurrent_trades = current_concurrent_trades
        elif event_type == 'exit':
            current_concurrent_trades -= 1
    
    print(f"Maximum number of concurrent trades: {max_concurrent_trades}")
    return max_concurrent_trades

if __name__ == '__main__':
    calculate_max_concurrent_trades()
