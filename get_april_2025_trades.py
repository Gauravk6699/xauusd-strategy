import sqlite3
import pandas as pd

DATABASE_NAME = "trade_log.db"
TABLE_NAME = "trade_info"

def get_trades_for_april_2025():
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        # The entry_time is stored as TEXT, so we can use string functions
        # Assuming entry_time is in a format like 'YYYY-MM-DD HH:MM:SS' or ISO8601 'YYYY-MM-DDTHH:MM:SS'
        # The query will select all columns for trades opened in April 2025.
        query = f"SELECT * FROM {TABLE_NAME} WHERE strftime('%Y-%m', entry_time) = '2025-04';"
        
        df = pd.read_sql_query(query, conn)
        
        if not df.empty:
            print(f"Trades opened in April 2025 from '{DATABASE_NAME}', table '{TABLE_NAME}':")
            # Convert entry_time and exit_time to a more readable format if they are full timestamps
            if 'entry_time' in df.columns:
                df['entry_time'] = pd.to_datetime(df['entry_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
            if 'exit_time' in df.columns:
                # Handle 'STILL_OPEN' or other non-datetime strings in exit_time
                df['exit_time'] = df['exit_time'].apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%d %H:%M:%S') if pd.to_datetime(x, errors='coerce') is not pd.NaT else x)

            with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 1000):
                print(df)
            
            # Save the DataFrame to a CSV file
            csv_filename = "april_2025_trades.csv"
            try:
                df.to_csv(csv_filename, index=False)
                print(f"\nSuccessfully saved April 2025 trades to {csv_filename}")
            except Exception as e_csv:
                print(f"\nError saving trades to CSV {csv_filename}: {e_csv}")
        else:
            print("No trades found that were opened in April 2025.")
            
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    get_trades_for_april_2025()
