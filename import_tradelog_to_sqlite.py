import pandas as pd
import sqlite3
import os

# Configuration
CSV_FILE_PATH = 'backtest_trade_log.csv'
DB_FILE_PATH = 'trade_log.db'
TABLE_NAME = 'trade_info'

def import_csv_to_sqlite():
    """
    Reads data from a CSV file and imports it into an SQLite database table.
    If the database or table already exists, it will be replaced.
    """
    # Check if CSV file exists
    if not os.path.exists(CSV_FILE_PATH):
        print(f"Error: CSV file not found at {CSV_FILE_PATH}")
        return

    try:
        # Read CSV into a pandas DataFrame
        print(f"Reading CSV file: {CSV_FILE_PATH}...")
        df = pd.read_csv(CSV_FILE_PATH)
        print(f"Successfully read {len(df)} rows from CSV.")

        # Ensure column names are suitable for SQLite (e.g., no spaces, special chars)
        # Pandas to_sql handles this reasonably well, but good practice to clean if needed.
        # For now, we assume column names from CSV are acceptable.

        # Connect to SQLite database (this will create the DB if it doesn't exist)
        print(f"Connecting to SQLite database: {DB_FILE_PATH}...")
        conn = sqlite3.connect(DB_FILE_PATH)
        
        # Use pandas to_sql to write data to the table
        # 'if_exists="replace"' will drop the table first if it exists and create a new one.
        # 'if_exists="append"' would add data if table exists.
        # 'if_exists="fail"' would raise an error if table exists.
        print(f"Writing data to table '{TABLE_NAME}' in database '{DB_FILE_PATH}'...")
        df.to_sql(TABLE_NAME, conn, if_exists='replace', index=False)
        
        print(f"Successfully imported data into table '{TABLE_NAME}'.")
        
        # Verify by counting rows in the new table
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
        count = cursor.fetchone()[0]
        print(f"Verification: Table '{TABLE_NAME}' contains {count} rows.")

    except FileNotFoundError:
        print(f"Error: CSV file not found at {CSV_FILE_PATH}")
    except pd.errors.EmptyDataError:
        print(f"Error: CSV file {CSV_FILE_PATH} is empty.")
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print(f"Database connection to '{DB_FILE_PATH}' closed.")

if __name__ == '__main__':
    import_csv_to_sqlite()
