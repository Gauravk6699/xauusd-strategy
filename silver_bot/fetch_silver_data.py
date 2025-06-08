import requests
import sqlite3
import json
from datetime import datetime, timedelta, date
import time
import pandas as pd

# --- Configuration ---
TRADERMADE_API_KEY = "8jAlKUm9dh31uKPjSs5b" # Renamed for clarity
POLYGON_API_KEY = "NuJZpfNWxPQ4XuUrLJWXV9uxRuS7ZI09"
SYMBOL = "XAGUSD" # Used for Tradermade, Polygon uses a specific format
POLYGON_SYMBOL = "C:XAGUSD" # Assumption for Polygon
INTERVAL_REQUEST = "minute"
RESAMPLE_INTERVAL = "15T"
DATABASE_NAME = "silver_bot/xagusd_15min_data.db"
TABLE_NAME = "xagusd_15min"
DAYS_PER_CHUNK = 2 # Fetch data in 2-day chunks as per API limit (for Tradermade, not used for Polygon)
REQUEST_DELAY_SECONDS = 5 # Delay between paginated API calls within a single fetch_polygon_data call (Increased from 1)
CHUNK_FETCH_DELAY_SECONDS = 10 # Delay between fetching major chunks (e.g., 6-month periods)

def get_overall_date_range():
    """Returns the overall start and end date for the last 30 days."""
    end_date_dt = datetime.now()
    start_date_dt = end_date_dt - timedelta(days=30) # Original for 30 days
    # start_date_dt = end_date_dt - timedelta(days=2) # Modified for 2 days
    # print(f"Debug: Fetching data for the last 2 days: {start_date_dt.strftime('%Y-%m-%d')} to {end_date_dt.strftime('%Y-%m-%d')}")
    return start_date_dt, overall_end_dt

def get_overall_date_range_for_polygon_test():
    """Returns start and end date from January 1, 2022, to now for Polygon fetch."""
    end_date_dt = datetime.now()
    start_date_dt = datetime(2022, 1, 1) # Changed to January 1, 2022
    print(f"Polygon Fetch: Fetching data from {start_date_dt.strftime('%Y-%m-%d')} to {end_date_dt.strftime('%Y-%m-%d')}")
    return start_date_dt, end_date_dt

def fetch_polygon_data(forex_ticker, multiplier, timespan, start_date_str, end_date_str):
    """Fetches historical OHLC data from Polygon.io API."""
    base_url = "https://api.polygon.io/v2/aggs/ticker"
    url = f"{base_url}/{forex_ticker}/range/{multiplier}/{timespan}/{start_date_str}/{end_date_str}"
    
    params = {
        "apiKey": POLYGON_API_KEY,
        "sort": "asc",
        "limit": 50000 # Polygon's typical max results
    }
    
    print(f"Fetching Polygon data from URL: {url}")
    print(f"With parameters: {params}") # Don't print API key in real production logs if sensitive

    all_results = []
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4XX or 5XX)
        data = response.json()
        
        # print(f"Raw JSON response from Polygon (first 500 chars): {str(data)[:500]}") # Comment out after testing

        # Improved error checking based on Polygon's typical responses
        if data.get("status") == "ERROR" or data.get("resultsCount") == 0 : # Check resultsCount too
            if data.get("status") == "ERROR":
                 # Polygon might use 'error' or 'message' for error details
                error_message = data.get('error', data.get('message', 'Unknown API error'))
                print(f"Polygon API Error: {error_message}")
            elif data.get("resultsCount") == 0:
                 print(f"Polygon API returned status {data.get('status', 'OK')} but 0 results for the given period.")
            return None # Return None if there's an error or no data
        
        results = data.get("results", [])
        all_results.extend(results)
        
        # Basic pagination handling
        next_url = data.get("next_url")
        request_count = 1
        max_requests = 20 # Safety break for pagination loop (Increased from 10)

        while next_url and request_count < max_requests:
            print(f"Fetching next page from Polygon (request {request_count + 1})...")
            # Ensure API key is part of the next_url or add it. Polygon's next_url usually includes it.
            if "apiKey=" not in next_url:
                next_url_with_key = f"{next_url}&apiKey={POLYGON_API_KEY}"
            else:
                next_url_with_key = next_url
            
            time.sleep(REQUEST_DELAY_SECONDS) # Be respectful to the API between paginated requests
            
            paginated_response = requests.get(next_url_with_key)
            paginated_response.raise_for_status()
            paginated_data = paginated_response.json()
            request_count += 1

            if paginated_data.get("status") == "ERROR":
                error_message = paginated_data.get('error', paginated_data.get('message', 'Unknown API error on pagination'))
                print(f"Polygon API Error on pagination: {error_message}")
                break 
            
            current_page_results = paginated_data.get("results", [])
            if not current_page_results: # No more results on this page
                print("Pagination: No more results on current page.")
                break
            
            all_results.extend(current_page_results)
            print(f"Fetched {len(current_page_results)} more records. Total now: {len(all_results)}")
            next_url = paginated_data.get("next_url")
            if not next_url:
                print("Pagination: No next_url found, ending pagination.")


        print(f"Fetched a total of {len(all_results)} records from Polygon for {start_date_str} to {end_date_str} after handling pagination.")

        if not all_results:
            print("No results returned from Polygon.")
            return []

        # Convert to DataFrame for easier handling and to match expected structure
        df = pd.DataFrame(all_results)
        
        # Assumptions for column names from Polygon: t, o, h, l, c, v
        # 't' is usually Unix MS timestamp
        column_map = {
            't': 'timestamp_ms', 'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume'
        }
        df.rename(columns=column_map, inplace=True)

        if 'timestamp_ms' not in df.columns:
            print("Error: 'timestamp_ms' (expected from 't') column missing in Polygon data.")
            print(f"Available columns: {df.columns.tolist()}")
            return [] # Or handle error appropriately

        # Convert timestamp from milliseconds to datetime objects (UTC)
        df['timestamp'] = pd.to_datetime(df['timestamp_ms'], unit='ms', utc=True)
        
        # Select and reorder columns to a standard format
        # For now, just return the essential columns for inspection
        # Later, this will feed into the database insertion which expects 'date_time' as string
        # For the test, we'll keep it as datetime objects in 'timestamp'
        processed_data = []
        for index, row in df.iterrows():
            processed_data.append({
                "date_time": row['timestamp'].strftime("%Y-%m-%d %H:%M:%S"), # Changed to 'date_time' and formatted string
                "open": row['open'],
                "high": row['high'],
                "low": row['low'],
                "close": row['close'],
                "volume": row.get('volume', 0) 
            })
        return processed_data

    except requests.exceptions.HTTPError as e:
        print(f"HTTP error fetching Polygon data: {e} - {e.response.text if e.response else 'No response text'}")
    except requests.exceptions.RequestException as e:
        print(f"Request exception fetching Polygon data: {e}")
    except json.JSONDecodeError:
        print(f"JSON decode error fetching Polygon data. Response text: {response.text if 'response' in locals() else 'Response object not available'}")
    except Exception as e:
        print(f"An unexpected error occurred in fetch_polygon_data: {e}")
    
    return None


def fetch_tradermade_time_series_chunked(overall_start_dt, overall_end_dt):
    """Fetches time series data from Tradermade API in chunks."""
    all_quotes = []
    current_start_dt = overall_start_dt

    print(f"Starting chunked fetch from {overall_start_dt.strftime('%Y-%m-%d')} to {overall_end_dt.strftime('%Y-%m-%d')}")

    while current_start_dt < overall_end_dt:
        # Determine end of the current chunk (max DAYS_PER_CHUNK)
        chunk_end_dt = min(current_start_dt + timedelta(days=DAYS_PER_CHUNK -1), overall_end_dt) # -1 because start_date is inclusive

        start_date_str = current_start_dt.strftime("%Y-%m-%d")
        end_date_str = chunk_end_dt.strftime("%Y-%m-%d")
        
        # Ensure end_date_str is not before start_date_str if overall_end_dt is very close to current_start_dt
        if chunk_end_dt < current_start_dt:
             # This can happen if overall_end_dt is the same as current_start_dt on the last iteration
             # and we only want data for that single day.
             end_date_str = start_date_str


        url = "https://marketdata.tradermade.com/api/v1/timeseries"
        params = {
            "currency": SYMBOL,
            "api_key": API_KEY,
            "start_date": start_date_str,
            "end_date": end_date_str,
            "format": "records",
            "interval": INTERVAL_REQUEST
        }
        
        print(f"Fetching chunk: {start_date_str} to {end_date_str} with params: {params}")
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            if "error" in data and data["error"]:
                print(f"API Error for chunk {start_date_str}-{end_date_str}: {data.get('message', 'Unknown error')}")
            elif "message" in data and "please upgrade your account" in data["message"].lower():
                print(f"API Error for chunk {start_date_str}-{end_date_str}: {data['message']}")
                # Potentially stop all fetching if it's an account-wide issue
                return None 
            elif "quotes" in data:
                chunk_quotes = data.get("quotes", [])
                all_quotes.extend(chunk_quotes)
                print(f"Fetched {len(chunk_quotes)} records for chunk {start_date_str}-{end_date_str}. Total: {len(all_quotes)}")
            else:
                print(f"Unexpected response for chunk {start_date_str}-{end_date_str}: 'quotes' field missing. Response: {data}")

        except requests.exceptions.HTTPError as e:
            print(f"HTTP error for chunk {start_date_str}-{end_date_str}: {e} - {e.response.text}")
            # Depending on error, might want to retry or stop
        except requests.exceptions.RequestException as e:
            print(f"Request exception for chunk {start_date_str}-{end_date_str}: {e}")
        except json.JSONDecodeError:
            print(f"JSON decode error for chunk {start_date_str}-{end_date_str}: {response.text}")
        
        # Move to the next chunk
        current_start_dt = chunk_end_dt + timedelta(days=1) # Next day after current chunk's end
        if current_start_dt < overall_end_dt: # Only sleep if there are more chunks
             print(f"Waiting for {REQUEST_DELAY_SECONDS}s before next request...")
             time.sleep(REQUEST_DELAY_SECONDS)

    print(f"Finished chunked fetch. Total raw '{INTERVAL_REQUEST}' data points: {len(all_quotes)}")
    return all_quotes

def resample_data_to_15min(minute_data):
    if not minute_data:
        print("No minute data to resample.")
        return []

    print(f"Debugging resample_data_to_15min:")
    print(f"  Type of minute_data: {type(minute_data)}")
    if isinstance(minute_data, list) and len(minute_data) > 0:
        print(f"  Length of minute_data: {len(minute_data)}")
        print(f"  First element of minute_data: {minute_data[0]}")
        all_dicts = all(isinstance(item, dict) for item in minute_data)
        print(f"  All elements in minute_data are dictionaries: {all_dicts}")
        if all_dicts and minute_data[0]:
            print(f"  Keys in first element: {list(minute_data[0].keys())}") # Use list() for cleaner print
            # Check a few more elements if possible
            if len(minute_data) > 100:
                 print(f"  Keys in 100th element: {list(minute_data[100].keys())}")


    try:
        df = pd.DataFrame(minute_data)
        print(f"  DataFrame columns after creation: {df.columns.tolist()}")
        
        if df.empty:
            print("  DataFrame is empty after creation. Cannot resample.")
            return []
            
        # Tradermade API returns the timestamp field as 'date'
        datetime_column_name = 'date' # Changed from 'date_time'
        if datetime_column_name not in df.columns:
            print(f"Error: '{datetime_column_name}' column missing in fetched data for resampling.")
            print(f"  DataFrame head (first 5 rows if available):\n{df.head()}")
            return []
        
        df[datetime_column_name] = pd.to_datetime(df[datetime_column_name])
        df.set_index(datetime_column_name, inplace=True)

        for col in ['open', 'high', 'low', 'close']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            else:
                print(f"Warning: Column '{col}' missing for resampling.")
                return []
        df.dropna(subset=['open', 'high', 'low', 'close'], inplace=True)
        if df.empty:
            print("DataFrame is empty after processing, cannot resample.")
            return []

        resampled_df = df.resample(RESAMPLE_INTERVAL).agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'
        }).dropna()

        resampled_data = []
        for timestamp, row in resampled_df.iterrows():
            resampled_data.append({
                "date_time": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "open": row['open'], "high": row['high'],
                "low": row['low'], "close": row['close'], "volume": 0
            })
        print(f"Resampled to {len(resampled_data)} {RESAMPLE_INTERVAL} data points.")
        return resampled_data
    except Exception as e:
        print(f"Error during resampling: {e}")
        return []

def create_database_table(conn):
    cursor = conn.cursor()
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        timestamp TEXT PRIMARY KEY, open REAL, high REAL, low REAL, close REAL, volume INTEGER DEFAULT 0
    )""")
    conn.commit()

def insert_data_into_db(conn, data_values):
    cursor = conn.cursor()
    inserted_count = 0
    skipped_count = 0
    if not data_values:
        print("No data values to insert.")
        return inserted_count, skipped_count
    for record in data_values:
        try:
            dt_obj = datetime.strptime(record["date_time"], "%Y-%m-%d %H:%M:%S")
            iso_timestamp = dt_obj.isoformat()
            volume = record.get("volume", 0)
            cursor.execute(f"""
            INSERT OR IGNORE INTO {TABLE_NAME} (timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?)
            """, (iso_timestamp, float(record["open"]), float(record["high"]),
                  float(record["low"]), float(record["close"]), int(volume)))
            if cursor.rowcount > 0: inserted_count += 1
            else: skipped_count += 1
        except Exception as e:
            print(f"Error inserting record {record}: {e}")
            skipped_count += 1
    conn.commit()
    return inserted_count, skipped_count

def main():
    # --- Polygon.io Data Fetch (from Jan 1, 2022, in chunks) ---
    print("--- Starting Polygon.io API Data Fetch (from Jan 1, 2022, in chunks) ---")
    
    overall_start_date = datetime(2022, 1, 1)
    overall_end_date = datetime.now()
    
    current_chunk_start_date = overall_start_date
    
    import os
    db_dir = os.path.dirname(DATABASE_NAME)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir)
    
    conn = None
    total_inserted_all_chunks = 0
    total_skipped_all_chunks = 0

    try:
        conn = sqlite3.connect(DATABASE_NAME)
        create_database_table(conn) # Ensure table exists

        while current_chunk_start_date < overall_end_date:
            # Determine end of the current 6-month chunk
            # A simple way to add 6 months is tricky with datetime. A more robust way:
            year = current_chunk_start_date.year
            month = current_chunk_start_date.month + 6
            day = current_chunk_start_date.day

            if month > 12:
                year += (month -1) // 12
                month = (month -1) % 12 + 1
            
            # Handle cases like Jan 31 + 6 months -> July 31, but Feb 28 + 6 months -> Aug 28
            # For simplicity in chunking, just aim for end of month or specific day.
            # Let's use a fixed day for simplicity or end of month logic.
            # A simpler approach for 6 month step:
            next_month_val = current_chunk_start_date.month + 5 # 0-indexed for month end, so +5 for 6 months
            next_year_val = current_chunk_start_date.year + next_month_val // 12
            next_month_val = next_month_val % 12 + 1

            # Find the last day of that target month
            if next_month_val == 12:
                current_chunk_end_date = datetime(next_year_val, next_month_val, 31)
            else:
                current_chunk_end_date = datetime(next_year_val, next_month_val + 1, 1) - timedelta(days=1)
            
            # Ensure chunk end date does not exceed overall end date
            current_chunk_end_date = min(current_chunk_end_date, overall_end_date)

            # Ensure start is not after end (can happen on the last chunk)
            if current_chunk_start_date > current_chunk_end_date:
                print(f"Chunk start date {current_chunk_start_date.strftime('%Y-%m-%d')} is after chunk end date {current_chunk_end_date.strftime('%Y-%m-%d')}. Ending fetch.")
                break

            print(f"\nFetching chunk from {current_chunk_start_date.strftime('%Y-%m-%d')} to {current_chunk_end_date.strftime('%Y-%m-%d')}")

            polygon_data = fetch_polygon_data(
                forex_ticker=POLYGON_SYMBOL,
                multiplier="15",
                timespan="minute",
                start_date_str=current_chunk_start_date.strftime("%Y-%m-%d"),
                end_date_str=current_chunk_end_date.strftime("%Y-%m-%d")
            )

            if polygon_data:
                print(f"Fetched {len(polygon_data)} records for chunk. Proceeding to database insertion.")
                print(f"Inserting Polygon.io data into {DATABASE_NAME}, table {TABLE_NAME}...")
                inserted, skipped = insert_data_into_db(conn, polygon_data) # Pass connection
                total_inserted_all_chunks += inserted
                total_skipped_all_chunks += skipped
                print(f"Chunk insertion complete. Inserted new: {inserted}, Skipped existing/errors: {skipped}")
                conn.commit() # Commit after each successful chunk insertion
            else:
                print(f"Failed to fetch data for chunk {current_chunk_start_date.strftime('%Y-%m-%d')} to {current_chunk_end_date.strftime('%Y-%m-%d')}. Skipping this chunk.")

            # Move to the next chunk
            current_chunk_start_date = current_chunk_end_date + timedelta(days=1)

            if current_chunk_start_date < overall_end_date:
                print(f"Waiting for {CHUNK_FETCH_DELAY_SECONDS}s before next major chunk request...")
                time.sleep(CHUNK_FETCH_DELAY_SECONDS)
        
        print(f"\n--- Overall Data Fetch Complete ---")
        print(f"Total new records inserted across all chunks: {total_inserted_all_chunks}")
        print(f"Total records skipped (existing/errors) across all chunks: {total_skipped_all_chunks}")

    except sqlite3.Error as e:
        print(f"SQLite error during chunk processing: {e}")
    except Exception as e:
        print(f"An unexpected error occurred in main during chunk processing: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

    # --- Original Tradermade Fetch (Now fully commented out as we are using Polygon) ---
    # overall_start_dt, overall_end_dt = get_overall_date_range()
    # print(f"Fetching raw '{INTERVAL_REQUEST}' data for {SYMBOL} from {overall_start_dt.strftime('%Y-%m-%d')} to {overall_end_dt.strftime('%Y-%m-%d')} using Tradermade (chunked)...")
    # raw_minute_data = fetch_tradermade_time_series_chunked(overall_start_dt, overall_end_dt)
    # if raw_minute_data:
    #     print(f"Resampling raw data to {RESAMPLE_INTERVAL} intervals...")
    #     resampled_15min_data = resample_data_to_15min(raw_minute_data)
    #     if resampled_15min_data:
    #         import os
    #         db_dir = os.path.dirname(DATABASE_NAME)
    #         if db_dir and not os.path.exists(db_dir):
    #             os.makedirs(db_dir)
    #         conn = None
    #         try:
    #             conn = sqlite3.connect(DATABASE_NAME)
    #             create_database_table(conn)
    #             print(f"Inserting resampled {RESAMPLE_INTERVAL} data into {DATABASE_NAME}, table {TABLE_NAME}...")
    #             inserted, skipped = insert_data_into_db(conn, resampled_15min_data)
    #             print(f"Data insertion complete. Inserted new: {inserted}, Skipped existing/errors: {skipped}")
    #         except sqlite3.Error as e: print(f"SQLite error: {e}")
    #         finally:
    #             if conn: conn.close()
    #     else: print("No data after resampling. Nothing to insert into database.")
    # else: print("Failed to fetch raw data from Tradermade. No data to process.")


if __name__ == "__main__":
    main()
