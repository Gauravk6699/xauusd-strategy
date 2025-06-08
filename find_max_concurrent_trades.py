import pandas as pd
import sqlite3
from datetime import datetime, timezone

# Constants
TRADE_LOG_PATH = 'backtest_trade_log.csv'
DB_PATH = 'silver_bot/xagusd_15min_data.db'
PRICE_TABLE_NAME = 'xagusd_15min'

# P&L Calculation Constants (adjust if necessary)
POINT_VALUE = 0.01  # For XAGUSD, 1 pip is typically $0.01
PNL_INR_PER_PIP = 83.0 # Assuming 1 pip movement for 1 unit of base currency, converted to INR (e.g., $0.01 * 83 USD/INR)
# Note: If 'quantity' per trade varies and affects P&L, the formula might need trade['quantity'].
# The formula used here matches the structure from generate_new_cluster_report.py context.

def load_trades(file_path):
    """Loads trades from the CSV file."""
    try:
        trades_df = pd.read_csv(file_path)
        # Convert entry_time and exit_time to datetime objects
        # Assuming timestamps are in UTC and include timezone info like 'YYYY-MM-DD HH:MM:SS+00:00'
        # If they are naive, we might need to localize them or handle them as such.
        # For now, let's assume they can be parsed directly or are timezone-naive UTC.
        trades_df['entry_time'] = pd.to_datetime(trades_df['entry_time'], errors='coerce')
        
        # Handle 'STILL_OPEN' for exit_time before converting
        # We'll replace 'STILL_OPEN' with a very late date or handle it after loading prices
        # For now, coerce errors will turn unparsable 'STILL_OPEN' into NaT
        trades_df['exit_time_str'] = trades_df['exit_time'] # Keep original string for 'STILL_OPEN'
        trades_df['exit_time'] = pd.to_datetime(trades_df['exit_time'], errors='coerce')
        
        print(f"Loaded {len(trades_df)} trades from {file_path}")
        if trades_df['entry_time'].isnull().any():
            print("Warning: Some entry_time values could not be parsed.")
        return trades_df
    except FileNotFoundError:
        print(f"Error: Trade log file not found at {file_path}")
        return None
    except Exception as e:
        print(f"Error loading trades: {e}")
        return None

def load_price_data(db_path, table_name):
    """Loads 15-minute price data from the SQLite database."""
    try:
        conn = sqlite3.connect(db_path)
        # Corrected column name from 'time' to 'timestamp'
        prices_df = pd.read_sql_query(f"SELECT * FROM {table_name} ORDER BY timestamp", conn)
        conn.close()
        
        # Convert the 'timestamp' column to datetime objects
        prices_df['timestamp'] = pd.to_datetime(prices_df['timestamp'])
        
        # Ensure timestamps are timezone-aware (UTC)
        if prices_df['timestamp'].dt.tz is None:
            prices_df['timestamp'] = prices_df['timestamp'].dt.tz_localize('UTC')
        else:
            prices_df['timestamp'] = prices_df['timestamp'].dt.tz_convert('UTC')
            
        print(f"Loaded {len(prices_df)} price candles from {db_path}, table {table_name}")
        if prices_df.empty:
            print("Warning: Price data is empty.")
        else:
            # Corrected column name for min/max display
            print(f"Price data from {prices_df['timestamp'].min()} to {prices_df['timestamp'].max()}")
        return prices_df
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        return None
    except Exception as e:
        print(f"Error loading price data: {e}")
        return None

def find_max_concurrent_trades():
    """
    Finds the maximum number of concurrently open trades and the time it occurred.
    """
    trades_df = load_trades(TRADE_LOG_PATH)
    prices_df = load_price_data(DB_PATH, PRICE_TABLE_NAME)

    if trades_df is None or prices_df is None or prices_df.empty:
        print("Could not proceed due to errors loading data.")
        return

    # Ensure trades timestamps are compatible with price timestamps (UTC)
    if trades_df['entry_time'].dt.tz is None:
        trades_df['entry_time'] = trades_df['entry_time'].dt.tz_localize('UTC')
    else:
        trades_df['entry_time'] = trades_df['entry_time'].dt.tz_convert('UTC')

    # Handle 'STILL_OPEN' trades by setting their exit time to the last candle's end time + 1 microsecond
    # or a very large future date if no price data.
    # Corrected column name for max()
    last_candle_time = prices_df['timestamp'].max() if not prices_df.empty else pd.Timestamp.max.tz_localize('UTC')
    
    # Define exit_time for 'STILL_OPEN' trades
    # We consider a trade to extend just beyond the last known data point if it's still open.
    # For comparison, if a candle ends at T, a trade open at T is counted.
    # If a trade exits at T, it's not counted for a candle starting at T.
    # So, 'STILL_OPEN' trades should effectively have an exit_time > last_candle_time.
    # Let's use last_candle_time + a small delta, or if we define candle intervals as [start, end),
    # then exit_time = last_candle_end_time.
    
    # For simplicity, let's use the timestamp of the last candle + 15 minutes as the effective exit
    # for 'STILL_OPEN' trades, ensuring they are included in the last candle check.
    effective_still_open_exit_time = last_candle_time + pd.Timedelta(minutes=15)

    for index, row in trades_df.iterrows():
        if row['exit_time_str'] == 'STILL_OPEN' or pd.isna(row['exit_time']):
            trades_df.loc[index, 'exit_time'] = effective_still_open_exit_time
        elif trades_df.loc[index, 'exit_time'].tz is None: # Ensure exit_time is also UTC
             trades_df.loc[index, 'exit_time'] = trades_df.loc[index, 'exit_time'].tz_localize('UTC')
        else:
             trades_df.loc[index, 'exit_time'] = trades_df.loc[index, 'exit_time'].tz_convert('UTC')
    
    # Drop trades with invalid entry times after conversion
    trades_df.dropna(subset=['entry_time'], inplace=True)


    max_concurrent_trades = 0
    times_of_max_concurrency = []
    
    max_floating_loss_inr = 0  # Max loss will be the most negative P&L sum
    times_of_max_floating_loss = []


    # Define candle duration (assuming 15 minutes as per typical data)
    # This is crucial for defining the interval [candle_start, candle_end)
    # If prices_df['timestamp'] marks the START of the candle:
    candle_duration = pd.Timedelta(minutes=15) # Assuming 15-min candles

    print(f"Analyzing {len(prices_df)} candles...")
    for _, candle in prices_df.iterrows():
        # Corrected column name for candle time
        candle_start_time = candle['timestamp']
        candle_end_time = candle_start_time + candle_duration # Candle interval is [start, end)
        candle_close_price = candle['close'] # Used for floating P&L

        # Optimized: Filter trades active during this candle period
        active_trades_df = trades_df[
            (trades_df['entry_time'] < candle_end_time) & 
            (trades_df['exit_time'] > candle_start_time)
        ]
        
        concurrent_trades_count = len(active_trades_df)
        
        if concurrent_trades_count > 0:
            current_floating_pnl_sum_inr = 0
            # Iterate over the smaller, filtered DataFrame of active trades
            for _, trade in active_trades_df.iterrows():
                floating_pnl_trade_inr = 0
                # Ensure 'entry_price' and 'trade_type' columns exist
                if 'entry_price' not in trade or 'trade_type' not in trade:
                    print(f"Warning: Skipping P&L for trade {trade.get('trade_id', 'N/A')} due to missing columns.")
                    continue

                if trade['trade_type'] == 'LONG':
                    price_diff = candle_close_price - trade['entry_price']
                elif trade['trade_type'] == 'SHORT':
                    price_diff = trade['entry_price'] - candle_close_price
                else:
                    continue # Unknown trade type

                # P&L calculation based on price difference in points
                # Assumes PNL_INR_PER_PIP is for 1 unit of quantity.
                # If trade log has 'quantity' and it should be factored:
                # quantity = trade.get('quantity', 1) # Default to 1 if not present
                # floating_pnl_trade_inr = (price_diff / POINT_VALUE) * PNL_INR_PER_PIP * quantity
                floating_pnl_trade_inr = (price_diff / POINT_VALUE) * PNL_INR_PER_PIP

                current_floating_pnl_sum_inr += floating_pnl_trade_inr

            if current_floating_pnl_sum_inr < max_floating_loss_inr:
                max_floating_loss_inr = current_floating_pnl_sum_inr
                times_of_max_floating_loss = [candle_start_time.strftime('%Y-%m-%d %H:%M:%S %Z')]
            elif current_floating_pnl_sum_inr == max_floating_loss_inr:
                if candle_start_time.strftime('%Y-%m-%d %H:%M:%S %Z') not in times_of_max_floating_loss:
                    times_of_max_floating_loss.append(candle_start_time.strftime('%Y-%m-%d %H:%M:%S %Z'))

        if concurrent_trades_count > max_concurrent_trades:
            max_concurrent_trades = concurrent_trades_count
            times_of_max_concurrency = [candle_start_time.strftime('%Y-%m-%d %H:%M:%S %Z')]
        elif concurrent_trades_count == max_concurrent_trades and max_concurrent_trades > 0:
            if candle_start_time.strftime('%Y-%m-%d %H:%M:%S %Z') not in times_of_max_concurrency:
                 times_of_max_concurrency.append(candle_start_time.strftime('%Y-%m-%d %H:%M:%S %Z'))

    # Output Results
    print(f"\n--- Overall Backtest Analysis ---")
    if max_concurrent_trades > 0:
        print(f"Maximum number of concurrently open trades: {max_concurrent_trades}")
        print(f"This occurred at the start of the following 15-minute candle(s):")
        for t in times_of_max_concurrency:
            print(f"- {t}")
    else:
        print("No trades found or no concurrent trades detected for max concurrency.")

    if max_floating_loss_inr < 0: # Only print if there was an actual floating loss
        print(f"\nMaximum floating loss experienced (sum of P&L of concurrent trades): {max_floating_loss_inr:.2f} INR")
        print(f"This occurred at the start of the following 15-minute candle(s):")
        for t in times_of_max_floating_loss:
            print(f"- {t}")
    else:
        print("\nNo overall floating loss recorded (or P&L remained non-negative).")

    # Original conditional print for max_concurrent_trades, now part of the combined output.
    # if max_concurrent_trades > 0:
    #     print(f"\n--- Maximum Concurrent Trades Analysis ---")
        # This section is now part of the combined output above.
        # print(f"Maximum number of concurrently open trades: {max_concurrent_trades}")
        # print(f"This occurred at the start of the following 15-minute candle(s):")
        # for t in times_of_max_concurrency:
        #     print(f"- {t}")
    # else:
        # print("No trades found or no concurrent trades detected.") # Covered by new output structure

if __name__ == '__main__':
    find_max_concurrent_trades()
