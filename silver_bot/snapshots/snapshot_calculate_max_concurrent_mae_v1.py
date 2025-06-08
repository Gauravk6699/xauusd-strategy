import pandas as pd
import sqlite3
from datetime import datetime

# Configuration
TRADE_LOG_PATH = "backtest_trade_log.csv"
DATABASE_PATH = "silver_bot/xagusd_15min_data.db"
PRICE_TABLE_NAME = "xagusd_15min"
CONTRACT_SIZE = 5000 # Assuming 1 lot = 5000 units for P&L calculation, as per strategy context

def calculate_max_concurrent_floating_loss():
    # Load trade log
    try:
        trades_df = pd.read_csv(TRADE_LOG_PATH)
    except FileNotFoundError:
        print(f"Error: Trade log file not found at {TRADE_LOG_PATH}")
        return

    # Load price data
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        # Query to ensure correct data types, especially for timestamp
        query = f"SELECT timestamp, open, high, low, close FROM {PRICE_TABLE_NAME} ORDER BY timestamp ASC"
        prices_df = pd.read_sql_query(query, conn)
        conn.close()
    except Exception as e:
        print(f"Error loading price data from {DATABASE_PATH}: {e}")
        return

    if trades_df.empty:
        print("Trade log is empty.")
        return
    if prices_df.empty:
        print("Price data is empty.")
        return

    # Preprocess data
    # Trades DataFrame
    trades_df['entry_datetime'] = pd.to_datetime(trades_df['entry_time']) # Corrected column name
    
    # Handle potential NaT for exit_time if a trade is still open
    # The backtest output shows the last trade as "STILL_OPEN" for exit_time_disp (actual column: exit_time)
    # For calculation, we'll treat 'STILL_OPEN' as open until the last price data point.
    # pd.to_datetime will convert "STILL_OPEN" to NaT if not handled.
    # We'll replace NaT exit times with a time far in the future or the end of our price series.
    
    # Convert exit_time to datetime, coercing errors for non-date strings like 'STILL_OPEN'
    trades_df['exit_datetime'] = pd.to_datetime(trades_df['exit_time'], errors='coerce') # Corrected column name

    # Prices DataFrame
    prices_df['timestamp'] = pd.to_datetime(prices_df['timestamp'])
    
    # If there are trades with NaT exit_datetime (e.g. STILL_OPEN), set their exit to be after the last price point
    if not prices_df.empty:
        end_of_data_time = prices_df['timestamp'].max() + pd.Timedelta(minutes=15) # Ensure it's after the last candle
        trades_df['exit_datetime'] = trades_df['exit_datetime'].fillna(end_of_data_time)
    else: # Should not happen if checks above pass, but as a fallback
        print("Cannot determine end of data time as price data is empty after processing.")
        # Fallback: use current time if no price data, though this scenario is unlikely
        trades_df['exit_time'] = trades_df['exit_time'].fillna(pd.Timestamp.now() + pd.Timedelta(days=1))


    min_concurrent_pnl = 0  # Max loss will be the most negative P&L sum
    
    # Iterate through each 15-minute candle
    for _, candle in prices_df.iterrows():
        candle_time = candle['timestamp']
        candle_low = candle['low']
        
        current_concurrent_pnl = 0
        
        # Find trades open during this candle
        # A trade is open if: entry_datetime <= candle_time < exit_datetime
        # Note: candle_time is the START of the 15-min interval.
        # A trade exited at candle_time is considered closed for that candle.
        open_trades = trades_df[
            (trades_df['entry_datetime'] <= candle_time) & 
            (trades_df['exit_datetime'] > candle_time)
        ]
        
        if not open_trades.empty:
            for _, trade in open_trades.iterrows():
                # Assuming all trades are LONG as per strategy context
                # Floating P&L at the low of the current candle
                # MAE is adverse, so for a long, it's entry - low (if low < entry)
                # We want the sum of current losses.
                # If current low is below entry, it's a loss.
                floating_loss_for_trade = (candle_low - trade['entry_price']) * CONTRACT_SIZE * trade['size']
                current_concurrent_pnl += floating_loss_for_trade
                                
        if current_concurrent_pnl < min_concurrent_pnl:
            min_concurrent_pnl = current_concurrent_pnl
            
    print(f"Maximum concurrent floating loss (sum of MAE for simultaneously open trades): {min_concurrent_pnl:.2f}")

if __name__ == "__main__":
    calculate_max_concurrent_floating_loss()
