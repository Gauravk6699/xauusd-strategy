import sqlite3
import pandas as pd
import json
from collections import Counter

# Database path
LOSING_TRADES_DB_PATH = "losing_trades_context.db"
LOSING_TRADES_TABLE_NAME = "losing_trade_details"

def connect_db(db_path):
    """Establishes a connection to the SQLite database."""
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        print(f"Successfully connected to {db_path}")
    except sqlite3.Error as e:
        print(f"Error connecting to database {db_path}: {e}")
    return conn

def fetch_losing_trades(conn):
    """Fetches all records from the losing_trade_details table."""
    try:
        df = pd.read_sql_query(f"SELECT * FROM {LOSING_TRADES_TABLE_NAME}", conn)
        print(f"Fetched {len(df)} losing trades from the database.")
        return df
    except Exception as e:
        print(f"Error fetching losing trades: {e}")
        return pd.DataFrame()

def deserialize_json_columns(df):
    """Deserializes JSON string columns into Python objects (lists/dicts)."""
    json_columns = [
        'sr_15min_supports_json', 'sr_15min_resistances_json',
        'sr_4hour_supports_json', 'sr_4hour_resistances_json',
        'pre_trade_data_5min_json', 'pre_trade_data_15min_json',
        'pre_trade_data_4hour_json', 'raw_signal_data_json'
    ]
    for col in json_columns:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: json.loads(x) if pd.notna(x) and isinstance(x, str) else None)
    return df

def analyze_common_conditions(df_losing_trades):
    """
    Performs fact-based analysis to identify common conditions in losing trades.
    """
    if df_losing_trades.empty:
        print("No losing trades to analyze.")
        return

    print("\n--- Losing Trade Analysis ---")

    # 1. Distribution of losses by 15-min trend at signal
    if 'trend_15min_at_signal' in df_losing_trades.columns:
        trend_counts = df_losing_trades['trend_15min_at_signal'].value_counts(normalize=True) * 100
        print("\n1. Distribution of Losses by 15-min Trend at Signal:")
        for trend, percentage in trend_counts.items():
            print(f"   - {trend.capitalize()} Trend: {percentage:.2f}% of losing trades")

    # 2. Distribution of losses by exit reason
    if 'exit_reason' in df_losing_trades.columns:
        exit_reason_counts = df_losing_trades['exit_reason'].value_counts(normalize=True) * 100
        print("\n2. Distribution of Losses by Exit Reason:")
        for reason, percentage in exit_reason_counts.items():
            print(f"   - {reason}: {percentage:.2f}% of losing trades")

    # 3. Summary of RSI and SMA_RSI at crossover for losing trades
    if 'crossover_candle_rsi' in df_losing_trades.columns and 'crossover_candle_sma_rsi' in df_losing_trades.columns:
        print("\n3. RSI and SMA_RSI at Crossover (Losing Trades):")
        print(f"   - Average RSI: {df_losing_trades['crossover_candle_rsi'].mean():.2f} (Std: {df_losing_trades['crossover_candle_rsi'].std():.2f})")
        print(f"   - Average SMA_RSI: {df_losing_trades['crossover_candle_sma_rsi'].mean():.2f} (Std: {df_losing_trades['crossover_candle_sma_rsi'].std():.2f})")
        
        long_losses = df_losing_trades[df_losing_trades['direction'] == 'long']
        short_losses = df_losing_trades[df_losing_trades['direction'] == 'short']

        if not long_losses.empty:
            print(f"   - Long Losses - Average RSI: {long_losses['crossover_candle_rsi'].mean():.2f}")
        if not short_losses.empty:
            print(f"   - Short Losses - Average RSI: {short_losses['crossover_candle_rsi'].mean():.2f}")


    # 4. Proximity to S/R Levels (simplified: count trades near any S/R)
    # This requires more complex logic to define "near" based on ATR or fixed pips.
    # For a simple factual analysis, let's count how many trades had S/R levels defined.
    sr_defined_15m_support = df_losing_trades['sr_15min_supports_json'].apply(lambda x: bool(x)).sum()
    sr_defined_15m_resistance = df_losing_trades['sr_15min_resistances_json'].apply(lambda x: bool(x)).sum()
    sr_defined_4h_support = df_losing_trades['sr_4hour_supports_json'].apply(lambda x: bool(x)).sum()
    sr_defined_4h_resistance = df_losing_trades['sr_4hour_resistances_json'].apply(lambda x: bool(x)).sum()
    
    print("\n4. Presence of S/R Levels in Losing Trades Context:")
    print(f"   - 15-min Supports defined for: {sr_defined_15m_support}/{len(df_losing_trades)} trades ({sr_defined_15m_support/len(df_losing_trades)*100:.2f}%)")
    print(f"   - 15-min Resistances defined for: {sr_defined_15m_resistance}/{len(df_losing_trades)} trades ({sr_defined_15m_resistance/len(df_losing_trades)*100:.2f}%)")
    print(f"   - 4-hour Supports defined for: {sr_defined_4h_support}/{len(df_losing_trades)} trades ({sr_defined_4h_support/len(df_losing_trades)*100:.2f}%)")
    print(f"   - 4-hour Resistances defined for: {sr_defined_4h_resistance}/{len(df_losing_trades)} trades ({sr_defined_4h_resistance/len(df_losing_trades)*100:.2f}%)")

    # 5. Analysis of pre-trade 5-min candle data (e.g., average RSI leading up to loss)
    if 'pre_trade_data_5min_json' in df_losing_trades.columns:
        avg_rsi_pre_trade_list = []
        for idx, row in df_losing_trades.iterrows():
            if row['pre_trade_data_5min_json']:
                pre_trade_df = pd.DataFrame(row['pre_trade_data_5min_json'])
                if 'rsi' in pre_trade_df.columns and not pre_trade_df['rsi'].empty:
                    avg_rsi_pre_trade_list.append(pre_trade_df['rsi'].mean())
        
        if avg_rsi_pre_trade_list:
            avg_rsi_overall = pd.Series(avg_rsi_pre_trade_list).mean()
            std_rsi_overall = pd.Series(avg_rsi_pre_trade_list).std()
            print("\n5. Pre-Trade 5-min Data (Average RSI of ~50 candles before crossover):")
            print(f"   - Overall Average RSI in pre-trade window: {avg_rsi_overall:.2f} (Std: {std_rsi_overall:.2f})")

    # Further analysis ideas:
    # - Time of day for losing trades (requires parsing 'entry_datetime')
    # - ATR values at crossover for losing trades
    # - Specific candle patterns in pre-trade data (more complex, requires pattern recognition logic)

def main():
    """Main function to run the analysis."""
    # Connect to the database
    conn = connect_db(LOSING_TRADES_DB_PATH)
    if conn is None:
        return

    try:
        # Fetch losing trades
        df_losing_trades = fetch_losing_trades(conn)
        if df_losing_trades.empty:
            return

        # Deserialize JSON columns
        df_losing_trades = deserialize_json_columns(df_losing_trades)

        # Perform analysis
        analyze_common_conditions(df_losing_trades)

    finally:
        if conn:
            conn.close()
            print(f"\nDisconnected from {LOSING_TRADES_DB_PATH}")

if __name__ == "__main__":
    main()
