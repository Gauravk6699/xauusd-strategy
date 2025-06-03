import sqlite3
import pandas as pd
import json
import numpy as np

# --- CONFIGURATIONS ---
DB_PATH = "losing_trades_context.db"
LOSING_TRADES_TABLE = "losing_trade_details"
PROFITABLE_TRADES_TABLE = "profitable_trade_details"

# Columns containing JSON strings that need deserialization
JSON_COLUMNS = [
    'sr_15min_supports_json', 'sr_15min_resistances_json',
    'sr_4hour_supports_json', 'sr_4hour_resistances_json',
    'pre_trade_data_5min_json', 'pre_trade_data_15min_json',
    'pre_trade_data_4hour_json', 'raw_signal_data_json'
]

# Key numerical indicators from the main table to analyze directly
INDICATOR_COLUMNS = [
    'crossover_candle_rsi', 'crossover_candle_sma_rsi', 'crossover_candle_atr', 'pnl', 'entry_price', 'exit_price'
]

CATEGORICAL_COLUMNS = ['direction', 'trend_15min_at_signal', 'exit_reason']

# --- HELPER FUNCTIONS ---

def deserialize_json_columns(df, json_cols):
    """Deserializes JSON strings in specified columns of a DataFrame."""
    for col in json_cols:
        if col in df.columns:
            # Convert to string first to handle potential non-string types, then handle None
            df[col] = df[col].astype(str).apply(lambda x: json.loads(x) if x not in ['None', 'nan', None] else None)
    return df

def print_descriptive_stats(df, columns, trade_type_description):
    """Prints descriptive statistics for specified numerical columns."""
    print(f"\n--- Descriptive Statistics for Numerical Indicators ({trade_type_description}) ---")
    for col in columns:
        if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
            print(f"\nStatistics for '{col}':")
            # Ensure data is numeric, coercing errors for describe
            numeric_col_data = pd.to_numeric(df[col], errors='coerce').dropna()
            if not numeric_col_data.empty:
                stats = numeric_col_data.describe()
                print(stats)
            else:
                print("No valid numerical data to describe.")
        else:
            print(f"Column '{col}' not found or not numeric.")
    print("--------------------------------------------------")

def analyze_sr_levels(df, trade_type_description):
    """Analyzes deserialized S/R level columns."""
    print(f"\n--- S/R Level Analysis ({trade_type_description}) ---")
    sr_cols_map = {
        'sr_15min_supports_json': '15min Supports', 'sr_15min_resistances_json': '15min Resistances',
        'sr_4hour_supports_json': '4hour Supports', 'sr_4hour_resistances_json': '4hour Resistances'
    }
    for col, desc in sr_cols_map.items():
        if col in df.columns:
            # Calculate average number of S/R levels
            # Ensure items are lists before calling len, handle None
            num_levels = df[col].apply(lambda x: len(x) if isinstance(x, list) else 0)
            print(f"\nAverage number of {desc}: {num_levels.mean():.2f} (Std: {num_levels.std():.2f})")
            print(f"Min/Max number of {desc}: {num_levels.min()}/{num_levels.max()}")
            # Further analysis could involve proximity to entry_price if entry_price is available and S/R levels are numeric
        else:
            print(f"S/R Column '{col}' not found.")
    print("--------------------------------------------------")

def analyze_categorical_data(df, column_name, trade_type_description):
    """Prints value counts and percentages for a categorical column."""
    print(f"\n--- Analysis for Categorical Column: '{column_name}' ({trade_type_description}) ---")
    if column_name in df.columns:
        counts = df[column_name].value_counts()
        percentages = df[column_name].value_counts(normalize=True) * 100
        summary_df = pd.DataFrame({'Counts': counts, 'Percentage (%)': percentages.round(2)})
        print(summary_df)
    else:
        print(f"Column '{column_name}' not found.")
    print("--------------------------------------------------")

def analyze_pre_trade_data_summary(df, trade_type_description):
    """Provides a basic summary of pre-trade candle data availability."""
    print(f"\n--- Pre-Trade Candle Data Summary ({trade_type_description}) ---")
    pre_trade_cols = ['pre_trade_data_5min_json', 'pre_trade_data_15min_json', 'pre_trade_data_4hour_json']
    for col in pre_trade_cols:
        if col in df.columns:
            num_candles = df[col].apply(lambda x: len(x) if isinstance(x, list) else 0)
            print(f"\nSummary for '{col}':")
            print(f"  Average number of pre-trade candles: {num_candles.mean():.2f}")
            print(f"  Trades with any pre-trade data: {num_candles[num_candles > 0].count()}/{len(df)}")
        else:
            print(f"Pre-trade data column '{col}' not found.")
    print("--------------------------------------------------")
    
def analyze_raw_signal_data(df, trade_type_description):
    """Analyzes deserialized raw_signal_data_json."""
    print(f"\n--- Raw Signal Candle Data Analysis ({trade_type_description}) ---")
    if 'raw_signal_data_json' in df.columns and not df['raw_signal_data_json'].isnull().all():
        # Assuming raw_signal_data_json contains a dict similar to a candle
        try:
            signal_candles_df = pd.DataFrame(df['raw_signal_data_json'].dropna().tolist())
            # Select only numeric columns that are typically in candle data
            numeric_signal_cols = signal_candles_df.select_dtypes(include=np.number).columns.tolist()
            if numeric_signal_cols:
                 print_descriptive_stats(signal_candles_df, numeric_signal_cols, f"Signal Candle - {trade_type_description}")
            else:
                print("No numeric columns found in deserialized raw_signal_data_json.")
        except Exception as e:
            print(f"Could not process raw_signal_data_json: {e}")
    else:
        print("No 'raw_signal_data_json' data available or column not found.")
    print("--------------------------------------------------")


# --- MAIN ANALYSIS FUNCTION ---

def perform_group_analysis(conn, table_name, trade_type_description):
    """Performs and prints analysis for a specific group of trades."""
    print(f"\n\n{'='*30} ANALYSIS FOR {trade_type_description.upper()} {'='*30}")
    
    try:
        df_group = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    except Exception as e:
        print(f"Error fetching data from {table_name}: {e}")
        return

    if df_group.empty:
        print(f"No data found for {trade_type_description} in table '{table_name}'.")
        return

    print(f"\nTotal {trade_type_description}: {len(df_group)}")
    
    # Basic DataFrame info (optional, can be verbose)
    # print("\nDataFrame Info:")
    # df_group.info(verbose=True, show_counts=True)

    # Deserialize JSON columns
    df_group = deserialize_json_columns(df_group, JSON_COLUMNS)

    # Analyze numerical indicators from the main table
    print_descriptive_stats(df_group, INDICATOR_COLUMNS, trade_type_description)

    # Analyze categorical columns
    for cat_col in CATEGORICAL_COLUMNS:
        analyze_categorical_data(df_group, cat_col, trade_type_description)
        
    # Analyze S/R levels
    analyze_sr_levels(df_group, trade_type_description)
    
    # Analyze Pre-trade Data Summary
    analyze_pre_trade_data_summary(df_group, trade_type_description)

    # Analyze Raw Signal Data (deserialized from JSON)
    analyze_raw_signal_data(df_group, trade_type_description)
    
    print(f"\n{'='*30} END OF ANALYSIS FOR {trade_type_description.upper()} {'='*30}")

# --- MAIN EXECUTION BLOCK ---

if __name__ == "__main__":
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        print(f"Successfully connected to database: {DB_PATH}")

        # Analyze Losing Trades
        perform_group_analysis(conn, LOSING_TRADES_TABLE, "Losing Trades")

        print("\n\n" + "#"*80 + "\n\n") # Separator between groups

        # Analyze Profitable Trades
        perform_group_analysis(conn, PROFITABLE_TRADES_TABLE, "Profitable Trades")

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()
            print(f"\nDatabase connection to {DB_PATH} closed.")
    
    print("\nAnalysis script finished.")
