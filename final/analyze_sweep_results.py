import sqlite3
import pandas as pd

# Database path
LOSING_TRADES_DB_PATH = "losing_trades_context.db"
SUMMARY_RESULTS_TABLE_NAME = "backtest_summary_results"

def connect_db(db_path):
    """Establishes a connection to the SQLite database."""
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        print(f"Successfully connected to {db_path}")
    except sqlite3.Error as e:
        print(f"Error connecting to database {db_path}: {e}")
    return conn

def fetch_summary_results(conn):
    """Fetches all data from the backtest_summary_results table."""
    df = pd.DataFrame()
    if conn:
        try:
            query = f"SELECT * FROM {SUMMARY_RESULTS_TABLE_NAME}"
            df = pd.read_sql_query(query, conn)
            print(f"Successfully fetched {len(df)} records from {SUMMARY_RESULTS_TABLE_NAME}.")
        except sqlite3.Error as e:
            print(f"Error fetching data from {SUMMARY_RESULTS_TABLE_NAME}: {e}")
        except Exception as e:
            print(f"An unexpected error occurred during fetch: {e}")
    return df

def analyze_and_display_results(df):
    """Analyzes and displays the sweep results."""
    if df.empty:
        print("No data to analyze.")
        return

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 200) # Adjust width for better console display
    pd.set_option('display.float_format', '{:.2f}'.format) # Format floats to 2 decimal places

    # Filter for robustness: minimum number of trades
    min_trades_threshold = 30
    df_robust = df[df['total_trades'] >= min_trades_threshold].copy() # Use .copy() to avoid SettingWithCopyWarning

    if df_robust.empty:
        print(f"No runs met the minimum trade threshold of {min_trades_threshold}. Displaying raw top results if available.")
        # Fallback to original df if df_robust is empty, but ranking might be less meaningful
        df_to_analyze = df.copy()
        if df_to_analyze.empty:
            print("No data at all to analyze.")
            return
    else:
        df_to_analyze = df_robust
        print(f"\nAnalysis performed on {len(df_to_analyze)} runs with >= {min_trades_threshold} trades.")


    # --- Top 5 by Individual Metrics ---
    print("\n--- Top 5 Results by Total P&L ---")
    df_top_pnl = df_to_analyze.sort_values(by="total_pnl", ascending=False)
    print(df_top_pnl.head(5)[['run_datetime', 'rsi_filter_long', 'rsi_filter_short', 'rr_multiplier', 'total_pnl', 'win_rate', 'total_trades', 'profit_factor', 'max_drawdown_percentage']])

    print("\n--- Top 5 Results by Win Rate ---")
    df_top_win_rate = df_to_analyze.sort_values(by="win_rate", ascending=False)
    print(df_top_win_rate.head(5)[['run_datetime', 'rsi_filter_long', 'rsi_filter_short', 'rr_multiplier', 'win_rate', 'total_pnl', 'total_trades', 'profit_factor']])

    print("\n--- Top 5 Results by Profit Factor ---")
    df_top_profit_factor = df_to_analyze.sort_values(by="profit_factor", ascending=False)
    print(df_top_profit_factor.head(5)[['run_datetime', 'rsi_filter_long', 'rsi_filter_short', 'rr_multiplier', 'profit_factor', 'win_rate', 'total_pnl', 'total_trades']])

    # --- Combined Ranking for Top 5 Overall ---
    print("\n--- Top 5 Overall Best Performing Settings (Combined Rank) ---")
    
    # Create ranks for each metric (higher is better, so ascending=False for value, then rank method handles it)
    # Rank with method='min' to handle ties appropriately (e.g., if two have same P&L, they get same rank)
    df_to_analyze.loc[:, 'pnl_rank'] = df_to_analyze['total_pnl'].rank(method='min', ascending=False)
    df_to_analyze.loc[:, 'win_rate_rank'] = df_to_analyze['win_rate'].rank(method='min', ascending=False)
    df_to_analyze.loc[:, 'profit_factor_rank'] = df_to_analyze['profit_factor'].rank(method='min', ascending=False)
    
    # Calculate composite score (sum of ranks - lower is better)
    df_to_analyze.loc[:, 'composite_rank_score'] = df_to_analyze['pnl_rank'] + df_to_analyze['win_rate_rank'] + df_to_analyze['profit_factor_rank']
    
    df_overall_best = df_to_analyze.sort_values(by="composite_rank_score", ascending=True)
    
    # Ensure 'rr_multiplier' is in the list of columns to print for overall best
    overall_best_cols = ['run_datetime', 'rsi_filter_long', 'rsi_filter_short', 'rr_multiplier', 
                         'total_pnl', 'win_rate', 'profit_factor', 'total_trades', 
                         'composite_rank_score', 'pnl_rank', 'win_rate_rank', 'profit_factor_rank']
    # Filter out any columns that might not exist (e.g. if rr_multiplier wasn't fetched)
    overall_best_cols_to_print = [col for col in overall_best_cols if col in df_overall_best.columns]
    print(df_overall_best.head(5)[overall_best_cols_to_print])

    # Save the full original dataframe sorted by P&L to CSV for detailed review
    try:
        csv_filename_full = "final/sweep_results_full_sorted_by_pnl.csv"
        df_sorted_by_pnl_original = df.sort_values(by="total_pnl", ascending=False)
        df_sorted_by_pnl_original.to_csv(csv_filename_full, index=False)
        print(f"\nFull original results (all runs, sorted by P&L) saved to {csv_filename_full}")
        
        # Optionally, save the ranked dataframe as well
        csv_filename_ranked = "final/sweep_results_ranked_robust.csv"
        df_overall_best.to_csv(csv_filename_ranked, index=False)
        print(f"Robust results with combined ranking saved to {csv_filename_ranked}")

    except Exception as e:
        print(f"Error saving results to CSV: {e}")

def main():
    conn = connect_db(LOSING_TRADES_DB_PATH)
    if conn:
        summary_df = fetch_summary_results(conn)
        conn.close()
        print("Database connection closed.")
        
        if not summary_df.empty:
            analyze_and_display_results(summary_df)
        else:
            print("No summary data was fetched. Cannot perform analysis.")
    else:
        print("Failed to connect to the database. Cannot perform analysis.")

if __name__ == "__main__":
    main()
