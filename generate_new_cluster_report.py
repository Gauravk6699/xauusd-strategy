import pandas as pd
from collections import defaultdict
import sqlite3

# Constants based on backtest_silver_strategy.py
PNL_INR_PER_PIP = 44.0
POINT_VALUE = 0.01
DATABASE_PATH = "silver_bot/xagusd_15min_data.db"
PRICE_TABLE_NAME = "xagusd_15min"

def generate_cluster_analysis(trade_log_path="backtest_trade_log.csv", output_path="new_cluster_analysis_report.csv"):
    try:
        trades_df = pd.read_csv(trade_log_path)
    except FileNotFoundError:
        print(f"Error: Trade log file not found at {trade_log_path}")
        return

    try:
        conn = sqlite3.connect(DATABASE_PATH)
        query = f"SELECT timestamp, open, high, low, close FROM {PRICE_TABLE_NAME} ORDER BY timestamp ASC"
        prices_df = pd.read_sql_query(query, conn)
        conn.close()
        prices_df['timestamp'] = pd.to_datetime(prices_df['timestamp'])
    except Exception as e:
        print(f"Error loading price data from {DATABASE_PATH}: {e}")
        return

    if trades_df.empty or prices_df.empty:
        print("Trade log or price data is empty. No analysis to perform.")
        with open(output_path, 'w') as f:
            f.write("Maximum number of trades open at a single point: 0\n")
            f.write("Cluster Size,Times Formed,Max MAE Sum,Avg PnL Sum,Max PnL Sum,Min Positive PnL,Max Loss Sum,Min Loss Sum\n")
        return

    trades_df['entry_datetime'] = pd.to_datetime(trades_df['entry_time'])
    
    last_price_timestamp = prices_df['timestamp'].max()
    # Ensure exit_datetime is timezone-naive if entry_datetime is
    trades_df['exit_datetime'] = trades_df['exit_time'].apply(
        lambda x: pd.to_datetime(x) if x != 'STILL_OPEN' else last_price_timestamp + pd.Timedelta(minutes=1) # Ensure STILL_OPEN trades extend beyond last candle
    )

    if trades_df['entry_datetime'].dt.tz is not None:
        trades_df['entry_datetime'] = trades_df['entry_datetime'].dt.tz_localize(None)
    if trades_df['exit_datetime'].dt.tz is not None:
        trades_df['exit_datetime'] = trades_df['exit_datetime'].dt.tz_localize(None)
    if prices_df['timestamp'].dt.tz is not None:
        prices_df['timestamp'] = prices_df['timestamp'].dt.tz_localize(None)

    candle_observations = [] # Stores (size, floating_pnl_sum, final_mae_sum_of_open_trades) for each candle
    max_concurrent_trades_overall = 0

    for _, candle in prices_df.iterrows():
        candle_time = candle['timestamp']
        candle_close = candle['close']
        
        open_trades_this_candle = trades_df[
            (trades_df['entry_datetime'] <= candle_time) & 
            (trades_df['exit_datetime'] > candle_time)
        ]
        
        num_open = len(open_trades_this_candle)
        max_concurrent_trades_overall = max(max_concurrent_trades_overall, num_open)

        if num_open > 0:
            current_floating_pnl_sum = 0
            current_final_mae_sum = 0
            
            for _, trade in open_trades_this_candle.iterrows():
                floating_pnl = 0
                if trade['trade_type'] == 'LONG':
                    floating_pnl = (candle_close - trade['entry_price']) / POINT_VALUE * PNL_INR_PER_PIP
                elif 'SHORT' in trade['trade_type'].upper(): # Covers SHORT and SHORT_ORIG
                    floating_pnl = (trade['entry_price'] - candle_close) / POINT_VALUE * PNL_INR_PER_PIP
                
                current_floating_pnl_sum += floating_pnl
                current_final_mae_sum += trade['mae_inr'] # Sum of final MAEs from log for open trades
            
            candle_observations.append({
                'size': num_open,
                'floating_pnl_sum': current_floating_pnl_sum,
                'final_mae_sum': current_final_mae_sum
            })

    analysis_by_size = defaultdict(lambda: {
        'times_formed': 0, # Number of candles this size was observed
        'floating_pnl_sums': [],
        'final_mae_sums': []
    })

    for obs in candle_observations:
        size = obs['size']
        analysis_by_size[size]['times_formed'] += 1
        analysis_by_size[size]['floating_pnl_sums'].append(obs['floating_pnl_sum'])
        analysis_by_size[size]['final_mae_sums'].append(obs['final_mae_sum'])

    report_data = []
    sorted_sizes = sorted(analysis_by_size.keys())

    for size in sorted_sizes:
        if size == 0: continue # Skip if somehow a 0-size cluster was recorded
        data = analysis_by_size[size]
        pnl_sums_series = pd.Series(data['floating_pnl_sums'])
        mae_sums_series = pd.Series(data['final_mae_sums'])

        positive_pnl_series = pnl_sums_series[pnl_sums_series > 0]
        loss_pnl_series = pnl_sums_series[pnl_sums_series < 0]

        report_data.append({
            'Cluster Size': size,
            'Times Formed': data['times_formed'],
            'Max MAE Sum': mae_sums_series.max() if not mae_sums_series.empty else 'N/A',
            'Avg PnL Sum': pnl_sums_series.mean() if not pnl_sums_series.empty else 'N/A',
            'Max PnL Sum': pnl_sums_series.max() if not pnl_sums_series.empty else 'N/A',
            'Min Positive PnL': positive_pnl_series.min() if not positive_pnl_series.empty else 'N/A',
            'Max Loss Sum': loss_pnl_series.max() if not loss_pnl_series.empty else 'N/A', 
            'Min Loss Sum': loss_pnl_series.min() if not loss_pnl_series.empty else 'N/A'
        })
    
    report_df = pd.DataFrame(report_data)

    with open(output_path, 'w', newline='') as f:
        f.write(f"Maximum number of trades open at a single point: {max_concurrent_trades_overall}\n")
        
        def format_value(val, is_int=False):
            if pd.isna(val) or val == 'N/A': return 'N/A'
            try:
                if is_int: return f"{int(val):,}"
                return f"{float(val):,.2f}"
            except ValueError: return 'N/A'

        headers = "Cluster Size,Times Formed,Max MAE Sum,Avg PnL Sum,Max PnL Sum,Min Positive PnL,Max Loss Sum,Min Loss Sum"
        f.write(headers + "\n")
        if not report_df.empty:
            for _, row in report_df.iterrows():
                row_values = [
                    format_value(row['Cluster Size'], is_int=True),
                    format_value(row['Times Formed'], is_int=True),
                    format_value(row['Max MAE Sum']),
                    format_value(row['Avg PnL Sum']),
                    format_value(row['Max PnL Sum']),
                    format_value(row['Min Positive PnL']),
                    format_value(row['Max Loss Sum']),
                    format_value(row['Min Loss Sum'])
                ]
                f.write(",".join(row_values) + "\n")
        else: # Handles case where no clusters > 0 size were formed
            pass


    print(f"Cluster analysis report generated at {output_path}")

if __name__ == "__main__":
    generate_cluster_analysis()
