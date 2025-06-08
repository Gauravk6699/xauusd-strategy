import pandas as pd
from collections import defaultdict
import datetime

def analyze_concurrent_trades(csv_filepath):
    try:
        df = pd.read_csv(csv_filepath)
    except FileNotFoundError:
        print(f"Error: The file {csv_filepath} was not found.")
        return None, None, None
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return None, None, None

    if df.empty:
        print("The CSV file is empty.")
        return 0, defaultdict(int), defaultdict(lambda: -float('inf'))

    # Ensure 'mae' and 'pnl' are numeric, coercing errors and filling NaNs with 0.0
    df['mae'] = pd.to_numeric(df['mae'], errors='coerce').fillna(0.0)
    df['pnl'] = pd.to_numeric(df['pnl'], errors='coerce').fillna(0.0)

    df['entry_time_dt_temp'] = pd.to_datetime(df['entry_time'], errors='coerce')
    df['exit_time_dt_temp'] = pd.to_datetime(df['exit_time'], errors='coerce')

    all_valid_times = []
    all_valid_times.extend(df['entry_time_dt_temp'].dropna().tolist())
    all_valid_times.extend(df['exit_time_dt_temp'].dropna().tolist())
    
    if not all_valid_times:
        print("Warning: No valid timestamps found. Using current time as log_generation_time.")
        log_generation_time = pd.Timestamp.now(tz='UTC') # Make timezone aware if needed
    else:
        log_generation_time = max(all_valid_times)

    df.drop(columns=['entry_time_dt_temp', 'exit_time_dt_temp'], inplace=True)

    events = []
    for index, row in df.iterrows(): # Using index as a simple unique trade_id
        try:
            entry_dt = pd.to_datetime(row['entry_time'])
            trade_mae = row['mae']
            trade_pnl = row['pnl'] # Read PnL
            trade_id = index 

            if row['status'] == 'STILL_OPEN':
                exit_dt = log_generation_time
            else:
                exit_dt_val = row['exit_time']
                if pd.isna(exit_dt_val) or str(exit_dt_val).strip() == "":
                    print(f"Warning: Missing exit_time for closed trade (ID {trade_id}, Entry: {row['entry_time']}). Skipping.")
                    continue
                exit_dt = pd.to_datetime(exit_dt_val)
            
            events.append({'time': entry_dt, 'type': 1, 'trade_id': trade_id, 'mae': trade_mae, 'pnl': trade_pnl})
            events.append({'time': exit_dt, 'type': -1, 'trade_id': trade_id, 'mae': trade_mae, 'pnl': trade_pnl})
        except Exception as e:
            print(f"Warning: Could not parse/process row (ID {index}, Entry: {row.get('entry_time', 'N/A')}). Error: {e}")
            continue
    
    events.sort(key=lambda x: (x['time'], -x['type'])) # Entries before exits at same time

    if not events:
        print("No processable trade events found.")
        return 0, defaultdict(int), defaultdict(lambda: -float('inf'))

    active_trades_details = {}  # trade_id -> {'mae': mae_value, 'pnl': pnl_value}
    max_open_count = 0
    cluster_formation_counts = defaultdict(int)
    max_mae_sum_for_cluster_size = defaultdict(lambda: -float('inf'))
    all_pnl_sums_for_cluster_size = defaultdict(list) # To store all PnL sums for avg, min, max calculations
    
    open_count_at_end_of_previous_timestamp = 0

    unique_event_times = sorted(list(set(event['time'] for event in events)))

    for event_time in unique_event_times:
        # Process all entry events at this time first
        for event in [e for e in events if e['time'] == event_time and e['type'] == 1]:
            active_trades_details[event['trade_id']] = {'mae': event['mae'], 'pnl': event['pnl']}
        
        # Then process all exit events at this time
        for event in [e for e in events if e['time'] == event_time and e['type'] == -1]:
            if event['trade_id'] in active_trades_details:
                del active_trades_details[event['trade_id']]
            
        current_open_count = len(active_trades_details)
        max_open_count = max(max_open_count, current_open_count)
        
        # Cluster formation count
        if current_open_count != open_count_at_end_of_previous_timestamp:
            if current_open_count > 0:
                cluster_formation_counts[current_open_count] += 1
        
        # Calculations for current state
        if current_open_count > 0:
            current_sum_mae = sum(details['mae'] for details in active_trades_details.values())
            max_mae_sum_for_cluster_size[current_open_count] = max(
                max_mae_sum_for_cluster_size[current_open_count], 
                current_sum_mae
            )
            
            current_sum_pnl = sum(details['pnl'] for details in active_trades_details.values())
            all_pnl_sums_for_cluster_size[current_open_count].append(current_sum_pnl)
            
        open_count_at_end_of_previous_timestamp = current_open_count
            
    return max_open_count, cluster_formation_counts, max_mae_sum_for_cluster_size, all_pnl_sums_for_cluster_size

import csv # Import csv module

if __name__ == "__main__":
    log_file = 'backtest_trade_log.csv'
    output_filename = 'cluster_analysis_report.csv'
    max_trades, cluster_counts, max_mae_sums, all_pnl_sums = analyze_concurrent_trades(log_file)

    with open(output_filename, 'w', newline='') as f: # Add newline='' for csv writer
        csv_writer = csv.writer(f)

        if max_trades is not None:
            # Write summary line as a single cell row or a commented line if preferred
            # For simplicity, writing it as a row that spans multiple columns if opened in excel
            # Or just a simple text line before proper CSV data.
            # Let's write it as a simple text line first, then the CSV table.
            f.write(f"Maximum number of trades open at a single point: {max_trades}\\n\\n") 
            
            header_cols = [
                "Cluster Size", "Times Formed", "Max MAE Sum", 
                "Avg PnL Sum", "Max PnL Sum", "Min Positive PnL", 
                "Max Loss Sum", "Min Loss Sum"
            ]
            csv_writer.writerow(header_cols)

            all_observed_cluster_sizes = set(cluster_counts.keys()) | set(max_mae_sums.keys()) | set(all_pnl_sums.keys())
            
            if not all_observed_cluster_sizes:
                # If no data, we might write a single row message or leave it after header
                csv_writer.writerow(["No cluster data to report."]) 
            else:
                for num_trades in sorted(list(all_observed_cluster_sizes)):
                    row_data = []
                    row_data.append(str(num_trades))
                    row_data.append(str(cluster_counts.get(num_trades, 0)))
                    
                    mae_sum_val = max_mae_sums.get(num_trades)
                    mae_sum_str = f"{mae_sum_val:.2f}" if mae_sum_val is not None and mae_sum_val != -float('inf') else "N/A"
                    row_data.append(mae_sum_str)

                    pnl_sums_list = all_pnl_sums.get(num_trades)
                    if not pnl_sums_list:
                        row_data.extend(["N/A"] * 5) 
                    else:
                        avg_pnl_sum = sum(pnl_sums_list) / len(pnl_sums_list)
                        row_data.append(f"{avg_pnl_sum:.2f}")
                        row_data.append(f"{max(pnl_sums_list):.2f}")
                        
                        positive_pnl_sums = [s for s in pnl_sums_list if s > 0]
                        negative_pnl_sums = [s for s in pnl_sums_list if s < 0]

                        min_positive_pnl_sum_str = f"{min(positive_pnl_sums):.2f}" if positive_pnl_sums else "N/A"
                        row_data.append(min_positive_pnl_sum_str)
                        
                        max_loss_sum_str = f"{min(negative_pnl_sums):.2f}" if negative_pnl_sums else "N/A"
                        row_data.append(max_loss_sum_str)

                        min_loss_sum_str = f"{max(negative_pnl_sums):.2f}" if negative_pnl_sums else "N/A"
                        row_data.append(min_loss_sum_str)
                    
                    csv_writer.writerow(row_data)
        else:
            f.write("Analysis could not be completed.\\n") # This will be a single line in the CSV
    print(f"Analysis complete. Output saved to {output_filename}")
