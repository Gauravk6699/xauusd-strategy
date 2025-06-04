import sqlite3
import pandas as pd
from datetime import datetime

DATABASE_NAME = "silver_bot/xagusd_15min_data.db"
TABLE_NAME = "xagusd_15min"

# Long Strategy Parameters
ENTRY_THRESHOLD_PERCENT = 0.005 # 0.5% drop from day's open
TAKE_PROFIT_PRICE_OFFSET = 0.5   # $0.5 rise from entry price
SWAP_COST_PER_LOT_PER_NIGHT = -22.0 # Daily swap cost for long positions

# General Parameters
STARTING_BALANCE = 100000.0
CONTRACT_SIZE = 5000  # Renamed from CONTRACT_PRICE_MULTIPLIER_PER_LOT
NUMBER_OF_LOTS = 1  # For testing (used for both long and short if size is 1 lot)
SPREAD_COST_PER_LOT = 0.0 # Spread cost removed as per user request (was -210.0)

def fetch_data_from_db():
    """Fetches historical data from the SQLite database."""
    conn = sqlite3.connect(DATABASE_NAME)
    try:
        df = pd.read_sql_query(f"SELECT timestamp, open, high, low, close FROM {TABLE_NAME} ORDER BY timestamp ASC", conn)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df
    except Exception as e:
        print(f"Error fetching data from database: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def run_backtest(df):
    """Runs the backtesting simulation."""
    if df.empty:
        print("No data to backtest.")
        return [], STARTING_BALANCE, 0.0, 0.0

    trades = []
    open_long_positions = []
    # open_short_positions = [] # Removed
    equity = STARTING_BALANCE
    peak_equity = STARTING_BALANCE
    max_drawdown = 0.0
    max_concurrent_mae = 0.0
    daily_open_price = None
    current_day_str = None
    # first_ever_open_price is not used by the current short logic, but kept for potential future use or other strategies
    # if not df.empty:
    #     first_ever_open_price = df['open'].iloc[0] 
    #     print(f"Using first ever open price for short strategy reference: {first_ever_open_price:.5f}")

    print(f"Starting backtest with {len(df)} candles...")

    for index, row in df.iterrows():
        candle_timestamp = row['timestamp']
        candle_open = row['open']
        candle_high = row['high']
        candle_low = row['low']
        candle_close = row['close'] # Explicitly get candle_close

        row_day_str = candle_timestamp.strftime("%Y-%m-%d")
        if current_day_str != row_day_str:
            daily_open_price = candle_open
            current_day_str = row_day_str
            print(f"\nNew trading day: {current_day_str}, Daily Open: {daily_open_price:.5f}")

        if daily_open_price is None:
            continue

        # --- Manage Open Long Positions ---
        for position in list(open_long_positions):
            adverse_move_long = (position['entry_price'] - candle_low) * CONTRACT_SIZE * position['size']
            position['mae'] = max(position.get('mae', 0.0), adverse_move_long)

            if candle_high >= position['tp_price']:
                exit_price = position['tp_price']
                entry_date = position['entry_time'].date()
                exit_date = candle_timestamp.date()
                days_held = (exit_date - entry_date).days
                if days_held < 0: days_held = 0
                
                swap_charges = days_held * SWAP_COST_PER_LOT_PER_NIGHT * position['size']
                gross_pnl = (exit_price - position['entry_price']) * CONTRACT_SIZE * position['size']
                trade_spread_cost = SPREAD_COST_PER_LOT * position['size']
                net_pnl = gross_pnl + swap_charges + trade_spread_cost

                trades.append({
                    "trade_type": "LONG", "entry_time": position['entry_time'], "entry_price": position['entry_price'],
                    "exit_time": candle_timestamp, "exit_price": exit_price, "gross_pnl": gross_pnl,
                    "swap_charges": swap_charges, "spread_cost": trade_spread_cost, "mae": position.get('mae', 0.0),
                    "pnl": net_pnl, "size": position['size'], "status": "CLOSED_TP"
                })
                print(f"  LONG TP HIT: PosID {position['id']} exited at {exit_price:.5f} (Entry: {position['entry_price']:.5f}, Gross PnL: {gross_pnl:.2f}, Swap: {swap_charges:.2f}, Spread: {trade_spread_cost:.2f}, MAE: {position.get('mae', 0.0):.2f}, Net PnL: {net_pnl:.2f})")
                open_long_positions.remove(position)
                equity += net_pnl
                current_drawdown = (peak_equity - equity) / peak_equity if peak_equity > 0 and equity < peak_equity else 0.0
                max_drawdown = max(max_drawdown, current_drawdown)
                peak_equity = max(peak_equity, equity)

        # --- Calculate Current Concurrent MAE ---
        momentary_total_adverse_excursion = 0.0
        for pos_long in open_long_positions:
            adverse_excursion_long = (pos_long['entry_price'] - candle_low) * CONTRACT_SIZE * pos_long['size']
            momentary_total_adverse_excursion += max(0.0, adverse_excursion_long)
        # for pos_short in open_short_positions: # Removed short MAE calculation
        #     adverse_excursion_short = (candle_high - pos_short['entry_price']) * CONTRACT_SIZE * pos_short['size']
        #     momentary_total_adverse_excursion += max(0.0, adverse_excursion_short)
        max_concurrent_mae = max(max_concurrent_mae, momentary_total_adverse_excursion)

        # --- Check for New Long Entry Signals ---
        entries_today_count = sum(1 for pos in open_long_positions if pos.get('daily_open_ref') == daily_open_price)
        next_entry_target_price = daily_open_price * (1 - (entries_today_count + 1) * ENTRY_THRESHOLD_PERCENT)

        if candle_low <= next_entry_target_price:
            entry_price = next_entry_target_price
            tp_price = entry_price + TAKE_PROFIT_PRICE_OFFSET
            position_id = f"Pos-{len(trades) + len(open_long_positions) + 1}-{candle_timestamp.strftime('%H%M%S')}" # Removed len(open_short_positions)
            
            new_long_position = {
                "id": position_id, "trade_type": "LONG", "entry_time": candle_timestamp,
                "entry_price": entry_price, "tp_price": tp_price, "size": NUMBER_OF_LOTS,
                "status": "OPEN", "daily_open_ref": daily_open_price, "mae": 0.0
            }
            initial_mae_long = (new_long_position['entry_price'] - candle_low) * CONTRACT_SIZE * new_long_position['size']
            new_long_position['mae'] = max(0.0, initial_mae_long)
            open_long_positions.append(new_long_position)
            print(f"  NEW LONG: PosID {position_id} opened at {entry_price:.5f} (TP: {tp_price:.5f}, Lots: {NUMBER_OF_LOTS}) based on DailyOpen {daily_open_price:.5f}. Entries today: {entries_today_count + 1}")

            # --- Short entry logic removed ---
            
            # Same-candle TP check for the new long position
            if candle_high >= tp_price: 
                exit_price_immediate = tp_price
                swap_charges_immediate = 0.0 
                trade_spread_cost_immediate = SPREAD_COST_PER_LOT * NUMBER_OF_LOTS
                gross_pnl_immediate = (exit_price_immediate - entry_price) * CONTRACT_SIZE * NUMBER_OF_LOTS
                net_pnl_immediate = gross_pnl_immediate + swap_charges_immediate + trade_spread_cost_immediate
                mae_immediate = new_long_position['mae'] 
                trades.append({
                    "trade_type": "LONG", "entry_time": candle_timestamp, "entry_price": entry_price,
                    "exit_time": candle_timestamp, "exit_price": exit_price_immediate, "gross_pnl": gross_pnl_immediate,
                    "swap_charges": swap_charges_immediate, "spread_cost": trade_spread_cost_immediate,
                    "mae": mae_immediate, "pnl": net_pnl_immediate, "size": NUMBER_OF_LOTS, "status": "CLOSED_TP_SAME_CANDLE"
                })
                print(f"  LONG TP HIT (Same Candle): PosID {position_id} exited at {exit_price_immediate:.5f} (Gross PnL: {gross_pnl_immediate:.2f}, Swap: {swap_charges_immediate:.2f}, Spread: {trade_spread_cost_immediate:.2f}, MAE: {mae_immediate:.2f}, Net PnL: {net_pnl_immediate:.2f})")
                open_long_positions.remove(new_long_position)
                equity += net_pnl_immediate
                current_drawdown_long_tp = (peak_equity - equity) / peak_equity if peak_equity > 0 and equity < peak_equity else 0.0
                max_drawdown = max(max_drawdown, current_drawdown_long_tp)
                peak_equity = max(peak_equity, equity)

    # At the end of backtest, mark remaining open positions
    if not df.empty:
        last_candle_timestamp = df['timestamp'].iloc[-1]
        last_close_price = df['close'].iloc[-1]
        
        for pos in open_long_positions:
            entry_date = pos['entry_time'].date()
            last_data_date = last_candle_timestamp.date()
            days_held = (last_data_date - entry_date).days
            if days_held < 0: days_held = 0
            swap_charges = days_held * SWAP_COST_PER_LOT_PER_NIGHT * pos['size']
            gross_pnl = (last_close_price - pos['entry_price']) * CONTRACT_SIZE * pos['size']
            trade_spread_cost = SPREAD_COST_PER_LOT * pos['size']
            net_pnl = gross_pnl + swap_charges + trade_spread_cost
            trades.append({
                "trade_type": "LONG", "entry_time": pos['entry_time'], "entry_price": pos['entry_price'],
                "exit_time": "STILL_OPEN", "exit_price": last_close_price, "gross_pnl": gross_pnl,
                "swap_charges": swap_charges, "spread_cost": trade_spread_cost, "mae": pos.get('mae', 0.0),
                "pnl": net_pnl, "size": pos['size'], "status": "STILL_OPEN"
            })
            print(f"  END OF BACKTEST (LONG): PosID {pos['id']} still open. Entry: {pos['entry_price']:.5f}, Current Price: {last_close_price:.5f}, Unrealized Gross PnL: {gross_pnl:.2f}, Swap: {swap_charges:.2f}, Spread: {trade_spread_cost:.2f}, MAE: {pos.get('mae', 0.0):.2f}, Unrealized Net PnL: {net_pnl:.2f}")

        # for pos in open_short_positions: # Removed processing of open short positions
        #     # ...
    
    return trades, equity, max_drawdown, max_concurrent_mae

def print_results(trades_history, starting_balance, final_equity, max_drawdown_value, max_concurrent_mae_value):
    file_output_lines = []
    if not trades_history:
        no_trades_messages = [
            "\nNo trades were made during the backtest.",
            f"Starting Balance: ${starting_balance:,.2f}",
            f"Ending Equity: ${starting_balance:,.2f}",
            f"Net P&L: $0.00",
            f"Maximum Drawdown: 0.00%",
            f"Max Concurrent MAE: $0.00"
        ]
        for msg in no_trades_messages:
            print(msg)
            file_output_lines.append(msg)
        output_file_name = "analysis_matrix_output.txt"
        try:
            with open(output_file_name, "w") as f:
                f.write("\n".join(file_output_lines).lstrip())
            print(f"\nFull analysis matrix also saved to {output_file_name}")
        except Exception as e:
            print(f"\nError saving analysis matrix to {output_file_name}: {e}")
        return

    df_trades = pd.DataFrame(trades_history)
    total_trades = len(df_trades)
    closed_trades_df = df_trades[df_trades['status'] != 'STILL_OPEN']
    num_closed_trades = len(closed_trades_df)
    still_open_trades_df = df_trades[df_trades['status'] == 'STILL_OPEN']
    num_still_open = len(still_open_trades_df)
    total_net_pnl = df_trades['pnl'].sum() 
    total_gross_pnl = df_trades['gross_pnl'].sum() if 'gross_pnl' in df_trades.columns else 0
    total_swap_charges = df_trades['swap_charges'].sum() if 'swap_charges' in df_trades.columns else 0
    total_spread_costs = df_trades['spread_cost'].sum() if 'spread_cost' in df_trades.columns else 0
    
    results_summary = [
        "\n--- Backtest Results ---",
        f"Starting Balance: ${starting_balance:,.2f}",
        f"Ending Equity: ${final_equity:,.2f}",
        f"Net P&L (from equity change): ${final_equity - starting_balance:,.2f}",
        f"Maximum Drawdown: {max_drawdown_value * 100:.2f}%",
        f"Max Concurrent MAE: ${max_concurrent_mae_value:,.2f}",
        "\n--- Trade Performance ---",
        f"Total Trades Recorded (incl. open): {total_trades}",
        f"Total Gross P&L (all trades): ${total_gross_pnl:,.2f}",
        f"Total Swap Charges (all trades): ${total_swap_charges:,.2f}",
        f"Total Spread Costs (all trades): ${total_spread_costs:,.2f}",
        f"Total Net P&L (all trades, from log): ${total_net_pnl:,.2f}",
        f"\nNumber of Closed Trades: {num_closed_trades}"
    ]
    for line in results_summary:
        print(line)
        file_output_lines.append(line)

    if num_closed_trades > 0:
        num_closed_winning = closed_trades_df[closed_trades_df['pnl'] > 0]['pnl'].count()
        win_rate_closed = (num_closed_winning / num_closed_trades) * 100 if num_closed_trades > 0 else 0
        avg_net_pnl_closed = closed_trades_df['pnl'].mean()
        avg_gross_pnl_closed = closed_trades_df['gross_pnl'].mean()
        avg_swap_closed = closed_trades_df['swap_charges'].mean()
        avg_spread_closed = closed_trades_df['spread_cost'].mean()
        avg_mae_closed = closed_trades_df['mae'].mean()

        closed_trade_stats = [
            f"  Winning Closed Trades (Net P&L > 0): {num_closed_winning}",
            f"  Win Rate (Closed Trades): {win_rate_closed:.2f}%",
            f"  Average Gross P&L per Closed Trade: ${avg_gross_pnl_closed:,.2f}",
            f"  Average Swap Cost per Closed Trade: ${avg_swap_closed:.2f}",
            f"  Average Spread Cost per Closed Trade: ${avg_spread_closed:.2f}",
            f"  Average MAE per Closed Trade: ${avg_mae_closed:.2f}",
            f"  Average Net P&L per Closed Trade: ${avg_net_pnl_closed:.2f}"
        ]
        for line in closed_trade_stats:
            print(line)
            file_output_lines.append(line)
    
    open_trade_summary = [f"\nNumber of Trades Still Open: {num_still_open}"]
    if num_still_open > 0:
        unrealized_net_pnl_open = still_open_trades_df['pnl'].sum()
        open_trade_summary.append(f"  Unrealized Net P&L for Open Trades: ${unrealized_net_pnl_open:.2f}")
    for line in open_trade_summary:
        print(line)
        file_output_lines.append(line)
    
    file_output_lines.append("\n--- Detailed Trade Log ---")
    print("\n--- Detailed Trade Log ---")
    
    if 'entry_time' in df_trades.columns:
      df_trades['entry_time_disp'] = df_trades['entry_time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M') if isinstance(x, datetime) else x)
    if 'exit_time' in df_trades.columns:
      df_trades['exit_time_disp'] = df_trades['exit_time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M') if isinstance(x, datetime) else x)
    
    display_cols = ['trade_type', 'entry_time_disp', 'entry_price', 'exit_time_disp', 'exit_price', 'size', 'gross_pnl', 'swap_charges', 'spread_cost', 'mae', 'pnl', 'status']
    final_display_cols = [col for col in display_cols if col in df_trades.columns or col.replace('_disp','') in df_trades.columns]
    if 'trade_type' in df_trades.columns:
        cols_for_print = ['trade_type'] + [col for col in final_display_cols if col != 'trade_type']
    else:
        cols_for_print = final_display_cols
    
    df_string_for_file = df_trades[cols_for_print].to_string(max_rows=None, float_format='{:,.2f}'.format, line_width=1200)
    file_output_lines.append(df_string_for_file)
    with pd.option_context('display.max_rows', None, 'display.width', 1200, 'display.float_format', '{:,.2f}'.format):
        print(df_trades[cols_for_print])

    output_file_name = "analysis_matrix_output.txt"
    try:
        with open(output_file_name, "w") as f:
            f.write("\n".join(file_output_lines).lstrip())
        print(f"\nFull analysis matrix also saved to {output_file_name}")
    except Exception as e:
        print(f"\nError saving analysis matrix to {output_file_name}: {e}")

    csv_filename = "backtest_trade_log.csv"
    try:
        csv_cols = ['trade_type', 'entry_time', 'entry_price', 'exit_time', 'exit_price', 'size', 
                    'gross_pnl', 'swap_charges', 'spread_cost', 'mae', 'pnl', 'status']
        cols_to_save = [col for col in csv_cols if col in df_trades.columns]
        df_trades_to_save = df_trades[cols_to_save].copy()
        if 'mae' in df_trades_to_save.columns:
             df_trades_to_save['mae'] = df_trades_to_save['mae'].abs()
        df_trades_to_save.to_csv(csv_filename, index=False, float_format='%.5f')
        print(f"\nDetailed trade log saved to {csv_filename}")
    except Exception as e:
        print(f"\nError saving trade log to CSV {csv_filename}: {e}")

def main():
    print("Fetching data for backtest...")
    historical_data_df = fetch_data_from_db()
    
    if not historical_data_df.empty:
        print(f"Data fetched. Rows: {len(historical_data_df)}, Columns: {historical_data_df.columns.tolist()}")
        print(f"Sample data:\n{historical_data_df.head()}")
        
        print("\nRunning backtest...")
        trades_log, final_equity, max_drawdown_result, max_concurrent_mae_result = run_backtest(historical_data_df)
        print_results(trades_log, STARTING_BALANCE, final_equity, max_drawdown_result, max_concurrent_mae_result)
    else:
        print("Could not fetch data. Aborting backtest.")

if __name__ == "__main__":
    main()
