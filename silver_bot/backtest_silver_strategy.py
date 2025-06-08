import sqlite3
import pandas as pd
from datetime import datetime

DATABASE_NAME = "silver_bot/xagusd_15min_data.db"
TABLE_NAME = "xagusd_15min"

# --- NEW FINANCIAL PARAMETERS (INR BASED) ---
LOT_SIZE_ACTUAL = 0.01
FEE_INR_PER_TRADE = 88.74  # Fee for a complete round trip trade
SWAP_LONG_INR_PER_NIGHT = -19.97  # For 0.01 lot
SWAP_SHORT_INR_PER_NIGHT = 0.0    # For 0.01 lot
MARGIN_INR_PER_TRADE = 79.46    # Informational
PNL_INR_PER_PIP = 44.0          # For 0.01 lot
POINT_VALUE = 0.01              # The smallest price change that constitutes 1 pip (e.g., 30.00 to 30.01)
STARTING_BALANCE_INR = 100000.0 # Starting balance in INR

# --- STRATEGY PARAMETERS (Keep these as they define strategy behavior in terms of price levels) ---
# Long Strategy Parameters
ENTRY_THRESHOLD_PERCENT = 0.005 # 0.5% drop from day's open for the FIRST trade
LONG_ENTRY_DOLLAR_STEP = 0.5    # Dollar step for SUBSEQUENT long entries below the PREVIOUS long entry
TAKE_PROFIT_PRICE_OFFSET = 0.5   # $0.5 rise from entry price (price units)

# Short Strategy Parameters (Original - opens with every long)
SHORT_LOT_SIZE_FRACTION_OF_LONG = 1.0 # This implies short lot will also be 0.01 if long is 0.01
SHORT_TAKE_PROFIT_PRICE_OFFSET = 1.0  # Price units
SHORT_ENTRY_PRICE_OFFSET_FROM_LONG = 2.0 # Price units (Reverted to 2.0 as per new request)

# Cluster-Initiated Hedge Short Strategy Parameters (COMMENTED OUT)
# HEDGE_SHORT_FIXED_LOT_SIZE = 1.5
# HEDGE_SHORT_TAKE_PROFIT_PRICE_OFFSET = 3.0
# HEDGE_SHORT_STOP_LOSS_PRICE_OFFSET = 0.5
# # HEDGE_SHORT_ENTRY_PRICE_OFFSET_FROM_LONG = 0.0 (Implicit, as it's same as long entry)

# --- OLD/OBSOLETE PARAMETERS (Review and remove/replace) ---
# SWAP_COST_PER_LOT_PER_NIGHT = -22.0 # USD based, replaced by SWAP_LONG_INR_PER_NIGHT
# CONTRACT_SIZE = 5000  # To be replaced by PNL_INR_PER_PIP and POINT_VALUE for P&L and MAE
# NUMBER_OF_LOTS = 1  # Replaced by LOT_SIZE_ACTUAL for trade entries
SPREAD_COST_PER_LOT = 0.0 # Spread cost removed as per user request (was -210.0) - Will be zero fee
# STARTING_BALANCE = 100000.0 # Replaced by STARTING_BALANCE_INR

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
        return [], STARTING_BALANCE_INR, 0.0, 0.0

    trades = []
    open_long_positions = []
    open_short_positions = []
    equity = STARTING_BALANCE_INR
    peak_equity = STARTING_BALANCE_INR
    max_drawdown = 0.0
    max_concurrent_mae = 0.0
    daily_open_price = None
    current_day_str = None
    # long_trade_sequence_counter = 0 # No longer used for hedge short trigger
    
    active_long_trade_group_ids = set() 
    active_short_trade_group_ids = set() 

    print(f"Starting backtest with {len(df)} candles...")

    for index, row in df.iterrows():
        candle_timestamp = row['timestamp']
        candle_open = row['open']
        candle_high = row['high']
        candle_low = row['low']
        candle_close = row['close'] 

        row_day_str = candle_timestamp.strftime("%Y-%m-%d")
        if current_day_str != row_day_str:
            daily_open_price = candle_open
            current_day_str = row_day_str
            print(f"\nNew trading day: {current_day_str}, Daily Open: {daily_open_price:.5f}")

        if daily_open_price is None:
            continue

        # --- START: 90-DAY TRADE CLOSURE RULE ---

        # --- Apply 90-day rule for open Long Positions ---
        for position in list(open_long_positions): # Iterate over a copy
            entry_date = position['entry_time'].date()
            current_candle_date = candle_timestamp.date()
            days_held = (current_candle_date - entry_date).days
            if days_held < 0: days_held = 0 # Safety check

            if days_held > 90:
                # Calculate current floating P&L for the check (excluding fees)
                pips_moved_long_float = (candle_close - position['entry_price']) / POINT_VALUE
                gross_pnl_inr_float = pips_moved_long_float * PNL_INR_PER_PIP
                swap_charges_inr_float = days_held * SWAP_LONG_INR_PER_NIGHT
                
                floating_net_pnl_inr_for_check = gross_pnl_inr_float + swap_charges_inr_float

                if floating_net_pnl_inr_for_check > -5000:
                    exit_price = candle_close # Close at current candle's close
                    
                    # Final P&L Calculation including fees
                    # Gross P&L and Swaps are the same as calculated for the check
                    final_fees_inr = FEE_INR_PER_TRADE # Apply full fee
                    final_net_pnl_inr = gross_pnl_inr_float + swap_charges_inr_float - final_fees_inr

                    trades.append({
                        "trade_type": "LONG", "entry_time": position['entry_time'], "entry_price": position['entry_price'],
                        "exit_time": candle_timestamp, "exit_price": exit_price, 
                        "gross_pnl_inr": gross_pnl_inr_float,
                        "swap_charges_inr": swap_charges_inr_float, 
                        "fees_inr": final_fees_inr,
                        "mae_inr": position.get('mae_inr', 0.0),
                        "net_pnl_inr": final_net_pnl_inr, 
                        "size": position['size'],
                        "margin_inr": MARGIN_INR_PER_TRADE,
                        "status": "CLOSED_90_DAY_RULE"
                    })
                    print(f"  LONG 90_DAY_RULE: PosID {position['id']} closed at {exit_price:.5f} (Days: {days_held}, Entry: {position['entry_price']:.5f}, Float PnL for check: {floating_net_pnl_inr_for_check:.2f}, Final Net PnL: {final_net_pnl_inr:.2f} INR)")
                    
                    open_long_positions.remove(position)
                    if position['id'] in active_long_trade_group_ids:
                        active_long_trade_group_ids.remove(position['id'])
                    
                    equity += final_net_pnl_inr
                    current_drawdown = (peak_equity - equity) / peak_equity if peak_equity > 0 and equity < peak_equity else 0.0
                    max_drawdown = max(max_drawdown, current_drawdown)
                    peak_equity = max(peak_equity, equity)
                    # This position is now closed and removed, so it won't be processed by subsequent TP logic in this candle iteration.

        # --- Apply 90-day rule for open Short Positions ---
        for position in list(open_short_positions): # Iterate over a copy
            entry_date = position['entry_time'].date()
            current_candle_date = candle_timestamp.date()
            days_held = (current_candle_date - entry_date).days
            if days_held < 0: days_held = 0 # Safety check

            if days_held > 90:
                # Calculate current floating P&L for the check (excluding fees)
                pips_moved_short_float = (position['entry_price'] - candle_close) / POINT_VALUE
                gross_pnl_inr_float = pips_moved_short_float * PNL_INR_PER_PIP
                swap_charges_inr_float = days_held * SWAP_SHORT_INR_PER_NIGHT
                
                floating_net_pnl_inr_for_check = gross_pnl_inr_float + swap_charges_inr_float

                if floating_net_pnl_inr_for_check > -5000:
                    exit_price = candle_close # Close at current candle's close

                    # Final P&L Calculation including fees
                    final_fees_inr = FEE_INR_PER_TRADE # Apply full fee
                    final_net_pnl_inr = gross_pnl_inr_float + swap_charges_inr_float - final_fees_inr

                    trades.append({
                        "trade_type": position.get('trade_type', 'SHORT'), "entry_time": position['entry_time'], "entry_price": position['entry_price'],
                        "exit_time": candle_timestamp, "exit_price": exit_price, 
                        "gross_pnl_inr": gross_pnl_inr_float,
                        "swap_charges_inr": swap_charges_inr_float,
                        "fees_inr": final_fees_inr,
                        "mae_inr": position.get('mae_inr', 0.0),
                        "net_pnl_inr": final_net_pnl_inr, 
                        "size": position['size'],
                        "margin_inr": MARGIN_INR_PER_TRADE,
                        "status": "CLOSED_90_DAY_RULE"
                    })
                    print(f"  {position.get('trade_type', 'SHORT')} 90_DAY_RULE: PosID {position['id']} closed at {exit_price:.5f} (Days: {days_held}, Entry: {position['entry_price']:.5f}, Float PnL for check: {floating_net_pnl_inr_for_check:.2f}, Final Net PnL: {final_net_pnl_inr:.2f} INR)")
                    
                    open_short_positions.remove(position)
                    if position['id'] in active_short_trade_group_ids:
                        active_short_trade_group_ids.remove(position['id'])
                    
                    equity += final_net_pnl_inr
                    current_drawdown = (peak_equity - equity) / peak_equity if peak_equity > 0 and equity < peak_equity else 0.0
                    max_drawdown = max(max_drawdown, current_drawdown)
                    peak_equity = max(peak_equity, equity)
                    # This position is now closed and removed.

        # --- END: 90-DAY TRADE CLOSURE RULE ---

        # --- Manage Open Long Positions ---
        for position in list(open_long_positions):
            # MAE Calculation in INR
            if candle_low < position['entry_price']: # Adverse move for long
                adverse_pips_long = (position['entry_price'] - candle_low) / POINT_VALUE
                current_mae_inr = adverse_pips_long * PNL_INR_PER_PIP
                position['mae_inr'] = max(position.get('mae_inr', 0.0), current_mae_inr)

            effective_tp_price = position['entry_price'] + TAKE_PROFIT_PRICE_OFFSET
            
            if candle_high >= effective_tp_price:
                exit_price = effective_tp_price
                entry_date = position['entry_time'].date()
                exit_date = candle_timestamp.date()
                days_held = (exit_date - entry_date).days
                if days_held < 0: days_held = 0 # Should not happen if data is chronological
                
                # P&L Calculation in INR
                pips_moved_long = (exit_price - position['entry_price']) / POINT_VALUE
                gross_pnl_inr = pips_moved_long * PNL_INR_PER_PIP
                
                swap_charges_inr = days_held * SWAP_LONG_INR_PER_NIGHT # Assumes SWAP_LONG_INR_PER_NIGHT is for LOT_SIZE_ACTUAL
                # trade_spread_cost_inr = SPREAD_COST_PER_LOT * position['size'] # SPREAD_COST_PER_LOT is 0
                fees_inr = FEE_INR_PER_TRADE # Applied per round trip
                
                net_pnl_inr = gross_pnl_inr + swap_charges_inr - fees_inr

                trades.append({
                    "trade_type": "LONG", "entry_time": position['entry_time'], "entry_price": position['entry_price'],
                    "exit_time": candle_timestamp, "exit_price": exit_price, 
                    "gross_pnl_inr": gross_pnl_inr,
                    "swap_charges_inr": swap_charges_inr, 
                    "fees_inr": fees_inr,
                    "mae_inr": position.get('mae_inr', 0.0), # MAE in INR
                    "net_pnl_inr": net_pnl_inr, 
                    "size": position['size'], # This should be LOT_SIZE_ACTUAL
                    "margin_inr": MARGIN_INR_PER_TRADE, # Informational
                    "status": "CLOSED_TP"
                })
                print(f"  LONG TP HIT: PosID {position['id']} exited at {exit_price:.5f} (Entry: {position['entry_price']:.5f}, TP Offset: {TAKE_PROFIT_PRICE_OFFSET}, Gross PnL: {gross_pnl_inr:.2f} INR, Swap: {swap_charges_inr:.2f} INR, Fees: {fees_inr:.2f} INR, MAE: {position.get('mae_inr', 0.0):.2f} INR, Net PnL: {net_pnl_inr:.2f} INR)")
                open_long_positions.remove(position)
                
                if position['id'] in active_long_trade_group_ids:
                    active_long_trade_group_ids.remove(position['id'])
                
                equity += net_pnl_inr # Update equity with INR P&L
                current_drawdown = (peak_equity - equity) / peak_equity if peak_equity > 0 and equity < peak_equity else 0.0
                max_drawdown = max(max_drawdown, current_drawdown)
                peak_equity = max(peak_equity, equity)

        # --- Manage Open Short Positions ---
        for position in list(open_short_positions):
            # MAE Calculation in INR
            if candle_high > position['entry_price']: # Adverse move for short
                adverse_pips_short = (candle_high - position['entry_price']) / POINT_VALUE
                current_mae_inr = adverse_pips_short * PNL_INR_PER_PIP
                position['mae_inr'] = max(position.get('mae_inr', 0.0), current_mae_inr)

            # Check for Stop Loss first for Hedge Shorts (COMMENTED OUT - ensure INR conversion if re-enabled)
            # ... (SL logic would need INR conversion for P&L)

            # Check for Take Profit
            if candle_low <= position['tp_price']: # tp_price for short is below entry
                exit_price = position['tp_price']
                status = "CLOSED_TP"
                
                entry_date = position['entry_time'].date()
                exit_date = candle_timestamp.date()
                days_held = (exit_date - entry_date).days
                if days_held < 0: days_held = 0

                # P&L Calculation in INR
                pips_moved_short = (position['entry_price'] - exit_price) / POINT_VALUE
                gross_pnl_inr = pips_moved_short * PNL_INR_PER_PIP
                
                swap_charges_inr = days_held * SWAP_SHORT_INR_PER_NIGHT # Assumes SWAP_SHORT_INR_PER_NIGHT is for LOT_SIZE_ACTUAL
                # trade_spread_cost_inr = 0.0 # SPREAD_COST_PER_LOT is 0
                fees_inr = FEE_INR_PER_TRADE

                net_pnl_inr = gross_pnl_inr + swap_charges_inr - fees_inr
                
                trades.append({
                    "trade_type": position.get('trade_type', 'SHORT'), 
                    "entry_time": position['entry_time'], "entry_price": position['entry_price'],
                    "exit_time": candle_timestamp, "exit_price": exit_price, 
                    "gross_pnl_inr": gross_pnl_inr,
                    "swap_charges_inr": swap_charges_inr,
                    "fees_inr": fees_inr,
                    "mae_inr": position.get('mae_inr', 0.0), # MAE in INR
                    "net_pnl_inr": net_pnl_inr, 
                    "size": position['size'], # This should be LOT_SIZE_ACTUAL
                    "margin_inr": MARGIN_INR_PER_TRADE, # Informational
                    "status": status
                })
                print(f"  {position.get('trade_type', 'SHORT')} TP HIT: PosID {position['id']} exited at {exit_price:.5f} (Entry: {position['entry_price']:.5f}, Gross PnL: {gross_pnl_inr:.2f} INR, Swap: {swap_charges_inr:.2f} INR, Fees: {fees_inr:.2f} INR, MAE: {position.get('mae_inr', 0.0):.2f} INR, Net PnL: {net_pnl_inr:.2f} INR)")
                open_short_positions.remove(position)
                if position['id'] in active_short_trade_group_ids:
                    active_short_trade_group_ids.remove(position['id'])
                equity += net_pnl_inr # Update equity with INR P&L
                current_drawdown = (peak_equity - equity) / peak_equity if peak_equity > 0 and equity < peak_equity else 0.0
                max_drawdown = max(max_drawdown, current_drawdown)
                peak_equity = max(peak_equity, equity)
        
        momentary_total_adverse_excursion_inr = 0.0
        for pos_long in open_long_positions:
            if candle_low < pos_long['entry_price']:
                adverse_pips = (pos_long['entry_price'] - candle_low) / POINT_VALUE
                momentary_total_adverse_excursion_inr += max(0.0, adverse_pips * PNL_INR_PER_PIP)
        for pos_short in open_short_positions:
            if candle_high > pos_short['entry_price']:
                adverse_pips = (candle_high - pos_short['entry_price']) / POINT_VALUE
                momentary_total_adverse_excursion_inr += max(0.0, adverse_pips * PNL_INR_PER_PIP)
        max_concurrent_mae = max(max_concurrent_mae, momentary_total_adverse_excursion_inr) # max_concurrent_mae is now in INR

        entries_today_count = sum(1 for pos in open_long_positions if pos.get('daily_open_ref') == daily_open_price)

        if entries_today_count == 0:
            # First entry of the day based on percentage drop
            next_entry_target_price = daily_open_price * (1 - ENTRY_THRESHOLD_PERCENT)
        else:
            # Subsequent entries based on $1 drop from the last long entry for the current day
            most_recent_long_for_day = None
            # Find the most recent (latest entry time) long position taken against the current daily_open_price
            # This assumes open_long_positions are not necessarily sorted by entry time if multiple days are mixed.
            # However, since we filter by daily_open_ref, it should be fine.
            # To be absolutely sure, find the one with the max entry_time among those matching daily_open_ref.
            
            latest_entry_time_for_day = pd.Timestamp.min # Initialize with a very old timestamp
            # Pandas Timestamps might not be timezone-aware by default from SQLite if not specified.
            # If candle_timestamp is timezone-aware, ensure latest_entry_time_for_day is compatible or make candle_timestamp naive.
            # Assuming candle_timestamp from df is naive or compatible.
            
            relevant_open_longs = [pos for pos in open_long_positions if pos.get('daily_open_ref') == daily_open_price]
            if relevant_open_longs:
                most_recent_long_for_day = max(relevant_open_longs, key=lambda x: x['entry_time'])
            
            if most_recent_long_for_day:
                next_entry_target_price = most_recent_long_for_day['entry_price'] - LONG_ENTRY_DOLLAR_STEP
            else:
                # Fallback: if no prior long for the day (should not happen if entries_today_count > 0),
                # use the percentage method for the first trade.
                # This effectively means only one trade attempt if this state is somehow reached.
                next_entry_target_price = daily_open_price * (1 - ENTRY_THRESHOLD_PERCENT)
                # To prevent re-triggering the first trade logic if it already happened and closed,
                # a more robust check might be needed, but entries_today_count should handle this.

        # --- START: MAX CONCURRENT TRADES LIMIT ---
        can_open_new_trade = (len(open_long_positions) + len(open_short_positions)) < 30
        # --- END: MAX CONCURRENT TRADES LIMIT ---

        if can_open_new_trade and candle_low <= next_entry_target_price:
            entry_price = next_entry_target_price # The actual entry price will be this calculated target
            
            # is_first_of_cluster = (len(open_long_positions) == 0) # COMMENTED OUT - For hedge short

            position_id_long = f"LPos-{len(trades) + len(open_long_positions) + len(open_short_positions) + 1}-{candle_timestamp.strftime('%H%M%S%f')}"
            tp_price_for_this_trade = entry_price + TAKE_PROFIT_PRICE_OFFSET
            new_long_position = {
                "id": position_id_long, "trade_type": "LONG", "entry_time": candle_timestamp,
                "entry_price": entry_price, "tp_price": tp_price_for_this_trade, 
                "size": LOT_SIZE_ACTUAL, # Use actual lot size
                "status": "OPEN", "daily_open_ref": daily_open_price, "mae_inr": 0.0 # Initialize MAE in INR
            }
            if candle_low < new_long_position['entry_price']: # Initial MAE for long
                initial_adverse_pips_long = (new_long_position['entry_price'] - candle_low) / POINT_VALUE
                new_long_position['mae_inr'] = max(0.0, initial_adverse_pips_long * PNL_INR_PER_PIP)
            
            open_long_positions.append(new_long_position)
            active_long_trade_group_ids.add(position_id_long) 
            print(f"  NEW LONG: PosID {position_id_long} opened at {entry_price:.5f} (TP: {tp_price_for_this_trade:.5f}, Lots: {LOT_SIZE_ACTUAL}) based on DailyOpen {daily_open_price:.5f}. Entries today: {entries_today_count + 1}")
            
            original_short_entry_price = entry_price + SHORT_ENTRY_PRICE_OFFSET_FROM_LONG
            original_short_position_id = f"SPosOrig-{len(trades) + len(open_long_positions) + len(open_short_positions) + 1}-{candle_timestamp.strftime('%H%M%S%f')}"
            original_short_size = LOT_SIZE_ACTUAL * SHORT_LOT_SIZE_FRACTION_OF_LONG # Ensure this results in 0.01 if fraction is 1.0
            original_tp_price_for_short = original_short_entry_price - SHORT_TAKE_PROFIT_PRICE_OFFSET
            new_original_short_position = {
                "id": original_short_position_id, "trade_type": "SHORT_ORIG", 
                "entry_time": candle_timestamp, "entry_price": original_short_entry_price,
                "tp_price": original_tp_price_for_short, "size": original_short_size,
                "status": "OPEN", "daily_open_ref": daily_open_price, "mae_inr": 0.0 # Initialize MAE in INR
            }
            if candle_high > new_original_short_position['entry_price']: # Initial MAE for short
                initial_adverse_pips_short = (candle_high - new_original_short_position['entry_price']) / POINT_VALUE
                new_original_short_position['mae_inr'] = max(0.0, initial_adverse_pips_short * PNL_INR_PER_PIP)

            open_short_positions.append(new_original_short_position)
            active_short_trade_group_ids.add(original_short_position_id)
            print(f"  NEW ORIGINAL SHORT: PosID {original_short_position_id} opened at {original_short_entry_price:.5f} (TP: {original_tp_price_for_short:.5f}, Lots: {original_short_size:.2f})")

            # --- Cluster-Initiated Hedge Short Entry (COMMENTED OUT - ensure INR conversion if re-enabled) ---
            # ... (Hedge short logic would need INR conversion for P&L and MAE)
            
            # Same-candle TP check for new LONG position
            if candle_high >= new_long_position['tp_price']: 
                exit_price_immediate = new_long_position['tp_price']
                
                pips_moved_immediate_long = (exit_price_immediate - entry_price) / POINT_VALUE
                gross_pnl_immediate_inr = pips_moved_immediate_long * PNL_INR_PER_PIP
                
                swap_charges_immediate_inr = 0.0 # No overnight for same candle
                fees_immediate_inr = FEE_INR_PER_TRADE
                net_pnl_immediate_inr = gross_pnl_immediate_inr + swap_charges_immediate_inr - fees_immediate_inr
                
                mae_immediate_inr = new_long_position['mae_inr'] 
                trades.append({
                    "trade_type": "LONG", "entry_time": candle_timestamp, "entry_price": entry_price,
                    "exit_time": candle_timestamp, "exit_price": exit_price_immediate, 
                    "gross_pnl_inr": gross_pnl_immediate_inr,
                    "swap_charges_inr": swap_charges_immediate_inr, 
                    "fees_inr": fees_immediate_inr,
                    "mae_inr": mae_immediate_inr, 
                    "net_pnl_inr": net_pnl_immediate_inr, 
                    "size": new_long_position['size'], 
                    "margin_inr": MARGIN_INR_PER_TRADE,
                    "status": "CLOSED_TP_SAME_CANDLE"
                })
                print(f"  LONG TP HIT (Same Candle): PosID {position_id_long} exited at {exit_price_immediate:.5f} (Gross PnL: {gross_pnl_immediate_inr:.2f} INR, Swap: {swap_charges_immediate_inr:.2f} INR, Fees: {fees_immediate_inr:.2f} INR, MAE: {mae_immediate_inr:.2f} INR, Net PnL: {net_pnl_immediate_inr:.2f} INR)")
                open_long_positions.remove(new_long_position)
                if new_long_position['id'] in active_long_trade_group_ids:
                    active_long_trade_group_ids.remove(new_long_position['id'])
                equity += net_pnl_immediate_inr
                current_drawdown_long_tp = (peak_equity - equity) / peak_equity if peak_equity > 0 and equity < peak_equity else 0.0
                max_drawdown = max(max_drawdown, current_drawdown_long_tp)
                peak_equity = max(peak_equity, equity)

            # Same-candle TP check for new ORIGINAL SHORT position
            if new_original_short_position and new_original_short_position in open_short_positions and candle_low <= new_original_short_position['tp_price']: 
                exit_price_immediate_orig_short = new_original_short_position['tp_price']
                
                pips_moved_immediate_short = (new_original_short_position['entry_price'] - exit_price_immediate_orig_short) / POINT_VALUE
                gross_pnl_immediate_orig_short_inr = pips_moved_immediate_short * PNL_INR_PER_PIP
                
                swap_charges_immediate_orig_short_inr = 0.0 # No overnight
                fees_immediate_orig_short_inr = FEE_INR_PER_TRADE
                net_pnl_immediate_orig_short_inr = gross_pnl_immediate_orig_short_inr + swap_charges_immediate_orig_short_inr - fees_immediate_orig_short_inr
                
                mae_immediate_orig_short_inr = new_original_short_position['mae_inr']
                trades.append({
                    "trade_type": new_original_short_position['trade_type'], "entry_time": candle_timestamp, 
                    "entry_price": new_original_short_position['entry_price'],
                    "exit_time": candle_timestamp, "exit_price": exit_price_immediate_orig_short, 
                    "gross_pnl_inr": gross_pnl_immediate_orig_short_inr,
                    "swap_charges_inr": swap_charges_immediate_orig_short_inr, 
                    "fees_inr": fees_immediate_orig_short_inr,
                    "mae_inr": mae_immediate_orig_short_inr, 
                    "net_pnl_inr": net_pnl_immediate_orig_short_inr, 
                    "size": new_original_short_position['size'], 
                    "margin_inr": MARGIN_INR_PER_TRADE,
                    "status": "CLOSED_TP_SAME_CANDLE"
                })
                print(f"  {new_original_short_position['trade_type']} TP HIT (Same Candle): PosID {new_original_short_position['id']} exited at {exit_price_immediate_orig_short:.5f} (Gross PnL: {gross_pnl_immediate_orig_short_inr:.2f} INR, Net PnL: {net_pnl_immediate_orig_short_inr:.2f} INR)")
                if new_original_short_position in open_short_positions: # Check again as it might have been removed by hedge logic if active
                    open_short_positions.remove(new_original_short_position)
                if new_original_short_position['id'] in active_short_trade_group_ids:
                    active_short_trade_group_ids.remove(new_original_short_position['id'])
                equity += net_pnl_immediate_orig_short_inr
                current_drawdown_short_tp = (peak_equity - equity) / peak_equity if peak_equity > 0 and equity < peak_equity else 0.0
                max_drawdown = max(max_drawdown, current_drawdown_short_tp)
                peak_equity = max(peak_equity, equity)

            # Same-candle SL/TP check for the new hedge short position (COMMENTED OUT - ensure INR conversion if re-enabled)
            # ... (Hedge short SL/TP logic would need INR conversion)
            
    if not df.empty:
        last_candle_timestamp = df['timestamp'].iloc[-1]
        last_close_price = df['close'].iloc[-1]
        
        # Closing STILL_OPEN Long positions
        for pos in open_long_positions:
            entry_date = pos['entry_time'].date()
            last_data_date = last_candle_timestamp.date()
            days_held = (last_data_date - entry_date).days
            if days_held < 0: days_held = 0
            
            pips_moved_open_long = (last_close_price - pos['entry_price']) / POINT_VALUE
            gross_pnl_inr = pips_moved_open_long * PNL_INR_PER_PIP
            swap_charges_inr = days_held * SWAP_LONG_INR_PER_NIGHT
            fees_inr = 0 # No fee for unrealized PNL, or apply half? For now, 0.
            # trade_spread_cost_inr = 0.0
            net_pnl_inr = gross_pnl_inr + swap_charges_inr # No fee for open trades

            trades.append({
                "trade_type": "LONG", "entry_time": pos['entry_time'], "entry_price": pos['entry_price'],
                "exit_time": "STILL_OPEN", "exit_price": last_close_price, 
                "gross_pnl_inr": gross_pnl_inr,
                "swap_charges_inr": swap_charges_inr, 
                "fees_inr": fees_inr, # Typically fees aren't applied to open PnL
                "mae_inr": pos.get('mae_inr', 0.0),
                "net_pnl_inr": net_pnl_inr, 
                "size": pos['size'], 
                "margin_inr": MARGIN_INR_PER_TRADE,
                "status": "STILL_OPEN"
            })
            print(f"  END OF BACKTEST (LONG): PosID {pos['id']} still open. Entry: {pos['entry_price']:.5f}, Current Price: {last_close_price:.5f}, Unrealized Gross PnL: {gross_pnl_inr:.2f} INR, Swap: {swap_charges_inr:.2f} INR, MAE: {pos.get('mae_inr', 0.0):.2f} INR, Unrealized Net PnL: {net_pnl_inr:.2f} INR)")

        # Closing STILL_OPEN Short positions
        for pos in open_short_positions:
            entry_date = pos['entry_time'].date()
            last_data_date = last_candle_timestamp.date()
            days_held = (last_data_date - entry_date).days
            if days_held < 0: days_held = 0

            pips_moved_open_short = (pos['entry_price'] - last_close_price) / POINT_VALUE
            gross_pnl_inr = pips_moved_open_short * PNL_INR_PER_PIP
            swap_charges_inr = days_held * SWAP_SHORT_INR_PER_NIGHT
            fees_inr = 0 # No fee for unrealized PNL
            # trade_spread_cost_inr = 0.0
            net_pnl_inr = gross_pnl_inr + swap_charges_inr # No fee for open trades
            
            trades.append({
                "trade_type": pos['trade_type'], "entry_time": pos['entry_time'], "entry_price": pos['entry_price'],
                "exit_time": "STILL_OPEN", "exit_price": last_close_price, 
                "gross_pnl_inr": gross_pnl_inr,
                "swap_charges_inr": swap_charges_inr, 
                "fees_inr": fees_inr,
                "mae_inr": pos.get('mae_inr', 0.0),
                "net_pnl_inr": net_pnl_inr, 
                "size": pos['size'], 
                "margin_inr": MARGIN_INR_PER_TRADE,
                "status": "STILL_OPEN"
            })
            print(f"  END OF BACKTEST ({pos['trade_type']}): PosID {pos['id']} still open. Entry: {pos['entry_price']:.5f}, Current Price: {last_close_price:.5f}, Unrealized Gross PnL: {gross_pnl_inr:.2f} INR, Unrealized Net PnL: {net_pnl_inr:.2f} INR)")
    
    return trades, equity, max_drawdown, max_concurrent_mae

def print_results(trades_history, starting_balance_inr, final_equity_inr, max_drawdown_value, max_concurrent_mae_value_inr):
    file_output_lines = []
    currency_symbol = "INR " # Changed from ₹ to avoid encoding/display issues

    if not trades_history:
        no_trades_messages = [
            "\nNo trades were made during the backtest.",
            f"Starting Balance: {currency_symbol}{starting_balance_inr:,.2f}",
            f"Ending Equity: {currency_symbol}{starting_balance_inr:,.2f}",
            f"Net P&L: {currency_symbol}0.00",
            f"Maximum Drawdown: 0.00%",
            f"Max Concurrent MAE: {currency_symbol}{max_concurrent_mae_value_inr:,.2f}" # Ensure this is passed correctly
        ]
        for msg in no_trades_messages:
            print(msg)
            file_output_lines.append(msg)
        output_file_name = "analysis_matrix_output.txt"
        try:
            with open(output_file_name, "w", encoding="utf-8") as f: # Explicitly use UTF-8 encoding
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

    # Use new INR columns
    total_net_pnl_inr = df_trades['net_pnl_inr'].sum() if 'net_pnl_inr' in df_trades.columns else 0
    total_gross_pnl_inr = df_trades['gross_pnl_inr'].sum() if 'gross_pnl_inr' in df_trades.columns else 0
    total_swap_charges_inr = df_trades['swap_charges_inr'].sum() if 'swap_charges_inr' in df_trades.columns else 0
    total_fees_inr = df_trades['fees_inr'].sum() if 'fees_inr' in df_trades.columns else 0 # Sum of all fees
    # total_spread_costs = df_trades['spread_cost'].sum() if 'spread_cost' in df_trades.columns else 0 # Spread is 0

    results_summary = [
        "\n--- Backtest Results (INR) ---",
        f"Starting Balance: {currency_symbol}{starting_balance_inr:,.2f}",
        f"Ending Equity: {currency_symbol}{final_equity_inr:,.2f}",
        f"Net P&L (from equity change): {currency_symbol}{final_equity_inr - starting_balance_inr:,.2f}",
        f"Maximum Drawdown: {max_drawdown_value * 100:.2f}%",
        f"Max Concurrent MAE: {currency_symbol}{max_concurrent_mae_value_inr:,.2f}",
        "\n--- Trade Performance (INR) ---",
        f"Total Trades Recorded (incl. open): {total_trades}",
        f"Total Gross P&L (all trades): {currency_symbol}{total_gross_pnl_inr:,.2f}",
        f"Total Swap Charges (all trades): {currency_symbol}{total_swap_charges_inr:,.2f}",
        f"Total Fees (all trades): {currency_symbol}{total_fees_inr:,.2f}",
        # f"Total Spread Costs (all trades): {currency_symbol}{total_spread_costs:,.2f}", # Spread is 0
        f"Total Net P&L (all trades, from log): {currency_symbol}{total_net_pnl_inr:,.2f}",
        f"\nNumber of Closed Trades: {num_closed_trades}"
    ]
    for line in results_summary:
        print(line)
        file_output_lines.append(line)

    if num_closed_trades > 0:
        num_closed_winning = closed_trades_df[closed_trades_df['net_pnl_inr'] > 0]['net_pnl_inr'].count()
        win_rate_closed = (num_closed_winning / num_closed_trades) * 100 if num_closed_trades > 0 else 0
        avg_net_pnl_closed_inr = closed_trades_df['net_pnl_inr'].mean()
        avg_gross_pnl_closed_inr = closed_trades_df['gross_pnl_inr'].mean()
        avg_swap_closed_inr = closed_trades_df['swap_charges_inr'].mean()
        avg_fees_closed_inr = closed_trades_df['fees_inr'].mean()
        # avg_spread_closed = closed_trades_df['spread_cost'].mean() # Spread is 0
        avg_mae_closed_inr = closed_trades_df['mae_inr'].mean()

        closed_trade_stats = [
            f"  Winning Closed Trades (Net P&L > 0): {num_closed_winning}",
            f"  Win Rate (Closed Trades): {win_rate_closed:.2f}%",
            f"  Average Gross P&L per Closed Trade: {currency_symbol}{avg_gross_pnl_closed_inr:,.2f}",
            f"  Average Swap Cost per Closed Trade: {currency_symbol}{avg_swap_closed_inr:.2f}",
            f"  Average Fee per Closed Trade: {currency_symbol}{avg_fees_closed_inr:,.2f}",
            # f"  Average Spread Cost per Closed Trade: {currency_symbol}{avg_spread_closed:.2f}",
            f"  Average MAE per Closed Trade: {currency_symbol}{avg_mae_closed_inr:.2f}",
            f"  Average Net P&L per Closed Trade: {currency_symbol}{avg_net_pnl_closed_inr:,.2f}"
        ]
        for line in closed_trade_stats:
            print(line)
            file_output_lines.append(line)
    
    open_trade_summary = [f"\nNumber of Trades Still Open: {num_still_open}"]
    if num_still_open > 0:
        unrealized_net_pnl_open_inr = still_open_trades_df['net_pnl_inr'].sum()
        open_trade_summary.append(f"  Unrealized Net P&L for Open Trades: {currency_symbol}{unrealized_net_pnl_open_inr:,.2f}")
    for line in open_trade_summary:
        print(line)
        file_output_lines.append(line)
    
    file_output_lines.append("\n--- Detailed Trade Log (INR) ---")
    print("\n--- Detailed Trade Log (INR) ---")
    
    if 'entry_time' in df_trades.columns:
      df_trades['entry_time_disp'] = df_trades['entry_time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M') if isinstance(x, datetime) else x)
    if 'exit_time' in df_trades.columns:
      df_trades['exit_time_disp'] = df_trades['exit_time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M') if isinstance(x, datetime) else x)
    
    # Update display_cols to use new INR field names
    display_cols = ['trade_type', 'entry_time_disp', 'entry_price', 'exit_time_disp', 'exit_price', 'size', 
                    'gross_pnl_inr', 'swap_charges_inr', 'fees_inr', 'mae_inr', 'net_pnl_inr', 'margin_inr', 'status']
    # 'spread_cost' was removed as it's zero, 'pnl' is now 'net_pnl_inr', 'gross_pnl' is 'gross_pnl_inr', etc.
    
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
        with open(output_file_name, "w", encoding="utf-8") as f: # Explicitly use UTF-8 encoding
            f.write("\n".join(file_output_lines).lstrip())
        print(f"\nFull analysis matrix also saved to {output_file_name}")
    except Exception as e:
        print(f"\nError saving analysis matrix to {output_file_name}: {e}")

    csv_filename = "backtest_trade_log.csv"
    try:
        # Update csv_cols to use new INR field names
        csv_cols = ['trade_type', 'entry_time', 'entry_price', 'exit_time', 'exit_price', 'size', 
                    'gross_pnl_inr', 'swap_charges_inr', 'fees_inr', 'mae_inr', 'net_pnl_inr', 'margin_inr', 'status']
        cols_to_save = [col for col in csv_cols if col in df_trades.columns]
        df_trades_to_save = df_trades[cols_to_save].copy()
        if 'mae_inr' in df_trades_to_save.columns: # Ensure it's the INR mae
             df_trades_to_save['mae_inr'] = df_trades_to_save['mae_inr'].abs()
        df_trades_to_save.to_csv(csv_filename, index=False, float_format='%.5f') # Keep float format for prices, PnL might need .2f
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
        trades_log, final_equity_inr, max_drawdown_result, max_concurrent_mae_result_inr = run_backtest(historical_data_df)
        print_results(trades_log, STARTING_BALANCE_INR, final_equity_inr, max_drawdown_result, max_concurrent_mae_result_inr)
    else:
        print("Could not fetch data. Aborting backtest.")

if __name__ == "__main__":
    main()
