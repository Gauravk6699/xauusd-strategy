"""
Backtesting Script for RSI/SMA Crossover Strategy on XAU/USD

This script performs an end-to-end backtest of a trading strategy based on
the crossover of the Relative Strength Index (RSI) and its Simple Moving Average (SMA).
It is designed for XAU/USD (Gold/US Dollar) using 5-minute interval data obtained
from the Twelve Data API, with local SQLite caching for historical data and
separate SQLite logging for losing trades with context.

Key functionalities include:
1.  Fetching data from Twelve Data API with local SQLite caching for 360 days.
2.  Calculation of RSI and SMA of RSI technical indicators on 5-min data.
3.  Trend determination using a 50-SMA on 15-min data.
4.  Support/Resistance level identification using Pivot Points on 15-min and 4-hour data.
5.  Generation of trading signals with S/R levels included in signal data.
6.  Simulation of trade execution.
7.  Logging of losing trades with pre-trade candle data and S/R context to a separate SQLite DB.
8.  Calculation and printing of comprehensive performance metrics.

Dependencies:
- requests, pandas, pandas-ta
"""

import requests
import json
import pandas as pd
import pandas_ta as ta
from datetime import datetime, timedelta, time, timezone
import sqlite3
import os

# --- GLOBAL CONFIGURATIONS ---

# Database Settings
DB_FILE_PATH = "historical_xauusd_data.db" # For caching price data
LOSING_TRADES_DB_PATH = "losing_trades_context.db" # For logging losing trades
LOSING_TRADES_TABLE_NAME = "losing_trade_details"
PROFITABLE_TRADES_TABLE_NAME = "profitable_trade_details" # New table for profitable trades
SUMMARY_RESULTS_TABLE_NAME = "backtest_summary_results" # New table for summary metrics

# Pre-trade context settings
PRE_TRADE_CANDLES_5MIN = 50
PRE_TRADE_CANDLES_15MIN = 10
PRE_TRADE_CANDLES_4HOUR = 5

# Twelve Data API Settings
API_KEY = "71b36f5f96a2489d8454c4a1f2da621e"
API_URL = "https://api.twelvedata.com/time_series"

# Market and Data Fetching Parameters
SYMBOL = "XAU/USD"
DAYS_TO_FETCH_DATA = 3 * 365
OUTPUT_SIZE = 5000
TREND_SMA_PERIOD = 50
SR_NEARNESS_FACTOR_ATR_MULTIPLE = 0.5

INTERVALS_CONFIG = {
    "5min": {"minutes": 5, "days_to_fetch": DAYS_TO_FETCH_DATA, "table_name": "data_5min"},
    "15min": {"minutes": 15, "days_to_fetch": DAYS_TO_FETCH_DATA, "table_name": "data_15min"},
    "4hour": {"minutes": 240, "days_to_fetch": DAYS_TO_FETCH_DATA, "table_name": "data_4hour"}
}

# Strategy Parameters - Technical Indicators
RSI_PERIOD = 29
SMA_PERIOD_ON_RSI = 14

# Strategy Parameters - Financial & Risk Management
INITIAL_BALANCE = 100000.0
TRADE_UNITS = 200.0
PIP_VALUE_XAU_USD = 0.01
# RR_MULTIPLIER = 2.5 # Risk-to-Reward Multiplier (e.g., 2.5 means 1:2.5 R:R) - Now using S/R for TP
MIN_SL_DISTANCE_PIPS = 10 # Minimum distance for stop loss in pips

# Strategy Parameters - Trading Hours (UTC)
TRADING_WINDOWS_UTC = {
    "window_1": (time(3, 30, 0), time(11, 59, 59)),
    "window_2": (time(14, 15, 0), time(15, 30, 0))
}

# --- Database Functions (Historical Data) ---
def create_db_connection(db_file_path):
    conn = None
    try:
        conn = sqlite3.connect(db_file_path)
    except sqlite3.Error as e:
        print(f"Error connecting to SQLite database {db_file_path}: {e}")
    return conn

def create_data_table_if_not_exists(conn, table_name):
    try:
        sql = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            datetime TEXT PRIMARY KEY, open REAL, high REAL, low REAL, close REAL);"""
        conn.cursor().execute(sql)
        conn.commit()
    except sqlite3.Error as e: print(f"Error creating table {table_name}: {e}")

def load_data_from_db(conn, table_name, start_date_iso=None, end_date_iso=None):
    try:
        params = ()
        if start_date_iso and end_date_iso:
            query = f"SELECT * FROM {table_name} WHERE datetime >= ? AND datetime <= ? ORDER BY datetime ASC"
            params = (start_date_iso, end_date_iso)
        else:
            query = f"SELECT * FROM {table_name} ORDER BY datetime ASC"
        df = pd.read_sql_query(query, conn, params=params)
        if not df.empty: df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
        return df
    except Exception as e: print(f"Error loading from {table_name}: {e}"); return pd.DataFrame()

def get_latest_datetime_in_db(conn, table_name):
    try:
        res = conn.cursor().execute(f"SELECT MAX(datetime) FROM {table_name}").fetchone()
        return res[0] if res and res[0] else None
    except Exception as e: print(f"Error getting latest datetime from {table_name}: {e}"); return None

def save_dataframe_to_db(conn, table_name, df):
    if df.empty: return
    try:
        df_copy = df.copy()
        if pd.api.types.is_datetime64_any_dtype(df_copy['datetime']):
             df_copy['datetime'] = df_copy['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
        tuples = [tuple(x) for x in df_copy.to_numpy()]
        sql = f"INSERT OR REPLACE INTO {table_name} (datetime, open, high, low, close) VALUES (?,?,?,?,?)"
        conn.cursor().executemany(sql, tuples)
        conn.commit()
        print(f"DB Save: {len(df)} records processed for {table_name}.")
    except Exception as e: print(f"Error saving to {table_name}: {e}")

# --- Helper functions for Losing Trades Database ---
def create_losing_trades_table(conn):
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {LOSING_TRADES_TABLE_NAME} (
            trade_id TEXT PRIMARY KEY, entry_datetime TEXT, entry_price REAL, direction TEXT,
            exit_datetime TEXT, exit_price REAL, pnl REAL, exit_reason TEXT,
            crossover_candle_datetime TEXT, crossover_candle_rsi REAL, crossover_candle_sma_rsi REAL,
            crossover_candle_atr REAL, trend_15min_at_signal TEXT,
            sr_15min_supports_json TEXT, sr_15min_resistances_json TEXT,
            sr_4hour_supports_json TEXT, sr_4hour_resistances_json TEXT,
            pre_trade_data_5min_json TEXT, pre_trade_data_15min_json TEXT,
            pre_trade_data_4hour_json TEXT, raw_signal_data_json TEXT);""")
        conn.commit()
    except sqlite3.Error as e: print(f"Error creating {LOSING_TRADES_TABLE_NAME}: {e}")

# New function to create profitable trades table
def create_profitable_trades_table(conn):
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {PROFITABLE_TRADES_TABLE_NAME} (
            trade_id TEXT PRIMARY KEY, entry_datetime TEXT, entry_price REAL, direction TEXT,
            exit_datetime TEXT, exit_price REAL, pnl REAL, exit_reason TEXT,
            crossover_candle_datetime TEXT, crossover_candle_rsi REAL, crossover_candle_sma_rsi REAL,
            crossover_candle_atr REAL, trend_15min_at_signal TEXT,
            sr_15min_supports_json TEXT, sr_15min_resistances_json TEXT,
            sr_4hour_supports_json TEXT, sr_4hour_resistances_json TEXT,
            pre_trade_data_5min_json TEXT, pre_trade_data_15min_json TEXT,
            pre_trade_data_4hour_json TEXT, raw_signal_data_json TEXT);""")
        conn.commit()
    except sqlite3.Error as e: print(f"Error creating {PROFITABLE_TRADES_TABLE_NAME}: {e}")

def save_losing_trade_details_to_db(conn, trade_details_dict):
    try:
        expected_keys = ['trade_id', 'entry_datetime', 'entry_price', 'direction', 'exit_datetime', 'exit_price', 'pnl', 'exit_reason', 'crossover_candle_datetime', 'crossover_candle_rsi', 'crossover_candle_sma_rsi', 'crossover_candle_atr', 'trend_15min_at_signal', 'sr_15min_supports_json', 'sr_15min_resistances_json', 'sr_4hour_supports_json', 'sr_4hour_resistances_json', 'pre_trade_data_5min_json', 'pre_trade_data_15min_json', 'pre_trade_data_4hour_json', 'raw_signal_data_json']
        for key in expected_keys: trade_details_dict.setdefault(key, None)
        for dt_key in ['entry_datetime', 'exit_datetime', 'crossover_candle_datetime']:
            if isinstance(trade_details_dict[dt_key], (pd.Timestamp, datetime)):
                trade_details_dict[dt_key] = trade_details_dict[dt_key].isoformat()
        
        columns = ', '.join(trade_details_dict.keys())
        placeholders = ', '.join('?' * len(trade_details_dict))
        sql = f"INSERT OR REPLACE INTO {LOSING_TRADES_TABLE_NAME} ({columns}) VALUES ({placeholders})"
        conn.cursor().execute(sql, list(trade_details_dict.values()))
        conn.commit()
    except Exception as e: print(f"Error saving losing trade ID {trade_details_dict.get('trade_id')}: {e}")

# New function to save profitable trade details
def save_profitable_trade_details_to_db(conn, trade_details_dict):
    try:
        expected_keys = ['trade_id', 'entry_datetime', 'entry_price', 'direction', 'exit_datetime', 'exit_price', 'pnl', 'exit_reason', 'crossover_candle_datetime', 'crossover_candle_rsi', 'crossover_candle_sma_rsi', 'crossover_candle_atr', 'trend_15min_at_signal', 'sr_15min_supports_json', 'sr_15min_resistances_json', 'sr_4hour_supports_json', 'sr_4hour_resistances_json', 'pre_trade_data_5min_json', 'pre_trade_data_15min_json', 'pre_trade_data_4hour_json', 'raw_signal_data_json']
        for key in expected_keys: trade_details_dict.setdefault(key, None)
        for dt_key in ['entry_datetime', 'exit_datetime', 'crossover_candle_datetime']:
            if isinstance(trade_details_dict[dt_key], (pd.Timestamp, datetime)):
                trade_details_dict[dt_key] = trade_details_dict[dt_key].isoformat()
        
        columns = ', '.join(trade_details_dict.keys())
        placeholders = ', '.join('?' * len(trade_details_dict))
        sql = f"INSERT OR REPLACE INTO {PROFITABLE_TRADES_TABLE_NAME} ({columns}) VALUES ({placeholders})"
        conn.cursor().execute(sql, list(trade_details_dict.values()))
        conn.commit()
    except Exception as e: print(f"Error saving profitable trade ID {trade_details_dict.get('trade_id')}: {e}")

# New function to create summary results table
def create_summary_results_table(conn):
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {SUMMARY_RESULTS_TABLE_NAME} (
            run_datetime TEXT PRIMARY KEY,
            rsi_filter_long TEXT,
            rsi_filter_short TEXT,
            rr_multiplier REAL,
            total_trades INTEGER,
            winning_trades INTEGER,
            losing_trades INTEGER,
            win_rate REAL,
            total_pnl REAL,
            avg_pnl_per_trade REAL,
            avg_profit_winning_trade REAL,
            avg_loss_losing_trade REAL,
            max_profit_single_trade REAL,
            max_loss_single_trade REAL,
            profit_factor REAL,
            max_drawdown_percentage REAL,
            avg_trade_duration_hours REAL
        );""")
        conn.commit()
    except sqlite3.Error as e: print(f"Error creating {SUMMARY_RESULTS_TABLE_NAME}: {e}")

# --- Data Fetching and Processing ---
def fetch_single_interval_data_api(api_key, symbol, interval, start_date, end_date, max_outputsize, interval_minutes):
    print(f"API Fetch: For {symbol} from {start_date.strftime('%Y-%m-%d %H:%M:%S')} to {end_date.strftime('%Y-%m-%d %H:%M:%S')} ({interval})")
    dfs = []
    current_start = start_date
    while current_start < end_date:
        params = {"symbol": symbol, "interval": interval, "apikey": api_key, "start_date": current_start.strftime('%Y-%m-%d %H:%M:%S'), "end_date": min(current_start + timedelta(days=29), end_date).strftime('%Y-%m-%d %H:%M:%S'), "outputsize": max_outputsize, "order": "ASC"}
        try:
            response = requests.get(API_URL, params=params)
            response.raise_for_status()
            data = response.json()
            if data.get("status") == "ok" and data.get("values"):
                df = pd.DataFrame(data["values"])
                dfs.append(df)
                last_dt = pd.to_datetime(df['datetime'].iloc[-1]).replace(tzinfo=timezone.utc)
                current_start = last_dt + timedelta(minutes=interval_minutes)
                if len(df) < max_outputsize: break
            else: print(f"API Error/No Values: {data.get('message', 'Unknown API issue')}"); break
        except Exception as e: print(f"API Fetch Error: {e}"); break
    if not dfs: return pd.DataFrame()
    combined_df = pd.concat(dfs).drop_duplicates(subset=['datetime']).sort_values('datetime').reset_index(drop=True)
    for col in ['open', 'high', 'low', 'close']: combined_df[col] = pd.to_numeric(combined_df[col])
    combined_df['datetime'] = pd.to_datetime(combined_df['datetime'], utc=True)
    print(f"  API Fetch: Got {len(combined_df)} bars for {interval}.")
    return combined_df

def fetch_and_store_data_for_interval(conn, api_key, symbol, interval_str, cfg, overall_start, overall_end, max_outputsize):
    table, minutes = cfg['table_name'], cfg['minutes']
    api_param = "4h" if interval_str == "4hour" else interval_str
    print(f"\n--- Managing data for {interval_str} ({table}) ---")
    create_data_table_if_not_exists(conn, table)
    df_db = load_data_from_db(conn, table, overall_start.isoformat(), overall_end.isoformat())
    
    latest_db_dt_str = get_latest_datetime_in_db(conn, table)
    api_fetch_start = overall_start
    if latest_db_dt_str:
        latest_db_dt = pd.to_datetime(latest_db_dt_str, utc=True)
        if latest_db_dt >= overall_end - timedelta(minutes=minutes*2) and not df_db.empty and df_db['datetime'].min() <= overall_start:
            print(f"  DB data for {interval_str} is recent and covers range. Using DB data."); return df_db
        api_fetch_start = latest_db_dt + timedelta(minutes=minutes)
    
    if api_fetch_start < overall_end:
        print(f"  Fetching API for {interval_str} from {api_fetch_start.isoformat()}")
        df_new = fetch_single_interval_data_api(api_key, symbol, api_param, api_fetch_start, overall_end, max_outputsize, minutes)
        if not df_new.empty:
            save_dataframe_to_db(conn, table, df_new)
            return load_data_from_db(conn, table, overall_start.isoformat(), overall_end.isoformat()) # Reload full range
    return df_db

def fetch_all_timeframe_data_from_db(db_conn, api_key, symbol, intervals_cfg, end_date, max_outputsize):
    all_data = {}
    overall_start = end_date - timedelta(days=DAYS_TO_FETCH_DATA)
    for interval, cfg in intervals_cfg.items():
        all_data[interval] = fetch_and_store_data_for_interval(db_conn, api_key, symbol, interval, cfg, overall_start, end_date, max_outputsize)
    return all_data

def get_trend(df, sma_period=TREND_SMA_PERIOD):
    if df.empty or len(df) < sma_period: return "sideways"
    df_c = df.copy()
    if not pd.api.types.is_numeric_dtype(df_c['close']): df_c['close'] = pd.to_numeric(df_c['close'], errors='coerce')
    df_c['sma'] = ta.sma(df_c['close'], length=sma_period)
    df_c.dropna(subset=['close', 'sma'], inplace=True)
    if df_c.empty: return "sideways"
    lc, ls = df_c['close'].iloc[-1], df_c['sma'].iloc[-1]
    if pd.isna(ls) or pd.isna(lc): return "sideways"
    return "up" if lc > ls else "down" if lc < ls else "sideways"

def get_pivot_points(df_data):
    if df_data.empty: return [], []
    h, l, c = df_data['high'].iloc[-1], df_data['low'].iloc[-1], df_data['close'].iloc[-1]
    if any(pd.isna(x) for x in [h, l, c]): return [], []
    p = (h + l + c) / 3
    s = [(2*p)-h, p-(h-l), l-2*(h-p)]
    r = [(2*p)-l, p+(h-l), h+2*(p-l)]
    return sorted(list(set(x for x in s + [p] if pd.notna(x)))), sorted(list(set(x for x in r + [p] if pd.notna(x))))

def add_technical_indicators(df, rsi_p, sma_p):
    if df.empty: return df
    for col in ['close','high','low']: df[col] = pd.to_numeric(df[col], errors='coerce')
    df.dropna(subset=['close','high','low'], inplace=True)
    if df.empty: return df
    df['rsi'] = ta.rsi(df['close'], length=rsi_p)
    df['sma_rsi'] = ta.sma(df['rsi'], length=sma_p)
    df['atr'] = ta.atr(df['high'], df['low'], df['close'], length=14)
    return df

def is_valid_trading_time(ts_utc, windows_cfg):
    ts_utc = pd.to_datetime(ts_utc, utc=True)
    t_utc = ts_utc.time()
    return any(start <= t_utc <= end for start, end in windows_cfg.values())

def generate_trading_signals(df_5m, all_tf, windows_cfg, rsi_long_threshold, rsi_short_threshold, rsi_col='rsi', sma_col='sma_rsi'):
    print(f"\n--- Signal Generation (RSI Long < {rsi_long_threshold}, RSI Short > {rsi_short_threshold}) ---")
    signals = []
    df_15m, df_4h = all_tf.get("15min", pd.DataFrame()), all_tf.get("4hour", pd.DataFrame())
    valid_idx = df_5m[sma_col].first_valid_index()
    if valid_idx is None: return signals
    
    for i in range(max(1, valid_idx), len(df_5m) - 1):
        curr, prev = df_5m.iloc[i], df_5m.iloc[i-1]
        if any(pd.isna(curr[c]) or pd.isna(prev[c]) for c in [rsi_col, sma_col]) or pd.isna(curr['atr']): continue
        
        cross_dt = curr['datetime']
        trend_15m = get_trend(df_15m[df_15m['datetime'] <= cross_dt])
        
        rsi_curr, sma_curr, rsi_prev, sma_prev = curr[rsi_col], curr[sma_col], prev[rsi_col], prev[sma_col]
        is_long_cross = rsi_curr > sma_curr and rsi_prev <= sma_prev
        is_short_cross = rsi_curr < sma_curr and rsi_prev >= sma_prev
        # Parameterized RSI filters applied
        passes_rsi_filt = (is_long_cross and rsi_curr < rsi_long_threshold) or \
                          (is_short_cross and rsi_curr > rsi_short_threshold)
        if not passes_rsi_filt or not is_valid_trading_time(cross_dt, windows_cfg): continue

        confirm_candle = df_5m.iloc[i+1]
        direction = "long" if is_long_cross and confirm_candle['close'] > confirm_candle['open'] else \
                    "short" if is_short_cross and confirm_candle['close'] < confirm_candle['open'] else ""
        if not direction: continue
        # New rule: Only take trades if the 15-minute trend is "down"
        if trend_15m != "down": continue

        s_15m, r_15m = get_pivot_points(df_15m[df_15m['datetime'] < cross_dt])
        s_4h, r_4h = get_pivot_points(df_4h[df_4h['datetime'] < cross_dt])
        
        entry_px = confirm_candle['close']
        atr_val = curr['atr']
        near_thresh = (entry_px * 0.001) if (pd.isna(atr_val) or atr_val == 0) else (atr_val * SR_NEARNESS_FACTOR_ATR_MULTIPLE)
        
        blocked = False
        if direction == "long":
            for r_lvl in r_4h + r_15m:
                if entry_px < r_lvl and (r_lvl - entry_px) < near_thresh: blocked=True; break
        elif direction == "short":
            for s_lvl in s_4h + s_15m:
                if entry_px > s_lvl and (entry_px - s_lvl) < near_thresh: blocked=True; break
        if blocked: continue
            
        signals.append({**curr.to_dict(), **confirm_candle.add_prefix('cc_').to_dict(), 
                        "entry_datetime": confirm_candle['datetime'], "entry_price": entry_px, "direction": direction, 
                        "crossover_candle_datetime": cross_dt, "crossover_candle_index": i, 
                        "trend_15min_at_signal": trend_15m, "supports_15m_at_signal": json.dumps(s_15m), 
                        "resistances_15m_at_signal": json.dumps(r_15m), "supports_4h_at_signal": json.dumps(s_4h), 
                        "resistances_4h_at_signal": json.dumps(r_4h), "raw_signal_data_json": curr.to_json()}) # Added raw_signal_data
    print(f"Signals generated: {len(signals)}")
    return signals

def execute_backtest(df_5m, all_tf_data, losing_trades_conn, initial_balance, units, pip_val, signals, rsi_col='rsi', sma_col='sma_rsi'):
    print("\n--- Trade Simulation ---")
    balance = initial_balance
    trades = []
    
    # Ensure all_tf_data is available for pre-trade context
    df_15m = all_tf_data.get("15min", pd.DataFrame())
    df_4h = all_tf_data.get("4hour", pd.DataFrame())

    for sig in signals:
        entry_price = sig['entry_price']
        direction = sig['direction']

        # Parse S/R levels
        try:
            s_15m = json.loads(sig['supports_15m_at_signal']) if sig['supports_15m_at_signal'] else []
            r_15m = json.loads(sig['resistances_15m_at_signal']) if sig['resistances_15m_at_signal'] else []
            s_4h = json.loads(sig['supports_4h_at_signal']) if sig['supports_4h_at_signal'] else []
            r_4h = json.loads(sig['resistances_4h_at_signal']) if sig['resistances_4h_at_signal'] else []
        except json.JSONDecodeError:
            # print(f"Skipping trade due to S/R JSON parsing error for signal at {sig['entry_datetime']}")
            continue
            
        all_supports = sorted(list(set(s_15m + s_4h)))
        all_resistances = sorted(list(set(r_15m + r_4h)))

        stop_loss_price = None
        take_profit_price = None

        if direction == "long":
            # SL: Highest support strictly below entry
            potential_sls = [s for s in all_supports if s < entry_price]
            if potential_sls:
                stop_loss_price = max(potential_sls)
            
            # TP: Lowest resistance strictly above entry
            potential_tps = [r for r in all_resistances if r > entry_price]
            if potential_tps:
                take_profit_price = min(potential_tps)
        
        elif direction == "short":
            # SL: Lowest resistance strictly above entry
            potential_sls = [r for r in all_resistances if r > entry_price]
            if potential_sls:
                stop_loss_price = min(potential_sls)

            # TP: Highest support strictly below entry
            potential_tps = [s for s in all_supports if s < entry_price]
            if potential_tps:
                take_profit_price = max(potential_tps)

        # Validation and Skipping Trades
        if stop_loss_price is None or take_profit_price is None:
            # print(f"Skipping trade at {sig['entry_datetime']}: No valid S/R level for SL/TP.")
            continue

        if abs(entry_price - stop_loss_price) < (MIN_SL_DISTANCE_PIPS * pip_val):
            # print(f"Skipping trade at {sig['entry_datetime']}: SL too close ({stop_loss_price}). Entry: {entry_price}")
            continue
            
        risk = abs(entry_price - stop_loss_price)
        reward = abs(take_profit_price - entry_price)
        if risk == 0 or reward < risk: # Ensure risk is not zero and R:R is at least 1:1
            # print(f"Skipping trade at {sig['entry_datetime']}: Unfavorable R:R (Risk: {risk:.2f}, Reward: {reward:.2f}). SL: {stop_loss_price}, TP: {take_profit_price}")
            continue

        exit_px, exit_dt, exit_reason, duration = None, None, None, 0
        
        entry_idx = df_5m.index[df_5m['datetime'] == sig['entry_datetime']].tolist()
        if not entry_idx: continue
        
        for k in range(entry_idx[0] + 1, len(df_5m)):
            candle = df_5m.iloc[k]

            # S/R based SL/TP check
            if direction == "long":
                if candle['low'] <= stop_loss_price:
                    exit_px, exit_dt, exit_reason, duration = stop_loss_price, candle['datetime'], "SL Hit (S/R)", k - entry_idx[0]; break
                elif candle['high'] >= take_profit_price:
                    exit_px, exit_dt, exit_reason, duration = take_profit_price, candle['datetime'], "TP Hit (S/R)", k - entry_idx[0]; break
            elif direction == "short":
                if candle['high'] >= stop_loss_price:
                    exit_px, exit_dt, exit_reason, duration = stop_loss_price, candle['datetime'], "SL Hit (S/R)", k - entry_idx[0]; break
                elif candle['low'] <= take_profit_price:
                    exit_px, exit_dt, exit_reason, duration = take_profit_price, candle['datetime'], "TP Hit (S/R)", k - entry_idx[0]; break
            
            # RSI Crossover Stop (secondary exit)
            prev_candle_rsi = df_5m.iloc[k-1]
            if not any(pd.isna(x) for x in [candle[rsi_col], candle[sma_col], prev_candle_rsi[rsi_col], prev_candle_rsi[sma_col]]):
                if (sig['direction'] == "long" and candle[rsi_col] < candle[sma_col] and prev_candle_rsi[rsi_col] >= prev_candle_rsi[sma_col]) or \
                   (sig['direction'] == "short" and candle[rsi_col] > candle[sma_col] and prev_candle_rsi[rsi_col] <= prev_candle_rsi[sma_col]):
                    exit_px, exit_dt, exit_reason, duration = candle['close'], candle['datetime'], "RSI Crossover Stop", k - entry_idx[0]; break
        
        if exit_px:
            pnl = (exit_px - sig['entry_price'] if sig['direction'] == "long" else sig['entry_price'] - exit_px) * units
            balance += pnl
            trade_outcome = {**sig, "exit_datetime": exit_dt, "exit_price": exit_px, "exit_reason": exit_reason, 
                             "pnl": pnl, "account_balance_after_trade": balance, "duration_bars": duration,
                             "crossover_candle_rsi": sig[rsi_col], "crossover_candle_sma_rsi": sig[sma_col],
                             "crossover_candle_atr": sig['atr']}
            trades.append(trade_outcome)

            # Prepare data for DB logging (common for both losing and profitable)
            if losing_trades_conn: # Proceed only if DB connection exists
                trade_id = f"{sig['entry_datetime'].strftime('%Y%m%d%H%M%S')}_{sig['direction']}"
                
                # Pre-trade 5min data
                pre_5m_end_idx = sig['crossover_candle_index']
                pre_5m_start_idx = max(0, pre_5m_end_idx - PRE_TRADE_CANDLES_5MIN)
                df_5m_pre = df_5m.iloc[pre_5m_start_idx:pre_5m_end_idx].copy()

                # Pre-trade 15min data
                cross_dt_utc = pd.to_datetime(sig['crossover_candle_datetime'], utc=True)
                df_15m_pre = df_15m[df_15m['datetime'] < cross_dt_utc].tail(PRE_TRADE_CANDLES_15MIN).copy()

                # Pre-trade 4h data
                df_4h_pre = df_4h[df_4h['datetime'] < cross_dt_utc].tail(PRE_TRADE_CANDLES_4HOUR).copy()
                
                trade_db_record = {
                    'trade_id': trade_id,
                    'entry_datetime': sig['entry_datetime'], 'entry_price': sig['entry_price'], 'direction': sig['direction'],
                    'exit_datetime': exit_dt, 'exit_price': exit_px, 'pnl': pnl, 'exit_reason': exit_reason,
                    'crossover_candle_datetime': sig['crossover_candle_datetime'],
                    'crossover_candle_rsi': sig[rsi_col], 'crossover_candle_sma_rsi': sig[sma_col],
                    'crossover_candle_atr': sig['atr'], 'trend_15min_at_signal': sig['trend_15min_at_signal'],
                    'sr_15min_supports_json': sig['supports_15m_at_signal'], 
                    'sr_15min_resistances_json': sig['resistances_15m_at_signal'],
                    'sr_4hour_supports_json': sig['supports_4h_at_signal'],
                    'sr_4hour_resistances_json': sig['resistances_4h_at_signal'],
                    'pre_trade_data_5min_json': df_5m_pre.to_json(orient='records', date_format='iso') if not df_5m_pre.empty else None,
                    'pre_trade_data_15min_json': df_15m_pre.to_json(orient='records', date_format='iso') if not df_15m_pre.empty else None,
                    'pre_trade_data_4hour_json': df_4h_pre.to_json(orient='records', date_format='iso') if not df_4h_pre.empty else None,
                    'raw_signal_data_json': sig['raw_signal_data_json']
                }

                if pnl < 0:
                    save_losing_trade_details_to_db(losing_trades_conn, trade_db_record)
                elif pnl > 0: # Save profitable trades
                    save_profitable_trade_details_to_db(losing_trades_conn, trade_db_record)
                # Breakeven trades (pnl == 0) are currently not logged with context.
                
    print(f"Total executed trades: {len(trades)}")
    return trades, balance

def calculate_and_print_metrics(trades, initial_balance, final_balance, interval_minutes):
    print("\n--- Performance Metrics ---")
    if not trades: 
        print("No trades executed.")
        return

    total_trades = len(trades)
    pnl_values = [trade['pnl'] for trade in trades] # Store P&L values for easier access

    winning_trades_pnl = [pnl for pnl in pnl_values if pnl > 0]
    losing_trades_pnl = [pnl for pnl in pnl_values if pnl < 0]

    num_winning_trades = len(winning_trades_pnl)
    num_losing_trades = len(losing_trades_pnl)

    total_pnl_overall = final_balance - initial_balance 
    
    gross_profit = sum(winning_trades_pnl)
    # gross_loss for Profit Factor is usually positive, sum of absolute losses
    gross_loss_for_pf = abs(sum(losing_trades_pnl)) 

    avg_pnl_per_trade_all = total_pnl_overall / total_trades if total_trades > 0 else 0.0

    avg_profit_per_winning_trade = gross_profit / num_winning_trades if num_winning_trades > 0 else 0.0
    # For average loss, we use the actual sum of negative P&Ls
    avg_loss_per_losing_trade = sum(losing_trades_pnl) / num_losing_trades if num_losing_trades > 0 else 0.0 

    max_single_trade_profit = max(pnl_values) if pnl_values else 0.0
    # min_single_trade_pnl will be the most negative P&L, representing the max loss
    max_single_trade_loss = min(pnl_values) if pnl_values else 0.0 

    equity_curve = [initial_balance] + [trade['account_balance_after_trade'] for trade in trades]
    peak_equity = equity_curve[0]
    max_drawdown = 0.0
    for equity_value in equity_curve:
        if equity_value > peak_equity:
            peak_equity = equity_value
        drawdown = (peak_equity - equity_value) / peak_equity if peak_equity != 0 else 0
        if drawdown > max_drawdown:
            max_drawdown = drawdown
            
    avg_trade_duration_seconds = sum((pd.to_datetime(t['exit_datetime']) - pd.to_datetime(t['entry_datetime'])).total_seconds() for t in trades) / total_trades if total_trades > 0 else 0

    print(f"Total Return: {((final_balance - initial_balance) / initial_balance) * 100:.2f}%")
    print(f"Total P&L: ${total_pnl_overall:,.2f}")
    print(f"Total Trades: {total_trades}, Wins: {num_winning_trades}, Losses: {num_losing_trades}")
    print(f"Win Rate: {(num_winning_trades / total_trades) * 100:.2f}%" if total_trades > 0 else "N/A")
    
    print(f"Avg P&L per Trade (All Trades): ${avg_pnl_per_trade_all:,.2f}")
    if num_winning_trades > 0:
        print(f"Avg Profit per Winning Trade: ${avg_profit_per_winning_trade:,.2f}")
    else:
        print("Avg Profit per Winning Trade: N/A (No winning trades)")
    if num_losing_trades > 0:
        # This will print as a negative value, e.g., "Avg Loss per Losing Trade: $-100.00"
        print(f"Avg Loss per Losing Trade: ${avg_loss_per_losing_trade:,.2f}") 
    else:
        print("Avg Loss per Losing Trade: N/A (No losing trades)")

    if pnl_values: 
        print(f"Max Profit in a Single Trade: ${max_single_trade_profit:,.2f}")
        # This will print as a negative value, e.g., "Max Loss in a Single Trade: $-200.00"
        print(f"Max Loss in a Single Trade: ${max_single_trade_loss:,.2f}") 
    else:
        print("Max Profit in a Single Trade: N/A")
        print("Max Loss in a Single Trade: N/A")

    print(f"Profit Factor: {gross_profit / gross_loss_for_pf:.2f}" if gross_loss_for_pf != 0 else "Infinite")
    print(f"Max Drawdown: {max_drawdown * 100:.2f}%")
    print(f"Avg Trade Duration: {avg_trade_duration_seconds / 3600:.2f} hours")
    print("---------------------------")

    # Prepare dictionary of metrics to return
    metrics_dict = {
        "total_trades": total_trades,
        "winning_trades": num_winning_trades,
        "losing_trades": num_losing_trades,
        "win_rate": (num_winning_trades / total_trades) * 100 if total_trades > 0 else 0.0,
        "total_pnl": total_pnl_overall,
        "avg_pnl_per_trade": avg_pnl_per_trade_all,
        "avg_profit_winning_trade": avg_profit_per_winning_trade if num_winning_trades > 0 else 0.0,
        "avg_loss_losing_trade": avg_loss_per_losing_trade if num_losing_trades > 0 else 0.0,
        "max_profit_single_trade": max_single_trade_profit if pnl_values else 0.0,
        "max_loss_single_trade": max_single_trade_loss if pnl_values else 0.0,
        "profit_factor": gross_profit / gross_loss_for_pf if gross_loss_for_pf != 0 else float('inf'),
        "max_drawdown_percentage": max_drawdown * 100,
        "avg_trade_duration_hours": avg_trade_duration_seconds / 3600 if total_trades > 0 else 0.0
    }
    return metrics_dict

def save_summary_results_to_db(conn, metrics_dict, rsi_filter_long_str, rsi_filter_short_str, rr_multiplier_val):
    """Saves the summary metrics of a backtest run to the database."""
    try:
        data_to_insert = {
            "run_datetime": datetime.now(timezone.utc).isoformat(),
            "rsi_filter_long": rsi_filter_long_str,
            "rsi_filter_short": rsi_filter_short_str,
            "rr_multiplier": rr_multiplier_val,
            **metrics_dict
        }
        
        # Define the exact order and names of columns for the table
        ordered_columns = [
            "run_datetime", "rsi_filter_long", "rsi_filter_short", "rr_multiplier",
            "total_trades", "winning_trades", "losing_trades", "win_rate",
            "total_pnl", "avg_pnl_per_trade", "avg_profit_winning_trade",
            "avg_loss_losing_trade", "max_profit_single_trade", "max_loss_single_trade",
            "profit_factor", "max_drawdown_percentage", "avg_trade_duration_hours"
        ]
        
        # Prepare values in the correct order, substituting None if a key is missing from metrics_dict
        values_to_insert = [data_to_insert.get(col) for col in ordered_columns]

        columns_str = ', '.join(ordered_columns)
        placeholders_str = ', '.join('?' * len(ordered_columns))
        
        sql = f"INSERT INTO {SUMMARY_RESULTS_TABLE_NAME} ({columns_str}) VALUES ({placeholders_str})"
        
        conn.cursor().execute(sql, values_to_insert)
        conn.commit()
        print(f"Successfully saved summary metrics for run {data_to_insert['run_datetime']} (R:R {rr_multiplier_val}) to {SUMMARY_RESULTS_TABLE_NAME}.")
    except sqlite3.Error as e:
        print(f"Error saving summary metrics to {SUMMARY_RESULTS_TABLE_NAME} (R:R {rr_multiplier_val}): {e}")
    except Exception as e:
        print(f"An unexpected error occurred while saving summary metrics: {e}")

def calculate_and_print_monthly_performance(trades, initial_balance, df_5m_atr):
    print("\n--- Monthly Performance Breakdown ---")
    if not trades: print("No trades for monthly breakdown."); return
    trades_df = pd.DataFrame(trades)
    if trades_df.empty: print("Trades DataFrame empty."); return
    trades_df['entry_datetime'] = pd.to_datetime(trades_df['entry_datetime'])
    trades_df['year_month'] = trades_df['entry_datetime'].dt.to_period('M')
    monthly_groups = trades_df.groupby('year_month')
    print("\n{:<10} | {:>12} | {:>10} | {:>6} | {:>10}".format("Month", "P&L", "Trades", "Wins", "Win Rate %"))
    print("-" * 65)
    
    grand_total_pnl = 0
    grand_total_trades = 0
    grand_total_wins = 0
    
    for period, group_df in monthly_groups:
        monthly_pnl, num_trades = group_df['pnl'].sum(), len(group_df)
        wins = len(group_df[group_df['pnl'] > 0])
        win_rate = (wins / num_trades) * 100 if num_trades > 0 else 0
        print(f"{str(period):<10} | ${monthly_pnl:>11,.2f} | {num_trades:>10} | {wins:>6} | {win_rate:>9.2f}%")
        
        grand_total_pnl += monthly_pnl
        grand_total_trades += num_trades
        grand_total_wins += wins
        
    print("-" * 65) # Print separator before the total line
    overall_win_rate = (grand_total_wins / grand_total_trades) * 100 if grand_total_trades > 0 else 0.0
    print(f"{'Total':<10} | ${grand_total_pnl:>11,.2f} | {grand_total_trades:>10} | {grand_total_wins:>6} | {overall_win_rate:>9.2f}%")
    print("-----------------------------------") # Original final separator

def main():
    # Optimized RSI settings
    OPTIMIZED_RSI_LONG_THRESHOLD = 46
    OPTIMIZED_RSI_SHORT_THRESHOLD = 60

    end_date_target = datetime.now(timezone.utc)
    hist_data_conn = create_db_connection(DB_FILE_PATH)
    losing_trades_conn = create_db_connection(LOSING_TRADES_DB_PATH)

    if hist_data_conn is None or losing_trades_conn is None:
        print("Failed to connect to one or more databases. Exiting.")
        if hist_data_conn: hist_data_conn.close()
        if losing_trades_conn: losing_trades_conn.close()
        return

    try:
        cursor = losing_trades_conn.cursor()

        # Drop and recreate per-run detail tables
        cursor.execute(f"DROP TABLE IF EXISTS {LOSING_TRADES_TABLE_NAME}")
        cursor.execute(f"DROP TABLE IF EXISTS {PROFITABLE_TRADES_TABLE_NAME}")
        losing_trades_conn.commit()
        print(f"Dropped per-run detail tables: {LOSING_TRADES_TABLE_NAME}, {PROFITABLE_TRADES_TABLE_NAME}.")
        
        create_losing_trades_table(losing_trades_conn)
        create_profitable_trades_table(losing_trades_conn)
        
        # Ensure SUMMARY_RESULTS_TABLE_NAME exists with the correct schema, but do not drop it to accumulate results.
        # The create_summary_results_table function uses "CREATE TABLE IF NOT EXISTS"
        create_summary_results_table(losing_trades_conn) 
        print(f"Ensured table {SUMMARY_RESULTS_TABLE_NAME} exists.")

        # Fetch and prepare data
        all_tf_data = fetch_all_timeframe_data_from_db(hist_data_conn, API_KEY, SYMBOL, INTERVALS_CONFIG, end_date_target, OUTPUT_SIZE)
        df_5m_original = all_tf_data.get("5min", pd.DataFrame())
        if df_5m_original.empty:
            print("\nNo 5min data. Cannot proceed.")
            return
        df_5m_original = add_technical_indicators(df_5m_original, RSI_PERIOD, SMA_PERIOD_ON_RSI)
        
        print(f"\n--- Running Backtest with Optimized RSI Settings: Long < {OPTIMIZED_RSI_LONG_THRESHOLD}, Short > {OPTIMIZED_RSI_SHORT_THRESHOLD} ---")
        print(f"Initial Balance: ${INITIAL_BALANCE:,.2f}, Units: {TRADE_UNITS}")
        print(f"15m Trend SMA: {TREND_SMA_PERIOD}, S/R: Pivots, Nearness: {SR_NEARNESS_FACTOR_ATR_MULTIPLE}*ATR")

        signals = generate_trading_signals(df_5m_original, all_tf_data, TRADING_WINDOWS_UTC, OPTIMIZED_RSI_LONG_THRESHOLD, OPTIMIZED_RSI_SHORT_THRESHOLD)
        
        executed_trades, final_balance = execute_backtest(df_5m_original, all_tf_data, losing_trades_conn, INITIAL_BALANCE, TRADE_UNITS, PIP_VALUE_XAU_USD, signals)
        
        metrics_summary = calculate_and_print_metrics(executed_trades, INITIAL_BALANCE, final_balance, INTERVALS_CONFIG['5min']['minutes'])
        
        if metrics_summary and losing_trades_conn:
            rsi_long_str = f"< {OPTIMIZED_RSI_LONG_THRESHOLD}"
            rsi_short_str = f"> {OPTIMIZED_RSI_SHORT_THRESHOLD}"
            # RR_MULTIPLIER is no longer used for TP, pass None or a descriptive string if schema changed
            save_summary_results_to_db(losing_trades_conn, metrics_summary, rsi_long_str, rsi_short_str, None) 
            
        calculate_and_print_monthly_performance(executed_trades, INITIAL_BALANCE, df_5m_original)
        
        print("\n--- Backtest with Optimized Settings Finished ---")

    finally:
        if hist_data_conn: hist_data_conn.close(); print("Hist. DB conn closed.")
        if losing_trades_conn: losing_trades_conn.close(); print(f"Losing Trades DB conn closed.")
    print("\nScript execution finished.")

if __name__ == "__main__":
    main()
