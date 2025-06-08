import logging
import time
from datetime import datetime, timedelta, date
import MetaTrader5 as mt5 # For TIMEFRAME constants
import mt5_integration_module as mt5_conn 
# mt5_integration_module.py is in the same directory

# Configure logging for the live trader
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[
        logging.FileHandler("silver_bot_live_trader.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Default values, will be overridden by config
TRADING_SYMBOL = "XAGUSD" 
MAGIC_NUMBER = 123456     
LOT_SIZE = 0.01           
CONTRACT_SIZE = 5000 # Default, will try to fetch dynamically

# Strategy Parameters (defaults, will be loaded from config)
ENTRY_THRESHOLD_PERCENT = 0.005
TAKE_PROFIT_PRICE_OFFSET_LONG = 0.5
LONG_ENTRY_DOLLAR_STEP = 0.5

SHORT_LOT_SIZE_FRACTION_OF_LONG = 1.0
SHORT_ENTRY_PRICE_OFFSET_FROM_LONG = 2.0 
SHORT_TAKE_PROFIT_PRICE_OFFSET = 1.00

MAX_CONCURRENT_TRADES = 30
NINETY_DAY_PNL_THRESHOLD_INR = -10000.0

MT5_TIMEFRAME = mt5.TIMEFRAME_M15 
CANDLE_FETCH_COUNT = 50 
LOOP_SLEEP_SECONDS = 60 

# Global variable to store symbol info once fetched
symbol_info_global = None

def get_daily_open_price(symbol, candles_df):
    if candles_df is None or candles_df.empty:
        logger.warning("Cannot determine daily open: candles_df is empty.")
        return None
    
    last_candle_time = candles_df['time'].iloc[-1]
    current_trading_day_start = last_candle_time.replace(hour=0, minute=0, second=0, microsecond=0)
    
    daily_candles = candles_df[candles_df['time'] >= current_trading_day_start]
    if not daily_candles.empty:
        return daily_candles['open'].iloc[0]
    else:
        logger.warning(f"No candles found for the current trading day ({current_trading_day_start}) in the fetched data. Cannot determine daily open.")
        return None

def main_trading_loop():
    logger.info("Starting Silver Bot Live Trader...")
    
    global TRADING_SYMBOL, MAGIC_NUMBER, LOT_SIZE, CONTRACT_SIZE
    global ENTRY_THRESHOLD_PERCENT, TAKE_PROFIT_PRICE_OFFSET_LONG, LONG_ENTRY_DOLLAR_STEP
    global SHORT_LOT_SIZE_FRACTION_OF_LONG, SHORT_ENTRY_PRICE_OFFSET_FROM_LONG, SHORT_TAKE_PROFIT_PRICE_OFFSET
    global MAX_CONCURRENT_TRADES, NINETY_DAY_PNL_THRESHOLD_INR
    global symbol_info_global

    try:
        config = mt5_conn.load_config()
        # Load Trading Parameters
        TRADING_SYMBOL = config['Trading_Parameters'].get('symbol', TRADING_SYMBOL)
        MAGIC_NUMBER = int(config['Trading_Parameters'].get('magic_number', MAGIC_NUMBER))
        LOT_SIZE = float(config['Trading_Parameters'].get('lot_size', LOT_SIZE))

        # Load Strategy Parameters
        ENTRY_THRESHOLD_PERCENT = float(config['Strategy_Parameters'].get('entry_threshold_percent', ENTRY_THRESHOLD_PERCENT))
        TAKE_PROFIT_PRICE_OFFSET_LONG = float(config['Strategy_Parameters'].get('take_profit_offset_long', TAKE_PROFIT_PRICE_OFFSET_LONG))
        LONG_ENTRY_DOLLAR_STEP = float(config['Strategy_Parameters'].get('long_entry_dollar_step', LONG_ENTRY_DOLLAR_STEP))
        
        SHORT_LOT_SIZE_FRACTION_OF_LONG = float(config['Strategy_Parameters'].get('short_lot_size_fraction_of_long', SHORT_LOT_SIZE_FRACTION_OF_LONG))
        SHORT_ENTRY_PRICE_OFFSET_FROM_LONG = float(config['Strategy_Parameters'].get('short_entry_price_offset_from_long', SHORT_ENTRY_PRICE_OFFSET_FROM_LONG))
        SHORT_TAKE_PROFIT_PRICE_OFFSET = float(config['Strategy_Parameters'].get('short_take_profit_offset', SHORT_TAKE_PROFIT_PRICE_OFFSET))
        
        MAX_CONCURRENT_TRADES = int(config['Strategy_Parameters'].get('max_concurrent_trades', MAX_CONCURRENT_TRADES))
        NINETY_DAY_PNL_THRESHOLD_INR = float(config['Strategy_Parameters'].get('ninety_day_pnl_threshold_inr', NINETY_DAY_PNL_THRESHOLD_INR))
        
        logger.info("All strategy parameters loaded from config.")

    except Exception as e:
        logger.error(f"Error loading full configuration: {e}. Critical error, exiting.")
        return

    if not mt5_conn.connect_to_mt5():
        logger.error("Failed to connect to MT5. Exiting live trader.")
        return

    logger.info(f"Successfully connected to MT5. Symbol: {TRADING_SYMBOL}, Magic: {MAGIC_NUMBER}, Lots: {LOT_SIZE}")
    
    symbol_info_global = mt5_conn.get_symbol_details(TRADING_SYMBOL)
    if symbol_info_global and hasattr(symbol_info_global, 'trade_contract_size'):
        CONTRACT_SIZE = symbol_info_global.trade_contract_size
        logger.info(f"Using dynamic contract size for {TRADING_SYMBOL}: {CONTRACT_SIZE}")
    else:
        logger.warning(f"Could not fetch dynamic contract size for {TRADING_SYMBOL}. Using default: {CONTRACT_SIZE}")

    daily_open_price = None
    last_day_checked_for_open = None 

    try:
        while True:
            current_time_dt = datetime.now()
            
            logger.info(f"Fetching latest {CANDLE_FETCH_COUNT} candles for {TRADING_SYMBOL} on {MT5_TIMEFRAME}...")
            candles_df = mt5_conn.fetch_latest_candles(TRADING_SYMBOL, MT5_TIMEFRAME, CANDLE_FETCH_COUNT)

            if candles_df is None or candles_df.empty:
                logger.warning("Could not fetch candles. Skipping this cycle.")
                time.sleep(LOOP_SLEEP_SECONDS)
                continue

            current_candle_day_date = candles_df['time'].iloc[-1].date()
            if last_day_checked_for_open != current_candle_day_date:
                logger.info(f"New day ({current_candle_day_date}) or first run. Determining daily open price.")
                daily_open_price = get_daily_open_price(TRADING_SYMBOL, candles_df)
                if daily_open_price:
                    logger.info(f"Daily open for {TRADING_SYMBOL} on {current_candle_day_date}: {daily_open_price:.5f}")
                    last_day_checked_for_open = current_candle_day_date
                else:
                    logger.warning("Could not determine daily open price. Strategy may not function correctly.")
            
            if daily_open_price is None:
                logger.warning("Daily open price is not set. Cannot evaluate entry signals or manage trades. Skipping cycle.")
                time.sleep(LOOP_SLEEP_SECONDS)
                continue

            # --- 1. Manage Open Positions (90-Day Rule) ---
            all_bot_positions = mt5_conn.get_open_trades(symbol_name=TRADING_SYMBOL, magic_number_filter=MAGIC_NUMBER)
            if all_bot_positions:
                logger.info(f"Checking {len(all_bot_positions)} open positions for 90-day rule...")
                for pos in all_bot_positions:
                    try:
                        entry_datetime = datetime.fromtimestamp(pos.time)
                        days_held = (current_time_dt.date() - entry_datetime.date()).days
                        
                        if days_held > 90:
                            net_pnl_inr = pos.profit # Using Net P&L now
                            logger.info(f"Position {pos.ticket} held for {days_held} days. Current Net PnL: {net_pnl_inr:.2f} INR. (Swap: {pos.swap:.2f}, Comm: {pos.commission:.2f})")
                            if net_pnl_inr > NINETY_DAY_PNL_THRESHOLD_INR:
                                logger.info(f"Closing position {pos.ticket} due to 90-day rule (Net PnL {net_pnl_inr:.2f} > {NINETY_DAY_PNL_THRESHOLD_INR:.2f} INR).")
                                close_result = mt5_conn.close_trade_by_ticket(pos.ticket, comment="90-day rule closure (Net PnL)")
                                logger.info(f"90-day rule close result for {pos.ticket}: {close_result}")
                            else:
                                logger.info(f"Position {pos.ticket} held >90 days, but Net PnL {net_pnl_inr:.2f} INR is not > {NINETY_DAY_PNL_THRESHOLD_INR:.2f} INR. Not closing.")
                    except Exception as e_90day:
                        logger.error(f"Error processing position {pos.ticket} for 90-day rule: {e_90day}")
                # Re-fetch positions after potential closures
                all_bot_positions = mt5_conn.get_open_trades(symbol_name=TRADING_SYMBOL, magic_number_filter=MAGIC_NUMBER)
                if all_bot_positions is None: all_bot_positions = [] # Ensure it's a list

            # --- 2. Evaluate New Entry Signals ---
            # latest_low_price = candles_df['low'].iloc[-1] # No longer using latest_low_price for entry condition
            
            current_tick_info = mt5.symbol_info_tick(TRADING_SYMBOL)
            current_market_ask_price = None
            if not current_tick_info or current_tick_info.ask == 0:
                logger.warning(f"Could not get valid current ask price for {TRADING_SYMBOL}. Skipping entry evaluation for this cycle.")
            else:
                current_market_ask_price = current_tick_info.ask

            # Concurrency Check
            num_open_positions = len(all_bot_positions) if all_bot_positions else 0
            logger.info(f"Currently {num_open_positions} open positions by this bot (Magic: {MAGIC_NUMBER}). Max allowed: {MAX_CONCURRENT_TRADES}.")

            if num_open_positions < MAX_CONCURRENT_TRADES:
                open_long_positions_today = [
                    p for p in (all_bot_positions if all_bot_positions else [])
                    if p.type == mt5.ORDER_TYPE_BUY and datetime.fromtimestamp(p.time_setup).date() == current_candle_day_date
                ]
                entries_today_count = len(open_long_positions_today)
                
                next_entry_target_price_long = None
                if entries_today_count == 0:
                    next_entry_target_price_long = daily_open_price * (1 - ENTRY_THRESHOLD_PERCENT)
                    logger.info(f"Eval Long (1st of day): DailyOpen={daily_open_price:.5f}, TargetEntry={next_entry_target_price_long:.5f}")
                else:
                    # Find the entry price of the most recent long trade opened today
                    if open_long_positions_today:
                        # Sort by setup time descending to get the latest
                        open_long_positions_today.sort(key=lambda p: p.time_setup, reverse=True)
                        most_recent_long_entry_price_today = open_long_positions_today[0].price_open
                        next_entry_target_price_long = most_recent_long_entry_price_today - LONG_ENTRY_DOLLAR_STEP
                        logger.info(f"Eval Long (Subsequent): LastLongEntry={most_recent_long_entry_price_today:.5f}, DollarStep={LONG_ENTRY_DOLLAR_STEP}, TargetEntry={next_entry_target_price_long:.5f}")
                    else: # Should not happen if entries_today_count > 0, but as a safeguard
                        next_entry_target_price_long = daily_open_price * (1 - ENTRY_THRESHOLD_PERCENT) 
                        logger.warning(f"Inconsistent state: entries_today_count={entries_today_count} but no open_long_positions_today found. Defaulting to 1st of day logic.")
                
                # Condition now uses current_market_ask_price instead of latest_low_price
                if next_entry_target_price_long and current_market_ask_price is not None and current_market_ask_price <= next_entry_target_price_long:
                    logger.info(f"Long entry signal detected for {TRADING_SYMBOL} at market (Target: {next_entry_target_price_long:.5f}, CurrentAsk: {current_market_ask_price:.5f}).")
                    
                    # current_ask is already fetched as current_market_ask_price
                    if current_market_ask_price > 0: # Ensure it's valid (already checked but good for safety)
                        long_fill_price = current_market_ask_price # Assuming market order fills at current ask
                        tp_long = round(long_fill_price + TAKE_PROFIT_PRICE_OFFSET_LONG, symbol_info_global.digits if symbol_info_global else 5)
                        
                        logger.info(f"Placing LONG market order for {TRADING_SYMBOL} at ~{long_fill_price:.5f}, TP: {tp_long:.5f}")
                        result_long = mt5_conn.place_market_trade(
                            TRADING_SYMBOL, mt5.ORDER_TYPE_BUY, LOT_SIZE,
                            take_profit_abs_price=tp_long,
                            magic_number=MAGIC_NUMBER,
                            comment=f"LiveBot Long {entries_today_count+1}"
                        )
                        logger.info(f"Long trade placement result: {result_long}")

                        if result_long and result_long.retcode == mt5.TRADE_RETCODE_DONE:
                            actual_long_fill_price = result_long.price # Get actual fill price from result
                            logger.info(f"Long trade successfully placed. Ticket: {result_long.order}, Fill Price: {actual_long_fill_price:.5f}")

                            # Place Paired Short SELL LIMIT Order
                            target_short_entry_price = round(actual_long_fill_price + SHORT_ENTRY_PRICE_OFFSET_FROM_LONG, symbol_info_global.digits if symbol_info_global else 5)
                            tp_short = round(target_short_entry_price - SHORT_TAKE_PROFIT_PRICE_OFFSET, symbol_info_global.digits if symbol_info_global else 5)
                            short_volume = round(LOT_SIZE * SHORT_LOT_SIZE_FRACTION_OF_LONG, 2)
                            
                            min_vol = 0.01 
                            if symbol_info_global and hasattr(symbol_info_global, 'volume_min'):
                                min_vol = symbol_info_global.volume_min
                            if short_volume < min_vol: 
                                logger.warning(f"Calculated short volume {short_volume} is less than min volume {min_vol}. Adjusting to min_volume.")
                                short_volume = min_vol

                            logger.info(f"Placing PAIRED SHORT SELL LIMIT order for {TRADING_SYMBOL} at {target_short_entry_price:.5f}, TP: {tp_short:.5f}, Vol: {short_volume}")
                            result_short_limit = mt5_conn.place_limit_order(
                                TRADING_SYMBOL, mt5.ORDER_TYPE_SELL_LIMIT, short_volume,
                                limit_price=target_short_entry_price,
                                take_profit_abs_price=tp_short,
                                magic_number=MAGIC_NUMBER, 
                                comment=f"LiveBot Paired Short Limit for L {result_long.order}"
                            )
                            logger.info(f"Paired Short SELL LIMIT order placement result: {result_short_limit}")
                            if not (result_short_limit and result_short_limit.retcode == mt5.TRADE_RETCODE_PLACED):
                                logger.error(f"Failed to place Paired Short SELL LIMIT order. Result: {result_short_limit}")
                        else:
                            logger.error(f"Failed to place Long trade. Result: {result_long}. Paired short will not be placed.")
                    # else: # This case is now covered by current_market_ask_price being None or not > 0
                        # logger.warning(f"Could not get valid ASK price for {TRADING_SYMBOL} to place LONG trade.") # Already logged if current_market_ask_price is None
                else:
                    if next_entry_target_price_long and current_market_ask_price is not None: # Only log if target and ask were valid
                         logger.info(f"No long entry signal: CurrentAsk ({current_market_ask_price:.5f}) > TargetEntry ({next_entry_target_price_long:.5f})")
                    elif next_entry_target_price_long and current_market_ask_price is None:
                        logger.info(f"No long entry signal: Could not get current ask price. Target was {next_entry_target_price_long:.5f}")
                    # else: # No target price was set (e.g. daily open not determined) - already logged earlier
            else:
                logger.info(f"Concurrency limit reached ({num_open_positions}/{MAX_CONCURRENT_TRADES}). No new trades will be opened.")

            logger.info(f"Cycle finished. Sleeping for {LOOP_SLEEP_SECONDS} seconds.")
            time.sleep(LOOP_SLEEP_SECONDS)

    except KeyboardInterrupt:
        logger.info("Trader shutdown requested (KeyboardInterrupt).")
    except Exception as e:
        logger.error(f"An critical error occurred in the main trading loop: {e}", exc_info=True)
    finally:
        logger.info("Disconnecting from MT5...")
        mt5_conn.disconnect_from_mt5()
        logger.info("Silver Bot Live Trader stopped.")

if __name__ == "__main__":
    main_trading_loop()
