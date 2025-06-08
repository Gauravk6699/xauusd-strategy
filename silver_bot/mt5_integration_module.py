import MetaTrader5 as mt5
import configparser
import logging
import os
from datetime import datetime
import pandas as pd # Import pandas at the module level

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
    handlers=[
        logging.FileHandler("silver_bot_mt5_integration.log"),
        logging.StreamHandler()
    ]
)

CONFIG_FILE_PATH = os.path.join(os.path.dirname(__file__), 'mt5_config.ini')

def load_config():
    """Loads MT5 configuration from mt5_config.ini."""
    config = configparser.ConfigParser()
    if not os.path.exists(CONFIG_FILE_PATH):
        logging.error(f"Configuration file not found: {CONFIG_FILE_PATH}")
        raise FileNotFoundError(f"Configuration file not found: {CONFIG_FILE_PATH}")
    config.read(CONFIG_FILE_PATH)
    return config

def connect_to_mt5():
    """Initializes and connects to the MetaTrader 5 terminal."""
    try:
        config = load_config()
        account = int(config['MT5_Credentials']['account'])
        password = config['MT5_Credentials']['password']
        server = config['MT5_Credentials']['server']
        path = config['MT5_Credentials'].get('mt5_terminal_path', None) # Optional path

        logging.info("Attempting to initialize MetaTrader 5...")
        initialized = False
        if path:
            initialized = mt5.initialize(path=path, login=account, password=password, server=server)
        else:
            initialized = mt5.initialize(login=account, password=password, server=server)

        if not initialized:
            logging.error(f"MT5 initialize() failed. Error code: {mt5.last_error()}")
            return False
        logging.info(f"MT5 initialize() call returned: {initialized}. Last error after initialize: {mt5.last_error()}")

        # Verify connection by checking terminal and account info
        terminal_info = mt5.terminal_info()
        logging.info(f"mt5.terminal_info() call done. Last error: {mt5.last_error()}")
        if terminal_info is None:
            logging.error(f"MT5 terminal_info() returned None after initialization. Error code: {mt5.last_error()}")
            mt5.shutdown()
            return False
        
        logging.info(f"Type of terminal_info: {type(terminal_info)}")
        logging.info(f"Attributes of terminal_info: {dir(terminal_info)}")

        account_info = mt5.account_info()
        logging.info(f"mt5.account_info() call done. Last error: {mt5.last_error()}")
        if account_info is None:
            logging.error(f"MT5 account_info() returned None after initialization. Error code: {mt5.last_error()}")
            mt5.shutdown()
            return False
            
        logging.info(f"Type of account_info: {type(account_info)}")
        logging.info(f"Attributes of account_info: {dir(account_info)}")

        # Check for essential attributes before accessing them
        if not hasattr(terminal_info, 'build'): # Only check for 'build' as 'version' is missing
            logging.error(f"TerminalInfo object does not have 'build' attribute. Available attributes: {dir(terminal_info)}")
            mt5.shutdown()
            return False
            
        if not hasattr(account_info, 'login') or not hasattr(account_info, 'server') or not hasattr(account_info, 'balance') or not hasattr(account_info, 'currency'):
            logging.error(f"AccountInfo object is missing one or more essential attributes. Available attributes: {dir(account_info)}")
            mt5.shutdown()
            return False

        logging.info(f"Successfully initialized MT5. Terminal build: {terminal_info.build}") # Log only build
        logging.info(f"Connected to account: {account_info.login} on server: {account_info.server}. Balance: {account_info.balance} {account_info.currency}")
        return True

    except FileNotFoundError:
        # Already logged in load_config
        return False
    except KeyError as e:
        logging.error(f"Missing configuration key in {CONFIG_FILE_PATH}: {e}")
        return False
    except ValueError as e:
        logging.error(f"Invalid configuration value in {CONFIG_FILE_PATH} (e.g., account number not an integer): {e}")
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred during MT5 connection: {e}")
        # Attempt shutdown only if we think it might have been initialized
        # A more robust check might involve a flag set after successful mt5.initialize()
        try:
            if mt5.terminal_info() is not None: # Check if terminal_info is accessible
                 mt5.shutdown()
        except Exception as shutdown_e:
            logging.error(f"Error during MT5 shutdown in exception handler: {shutdown_e}")
        return False

def disconnect_from_mt5():
    """Shuts down the connection to MetaTrader 5."""
    logging.info("Shutting down MetaTrader 5 connection.")
    mt5.shutdown()

def get_account_details():
    """Retrieves and logs account information."""
    if not mt5.terminal_info():
        logging.warning("Not connected to MT5. Cannot get account details.")
        return None
        
    account_info = mt5.account_info()
    if account_info is not None:
        logging.info(f"Account Details: Login: {account_info.login}, Balance: {account_info.balance} {account_info.currency}, Equity: {account_info.equity}, Margin: {account_info.margin}")
        return account_info
    else:
        logging.error(f"Failed to get account info, error code: {mt5.last_error()}")
        return None

def get_symbol_details(symbol_name):
    """Retrieves and logs details for a specific symbol."""
    if not mt5.terminal_info():
        logging.warning(f"Not connected to MT5. Cannot get details for symbol {symbol_name}.")
        return None

    symbol_info = mt5.symbol_info(symbol_name)
    if symbol_info is not None:
        logging.info(f"Symbol: {symbol_info.name}")
        logging.info(f"  Description: {symbol_info.description}")
        logging.info(f"  Digits: {symbol_info.digits}")
        logging.info(f"  Contract Size: {symbol_info.trade_contract_size}")
        logging.info(f"  Spread: {symbol_info.spread}")
        logging.info(f"  Min Volume: {symbol_info.volume_min}, Max Volume: {symbol_info.volume_max}, Step Volume: {symbol_info.volume_step}")
        
        # Ensure symbol is available in MarketWatch
        selected = mt5.symbol_select(symbol_name, True)
        if not selected:
            logging.warning(f"Failed to select {symbol_name} in MarketWatch, error: {mt5.last_error()}. Tick data might be unavailable.")
        
        tick = mt5.symbol_info_tick(symbol_name)
        if tick:
            logging.info(f"  Bid: {tick.bid}, Ask: {tick.ask}, Last: {tick.last}")
            logging.info(f"  Time: {datetime.fromtimestamp(tick.time)}")
        else:
            logging.warning(f"Could not retrieve tick for {symbol_name}, error: {mt5.last_error()}")
        return symbol_info
    else:
        logging.error(f"Failed to get info for symbol {symbol_name}, error code: {mt5.last_error()}")
        return None

def fetch_latest_candles(symbol_name, timeframe_mt5, num_candles=10):
    """
    Fetches the latest N candles for a given symbol and timeframe.
    :param symbol_name: e.g., "XAGUSD"
    :param timeframe_mt5: e.g., mt5.TIMEFRAME_M15
    :param num_candles: Number of candles to fetch
    :return: Pandas DataFrame of candles or None if failed
    """
    if not mt5.terminal_info():
        logging.warning(f"Not connected to MT5. Cannot fetch candles for {symbol_name}.")
        return None
    
    try:
        # Ensure symbol is available in MarketWatch
        if not mt5.symbol_select(symbol_name, True):
            logging.warning(f"Failed to select {symbol_name} in MarketWatch for fetching candles, error: {mt5.last_error()}. Candles might be unavailable.")
            # We can still attempt to fetch, sometimes it works if symbol was already selected.
            
        rates = mt5.copy_rates_from_pos(symbol_name, timeframe_mt5, 0, num_candles)
        if rates is None or len(rates) == 0:
            logging.error(f"Could not fetch candles for {symbol_name} on timeframe {timeframe_mt5}. Error: {mt5.last_error()}, Rates count: {len(rates) if rates is not None else 'None'}")
            return None
        
        # Convert to pandas DataFrame
        rates_df = pd.DataFrame(rates)
        # Convert timestamp to datetime objects
        rates_df['time'] = pd.to_datetime(rates_df['time'], unit='s')
        logging.info(f"Successfully fetched {len(rates_df)} candles for {symbol_name} on timeframe {timeframe_mt5}.")
        return rates_df

    except Exception as e:
        logging.error(f"Exception while fetching candles for {symbol_name}: {e}")
        return None

def place_market_trade(symbol_name, trade_type_mt5, volume, stop_loss_abs_price=None, take_profit_abs_price=None, magic_number=0, comment=""):
    """
    Places a market order.
    :param symbol_name: e.g., "XAGUSD"
    :param trade_type_mt5: mt5.ORDER_TYPE_BUY or mt5.ORDER_TYPE_SELL
    :param volume: Trade volume in lots
    :param stop_loss_abs_price: Absolute price for SL (optional)
    :param take_profit_abs_price: Absolute price for TP (optional)
    :param magic_number: Magic number for the order
    :param comment: Order comment
    :return: Result of mt5.order_send() or None if failed
    """
    if not mt5.terminal_info():
        logging.warning(f"Not connected to MT5. Cannot place trade for {symbol_name}.")
        return None

    symbol_info = mt5.symbol_info(symbol_name)
    if not symbol_info:
        logging.error(f"Failed to get info for symbol {symbol_name} before trading. Error: {mt5.last_error()}")
        return None

    if trade_type_mt5 == mt5.ORDER_TYPE_BUY:
        price = mt5.symbol_info_tick(symbol_name).ask
    elif trade_type_mt5 == mt5.ORDER_TYPE_SELL:
        price = mt5.symbol_info_tick(symbol_name).bid
    else:
        logging.error(f"Invalid trade type: {trade_type_mt5}")
        return None

    if price is None or price == 0: # Price might be 0 if market is closed or symbol not updating
        logging.error(f"Could not get valid market price for {symbol_name} (Ask/Bid is {price}). Cannot place trade.")
        return None

    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol_name,
        "volume": float(volume),
        "type": trade_type_mt5,
        "price": price,
        "deviation": 20,  # Allowable deviation from the price in points
        "magic": int(magic_number),
        "comment": comment,
        "type_time": mt5.ORDER_TIME_GTC, # Good till cancelled
        "type_filling": mt5.ORDER_FILLING_IOC, # Immediate Or Cancel (adjust if broker needs FOK or other)
    }

    if stop_loss_abs_price is not None and stop_loss_abs_price > 0:
        request["sl"] = float(stop_loss_abs_price)
    if take_profit_abs_price is not None and take_profit_abs_price > 0:
        request["tp"] = float(take_profit_abs_price)
    
    logging.info(f"Attempting to place trade: {request}")
    order_result = mt5.order_send(request)

    if order_result is None:
        logging.error(f"order_send failed, error code: {mt5.last_error()}")
        return None
    
    logging.info(f"Order send result: Code={order_result.retcode}, Deal={order_result.deal}, Order={order_result.order}, Comment={order_result.comment}, Request ID={order_result.request_id}")

    if order_result.retcode != mt5.TRADE_RETCODE_DONE and order_result.retcode != mt5.TRADE_RETCODE_PLACED:
        logging.error(f"Trade placement failed: {order_result.comment} (Retcode: {order_result.retcode})")
        # You might want to log more details from order_result.request here if available
        return order_result # Return the result object for further inspection

    logging.info(f"Trade placed successfully: Order Ticket {order_result.order}, Deal ID {order_result.deal}")
    return order_result

def place_limit_order(symbol_name, trade_type_mt5, volume, limit_price, stop_loss_abs_price=None, take_profit_abs_price=None, magic_number=0, comment=""):
    """
    Places a limit order (e.g., BUY_LIMIT, SELL_LIMIT).
    :param symbol_name: e.g., "XAGUSD"
    :param trade_type_mt5: mt5.ORDER_TYPE_BUY_LIMIT or mt5.ORDER_TYPE_SELL_LIMIT
    :param volume: Trade volume in lots
    :param limit_price: The price at which to set the limit order
    :param stop_loss_abs_price: Absolute price for SL (optional)
    :param take_profit_abs_price: Absolute price for TP (optional)
    :param magic_number: Magic number for the order
    :param comment: Order comment
    :return: Result of mt5.order_send() or None if failed
    """
    if not mt5.terminal_info():
        logging.warning(f"Not connected to MT5. Cannot place limit order for {symbol_name}.")
        return None

    symbol_info = mt5.symbol_info(symbol_name)
    if not symbol_info:
        logging.error(f"Failed to get info for symbol {symbol_name} before placing limit order. Error: {mt5.last_error()}")
        return None

    if trade_type_mt5 not in [mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT, mt5.ORDER_TYPE_BUY_STOP, mt5.ORDER_TYPE_SELL_STOP, mt5.ORDER_TYPE_BUY_STOP_LIMIT, mt5.ORDER_TYPE_SELL_STOP_LIMIT]:
        logging.error(f"Invalid trade type for limit/pending order: {trade_type_mt5}")
        return None

    request = {
        "action": mt5.TRADE_ACTION_PENDING, # Action for pending orders
        "symbol": symbol_name,
        "volume": float(volume),
        "type": trade_type_mt5,
        "price": float(limit_price), # Price for the limit order
        "deviation": 0,  # Deviation is not used for pending orders
        "magic": int(magic_number),
        "comment": comment,
        "type_time": mt5.ORDER_TIME_GTC, # Good till cancelled
        "type_filling": mt5.ORDER_FILLING_IOC, # Or FOK, depending on broker/preference
    }

    if stop_loss_abs_price is not None and stop_loss_abs_price > 0:
        request["sl"] = float(stop_loss_abs_price)
    if take_profit_abs_price is not None and take_profit_abs_price > 0:
        request["tp"] = float(take_profit_abs_price)
    
    logging.info(f"Attempting to place limit order: {request}")
    order_result = mt5.order_send(request)

    if order_result is None:
        logging.error(f"order_send for limit order failed, error code: {mt5.last_error()}")
        return None
    
    logging.info(f"Limit order send result: Code={order_result.retcode}, Order={order_result.order}, Comment={order_result.comment}, Request ID={order_result.request_id}")

    # For pending orders, TRADE_RETCODE_PLACED indicates success.
    if order_result.retcode != mt5.TRADE_RETCODE_PLACED:
        logging.error(f"Limit order placement failed: {order_result.comment} (Retcode: {order_result.retcode})")
        return order_result # Return the result object for further inspection

    logging.info(f"Limit order placed successfully: Order Ticket {order_result.order}")
    return order_result

def get_open_trades(symbol_name=None, magic_number_filter=None):
    """
    Retrieves a list of currently open positions.
    :param symbol_name: Filter by symbol (optional)
    :param magic_number_filter: Filter by magic number (optional)
    :return: List of position objects (namedtuples) or None if failed
    """
    if not mt5.terminal_info():
        logging.warning("Not connected to MT5. Cannot get open trades.")
        return None
    
    positions = None
    if symbol_name:
        positions = mt5.positions_get(symbol=symbol_name)
    else:
        positions = mt5.positions_get()

    if positions is None:
        logging.error(f"Failed to get positions. Error code: {mt5.last_error()}")
        return None
    
    if magic_number_filter is not None:
        positions = [pos for pos in positions if pos.magic == magic_number_filter]
        
    logging.info(f"Found {len(positions)} open positions matching criteria (Symbol: {symbol_name}, Magic: {magic_number_filter}).")
    return positions

def close_trade_by_ticket(ticket_id, volume_to_close=None, comment="Closed by bot"):
    """
    Closes a trade by its ticket ID.
    :param ticket_id: The ticket ID of the position to close.
    :param volume_to_close: Volume to close. If None, closes the entire position.
    :param comment: Comment for the closing order.
    :return: Result of mt5.order_send() or None if failed.
    """
    if not mt5.terminal_info():
        logging.warning("Not connected to MT5. Cannot close trade.")
        return None

    position_info = mt5.positions_get(ticket=ticket_id)
    if not position_info or len(position_info) == 0:
        logging.error(f"No position found with ticket ID {ticket_id}.")
        return None
    
    position = position_info[0] # positions_get returns a tuple of positions
    
    close_volume = position.volume if volume_to_close is None else float(volume_to_close)
    if close_volume > position.volume:
        logging.warning(f"Requested close volume {close_volume} for ticket {ticket_id} exceeds position volume {position.volume}. Closing with position volume.")
        close_volume = position.volume

    # Determine price and order type for closing
    if position.type == mt5.ORDER_TYPE_BUY: # Closing a BUY means placing a SELL
        price = mt5.symbol_info_tick(position.symbol).bid
        close_order_type = mt5.ORDER_TYPE_SELL
    elif position.type == mt5.ORDER_TYPE_SELL: # Closing a SELL means placing a BUY
        price = mt5.symbol_info_tick(position.symbol).ask
        close_order_type = mt5.ORDER_TYPE_BUY
    else:
        logging.error(f"Unknown position type {position.type} for ticket {ticket_id}.")
        return None

    if price is None or price == 0:
        logging.error(f"Could not get valid market price for {position.symbol} (Ask/Bid is {price}). Cannot close trade.")
        return None

    close_request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": position.symbol,
        "volume": close_volume,
        "type": close_order_type,
        "position": ticket_id, # Specify the position ticket to close
        "price": price,
        "deviation": 20,
        "magic": position.magic, # Use the magic number of the original order
        "comment": comment,
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }
    
    logging.info(f"Attempting to close trade: {close_request}")
    order_result = mt5.order_send(close_request)

    if order_result is None:
        logging.error(f"order_send for close failed, error code: {mt5.last_error()}")
        return None

    logging.info(f"Close order send result: Code={order_result.retcode}, Deal={order_result.deal}, Order={order_result.order}, Comment={order_result.comment}")
    if order_result.retcode != mt5.TRADE_RETCODE_DONE and order_result.retcode != mt5.TRADE_RETCODE_PLACED:
        logging.error(f"Trade close failed: {order_result.comment} (Retcode: {order_result.retcode})")
    else:
        logging.info(f"Trade {ticket_id} closed successfully (or close order placed).")
    return order_result

def modify_trade_sl_tp(ticket_id, new_stop_loss_abs_price=None, new_take_profit_abs_price=None):
    """
    Modifies the Stop Loss and/or Take Profit of an open position.
    :param ticket_id: The ticket ID of the position to modify.
    :param new_stop_loss_abs_price: New absolute SL price. 0 or None to not change/remove.
    :param new_take_profit_abs_price: New absolute TP price. 0 or None to not change/remove.
    :return: Result of mt5.order_send() or None if failed.
    """
    if not mt5.terminal_info():
        logging.warning("Not connected to MT5. Cannot modify trade.")
        return None

    position_info = mt5.positions_get(ticket=ticket_id)
    if not position_info or len(position_info) == 0:
        logging.error(f"No position found with ticket ID {ticket_id} for modification.")
        return None
    
    position = position_info[0]

    # Prepare the request for TRADE_ACTION_SLTP
    modify_request = {
        "action": mt5.TRADE_ACTION_SLTP,
        "symbol": position.symbol,
        "position": ticket_id,
    }
    
    changed = False
    if new_stop_loss_abs_price is not None: # Allow 0 to remove SL
        modify_request["sl"] = float(new_stop_loss_abs_price)
        changed = True
    if new_take_profit_abs_price is not None: # Allow 0 to remove TP
        modify_request["tp"] = float(new_take_profit_abs_price)
        changed = True

    if not changed:
        logging.info(f"No SL or TP modification requested for ticket {ticket_id}.")
        return None # Or return a custom success-like object indicating no action

    logging.info(f"Attempting to modify trade SL/TP: {modify_request}")
    order_result = mt5.order_send(modify_request)

    if order_result is None:
        logging.error(f"order_send for SL/TP modification failed, error code: {mt5.last_error()}")
        return None
    
    logging.info(f"SL/TP modification send result: Code={order_result.retcode}, Comment={order_result.comment}")
    if order_result.retcode != mt5.TRADE_RETCODE_DONE:
        logging.error(f"SL/TP modification failed: {order_result.comment} (Retcode: {order_result.retcode})")
    else:
        logging.info(f"SL/TP for trade {ticket_id} modified successfully.")
    return order_result


if __name__ == '__main__':
    # Ensure pandas is imported for the test block if not globally
    import pandas as pd 
    import time # For sleep
    logging.info("--- Testing MT5 Integration Module ---")
    
    try:
        config_data = load_config()
        trading_symbol = config_data['Trading_Parameters'].get('symbol', "XAGUSD")
        test_lot_size = float(config_data['Trading_Parameters'].get('lot_size', 0.01))
        test_magic_number = int(config_data['Trading_Parameters'].get('magic_number', 123456))
    except Exception as e:
        logging.error(f"Could not load configuration for test: {e}")
        # Fallback defaults if config loading fails for test section
        trading_symbol = "XAGUSD"
        test_lot_size = 0.01
        test_magic_number = 123456

    if connect_to_mt5():
        logging.info("Connection successful.")
        get_account_details()
        symbol_details = get_symbol_details(trading_symbol)
        
        if symbol_details:
            logging.info(f"\nAttempting to fetch candles for {trading_symbol}...")
            candles_df = fetch_latest_candles(trading_symbol, mt5.TIMEFRAME_M15, 5)
            if candles_df is not None and not candles_df.empty:
                logging.info(f"Last {len(candles_df)} M15 candles for {trading_symbol}:\n{candles_df}")
            else:
                logging.warning(f"Could not fetch M15 candles for {trading_symbol}.")

            # --- Test Trade Execution (COMMENTED OUT by default to prevent accidental trades) ---
            # To test trade execution:
            # 1. Ensure you are on a DEMO account.
            # 2. Uncomment the block below.
            # 3. Run this script directly.
            #
            # logging.info(f"\n--- Initiating Test Trade for {trading_symbol} ---")
            # current_ask_price = mt5.symbol_info_tick(trading_symbol).ask
            # if current_ask_price and current_ask_price > 0:
            #     # Place a BUY order
            #     tp_offset_test = 0.100 # Small TP for testing, e.g., $0.10
            #     tp_price_test = round(current_ask_price + tp_offset_test, symbol_details.digits if symbol_details else 3)
            #     
            #     logging.info(f"Attempting to place TEST BUY order: Symbol={trading_symbol}, Vol={test_lot_size}, Price={current_ask_price}, TP={tp_price_test} (No SL)")
            #     buy_order_result = place_market_trade(
            #         trading_symbol, 
            #         mt5.ORDER_TYPE_BUY, 
            #         test_lot_size,
            #         take_profit_abs_price=tp_price_test,
            #         stop_loss_abs_price=None, # No SL as per user request for main strategy
            #         magic_number=test_magic_number,
            #         comment="Test Open/Close"
            #     )
            #
            #     if buy_order_result and buy_order_result.retcode == mt5.TRADE_RETCODE_DONE:
            #         order_ticket = buy_order_result.order
            #         logging.info(f"Test BUY order placed successfully. Ticket: {order_ticket}. Waiting 10 seconds before attempting to close...")
            #         time.sleep(10) # Wait a bit
            #
            #         logging.info(f"Attempting to close test trade with ticket: {order_ticket}")
            #         close_result = close_trade_by_ticket(order_ticket)
            #         if close_result and close_result.retcode == mt5.TRADE_RETCODE_DONE:
            #             logging.info(f"Test trade {order_ticket} closed successfully.")
            #         else:
            #             logging.error(f"Failed to close test trade {order_ticket}. Result: {close_result}")
            #     else:
            #         logging.error(f"Failed to place test BUY order. Result: {buy_order_result}")
            # else:
            #     logging.warning(f"Could not get valid ask price for {trading_symbol}. Skipping test trade.")
            # logging.info("--- End of Test Trade ---")

        else:
            logging.warning(f"Skipping candle fetch and test trade because symbol details for {trading_symbol} could not be retrieved.")

        disconnect_from_mt5()
        logging.info("Disconnection complete.")
    else:
        logging.error("Connection failed. Please check mt5_config.ini and MT5 terminal status.")
    
    logging.info("--- End of MT5 Integration Module Test ---")
