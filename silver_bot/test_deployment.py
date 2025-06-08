import MetaTrader5 as mt5
import logging
import time
import os
from datetime import datetime
# Adjust the import path if mt5_integration_module is in the same directory or a different one
# Assuming mt5_integration_module is in the same directory as this script (silver_bot)
try:
    import mt5_integration_module as mt5_module
except ImportError:
    # If running from a different CWD, this might be needed.
    # For simplicity, this script assumes it's run from a context where 'silver_bot' is discoverable
    # or that mt5_integration_module.py is in the same directory.
    # If silver_bot is a package and this script is inside it:
    from . import mt5_integration_module as mt5_module


# Configure logging
log_file_path = os.path.join(os.path.dirname(__file__), 'test_deployment.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()
    ]
)

# --- Trade Parameters ---
SYMBOL = "XAGUSD"
LONG_LOT_SIZE = 0.01
LONG_TP_OFFSET = 0.07  # Take profit for long trade in price units (e.g., dollars)
SUBSEQUENT_LONG_ENTRY_DOLLAR_STEP = 0.05 # Step for subsequent long entries

# Updated Short Order Parameters
NEW_SHORT_ENTRY_OFFSET = 0.10       # Entry for short limit order, above long fill price
NEW_SHORT_TP_OFFSET = 0.10          # Take profit for short trade, below its entry price

MAGIC_NUMBER = 77777  # Unique magic number for these test trades
ORDER_COMMENT_LONG = "Test Long Market"
ORDER_COMMENT_SHORT_LIMIT = "Test Paired Short Limit"
MONITOR_INTERVAL_SECONDS = 10 # How often to check trade status

# Time and P&L based closure rule parameters
MAX_TRADE_DURATION_SECONDS = 600  # Updated: 10 minutes
PNL_THRESHOLD_FOR_TIMED_CLOSURE = -1.0 # Updated: P&L in account currency

def main():
    """
    Main function to execute the test trading scenario and monitor trades.
    """
    logging.info(f"--- Starting Test Deployment Script for {SYMBOL} ---")
    
    if not mt5_module.connect_to_mt5():
        logging.error("Failed to connect to MetaTrader 5. Exiting script.")
        return

    # --- State Tracking Variables ---
    # Stores the fill price of the most recently placed long trade by this script.
    last_placed_long_entry_price = None 
    # Stores details of open long positions: {ticket_id: {'entry_price': x, 'tp_price': y, 'comment': '...', 'open_time': server_timestamp}}
    active_long_positions = {} 
    # Stores details of pending short limit orders: {order_ticket_id: {'parent_long_ticket': lt, 'limit_price': p, 'tp_price': tp, 'comment': '...'}}
    active_short_limit_orders = {}
    # Stores details of open short positions: {position_ticket_id: {'entry_price': x, 'tp_price': y, 'comment': '...', 'open_time': server_timestamp}}
    active_short_positions = {}
    
    long_trade_counter = 0 # To give unique comments to subsequent trades

    try:
        logging.info(f"Fetching symbol details for {SYMBOL}...")
        symbol_info = mt5.symbol_info(SYMBOL)
        if not symbol_info:
            logging.error(f"Failed to get symbol_info for {SYMBOL}. Error: {mt5.last_error()}")
            return
        logging.info(f"Symbol {SYMBOL} details retrieved. Digits: {symbol_info.digits}")

        if not mt5.symbol_select(SYMBOL, True):
            logging.warning(f"Could not select {SYMBOL} in MarketWatch. Tick data might be stale. Error: {mt5.last_error()}")
        
        # --- Initial Long Trade Attempt ---
        # This section will only run if no long trade has been established by this script yet.
        if last_placed_long_entry_price is None:
            logging.info("Attempting to place INITIAL long trade...")
            tick = mt5.symbol_info_tick(SYMBOL)
            if not tick or tick.ask == 0:
                logging.error(f"Could not get valid tick or ask price for {SYMBOL} for initial trade. Ask: {tick.ask if tick else 'N/A'}.")
            else:
                current_ask_price = tick.ask
                long_tp_price = round(current_ask_price + LONG_TP_OFFSET, symbol_info.digits)
                long_trade_counter += 1
                long_comment = f"{ORDER_COMMENT_LONG} #{long_trade_counter}"
                
                logging.info(f"Placing INITIAL LONG market order for {SYMBOL} at ~{current_ask_price}, Lot: {LONG_LOT_SIZE}, TP: {long_tp_price}, Comment: {long_comment}")
                long_order_result = mt5_module.place_market_trade(SYMBOL, mt5.ORDER_TYPE_BUY, LONG_LOT_SIZE, take_profit_abs_price=long_tp_price, magic_number=MAGIC_NUMBER, comment=long_comment)

                if long_order_result and long_order_result.retcode == mt5.TRADE_RETCODE_DONE:
                    initial_long_ticket = long_order_result.order
                    logging.info(f"INITIAL LONG market order placed successfully. Ticket: {initial_long_ticket}, Deal ID: {long_order_result.deal}")
                    
                    time.sleep(2) # Wait for position to register
                    positions = mt5.positions_get(ticket=initial_long_ticket)
                    if positions and len(positions) > 0:
                        pos_data = positions[0]
                        last_placed_long_entry_price = pos_data.price_open
                        active_long_positions[initial_long_ticket] = {
                            'entry_price': pos_data.price_open, 
                            'tp_price': pos_data.tp, 
                            'comment': long_comment,
                            'open_time': pos_data.time # Store MT5 server open time
                        }
                        logging.info(f"Actual fill price for initial long order {initial_long_ticket}: {last_placed_long_entry_price}, Open Time: {datetime.fromtimestamp(pos_data.time)}")

                        # Place paired short limit for this initial long (using new offsets)
                        short_limit_entry_price = round(last_placed_long_entry_price + NEW_SHORT_ENTRY_OFFSET, symbol_info.digits)
                        short_limit_tp_price = round(short_limit_entry_price - NEW_SHORT_TP_OFFSET, symbol_info.digits)
                        short_comment = f"PairSL LT#{long_trade_counter}" # Shortened comment
                        logging.info(f"Placing PAIRED SHORT LIMIT for initial long: Entry: {short_limit_entry_price}, TP: {short_limit_tp_price}, Comment: {short_comment}")
                        
                        short_order_result = mt5_module.place_limit_order(SYMBOL, mt5.ORDER_TYPE_SELL_LIMIT, LONG_LOT_SIZE, short_limit_entry_price, take_profit_abs_price=short_limit_tp_price, magic_number=MAGIC_NUMBER, comment=short_comment)
                        if short_order_result and short_order_result.retcode == mt5.TRADE_RETCODE_PLACED:
                            active_short_limit_orders[short_order_result.order] = {
                                'parent_long_ticket': initial_long_ticket, 
                                'limit_price': short_limit_entry_price, 
                                'tp_price': short_limit_tp_price, 
                                'comment': short_comment
                            }
                            logging.info(f"PAIRED SHORT LIMIT for initial long placed. Ticket: {short_order_result.order}")
                        else:
                            logging.error(f"Failed to place PAIRED SHORT LIMIT for initial long. Retcode: {short_order_result.retcode if short_order_result else 'N/A'}")
                    else:
                        logging.error(f"Could not retrieve position details for initial long order {initial_long_ticket}.")
                else:
                    logging.error(f"Failed to place INITIAL LONG market order. Retcode: {long_order_result.retcode if long_order_result else 'N/A'}")
        
        # --- Main Monitoring and Subsequent Trading Loop ---
        if not active_long_positions and not active_short_limit_orders and last_placed_long_entry_price is None:
             logging.info("Initial trade placement failed and no active trades/orders to monitor. Exiting.")
        else:
            logging.info(f"--- Starting Trade Monitoring & Subsequent Trading Loop (Ctrl+C to stop, updates every {MONITOR_INTERVAL_SECONDS}s) ---")
            while True: # Loop continues until explicitly broken or script interrupted
                current_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                logging.info(f"--- Monitoring Update ({current_time_str}) ---")
                
                # --- 1. Monitor Active Long Positions ---
                for ticket in list(active_long_positions.keys()): # Iterate over a copy of keys
                    position_info_tuple = mt5.positions_get(ticket=ticket)
                    
                    if position_info_tuple is None:
                        logging.warning(f"  Failed to get status for long position {ticket}. Error: {mt5.last_error()}. Will retry.")
                        continue # Keep it in active_long_positions and retry next cycle

                    if len(position_info_tuple) > 0:
                        pos = position_info_tuple[0]
                        logging.info(f"  Active Long (Ticket: {pos.ticket}, Comment: {active_long_positions[ticket].get('comment', 'N/A')}): Vol={pos.volume}, OpenP={pos.price_open:.{symbol_info.digits}f}, TP={pos.tp:.{symbol_info.digits}f}, CurrP={pos.price_current:.{symbol_info.digits}f}, P/L={pos.profit:.2f}")
                    else:
                        # Position is no longer active (mt5.positions_get() returned empty tuple)
                        logging.info(f"  Long position (Ticket: {ticket}, Comment: {active_long_positions[ticket].get('comment', 'N/A')}) seems closed.")
                        deals = mt5.history_deals_get(position=ticket)
                        if deals:
                            for deal_obj in sorted(deals, key=lambda d: d.time_msc, reverse=True): # Log most recent deals first
                                if deal_obj.entry == mt5.DEAL_ENTRY_OUT or deal_obj.entry == mt5.DEAL_ENTRY_INOUT: # Closed by TP/SL or manually
                                    logging.info(f"    Closure Deal for Long {ticket}: DealID={deal_obj.ticket}, Price={deal_obj.price:.{symbol_info.digits}f}, Profit={deal_obj.profit:.2f}, Comment={deal_obj.comment}")
                                    break # Log only the primary closing deal
                        del active_long_positions[ticket]

                # --- 2. Monitor Active Short Limit Orders ---
                for ticket in list(active_short_limit_orders.keys()):
                    order_info_tuple = mt5.orders_get(ticket=ticket)
                    order_resolved = False

                    if order_info_tuple is None:
                        logging.warning(f"  Failed to get status for short limit order {ticket}. Error: {mt5.last_error()}. Will retry.")
                        continue # Keep it in active_short_limit_orders and retry next cycle

                    if len(order_info_tuple) > 0: # Order is actively pending
                        order = order_info_tuple[0]
                        logging.info(f"  Pending Short Limit (Ticket: {order.ticket}, Comment: {active_short_limit_orders[ticket].get('comment', 'N/A')}): State={order.state}, EntryP={order.price_open:.{symbol_info.digits}f}, TP={order.tp:.{symbol_info.digits}f}")
                        
                        if order.state == mt5.ORDER_STATE_FILLED:
                            logging.info(f"  Short limit order {ticket} state is FILLED. Verifying with history deals...")
                            # Rely on history_deals check below to process the fill and remove.
                            # This state means it's likely to disappear from orders_get soon.
                            # We don't set order_resolved = True here yet, let deal history confirm.
                        elif order.state in [mt5.ORDER_STATE_CANCELLED, mt5.ORDER_STATE_REJECTED, mt5.ORDER_STATE_EXPIRED]:
                            logging.info(f"  Short limit order {ticket} has terminal state {order.state} in active orders. Removing.")
                            order_resolved = True
                        # Else, it's still genuinely pending (e.g., ORDER_STATE_PLACED), do nothing here.
                    
                    # If order was not found by orders_get OR its state was FILLED (needs deal confirmation)
                    if len(order_info_tuple) == 0 or (len(order_info_tuple) > 0 and order_info_tuple[0].state == mt5.ORDER_STATE_FILLED):
                        if len(order_info_tuple) == 0: # Explicitly log if it wasn't found by orders_get
                            logging.info(f"  Short limit order (Ticket: {ticket}, Comment: {active_short_limit_orders[ticket].get('comment', 'N/A')}) not found in active pending orders. Checking history...")

                        deals = mt5.history_deals_get(order=ticket)
                        filled_deal_found = False
                        if deals:
                            for deal_obj in deals:
                                if deal_obj.entry == mt5.DEAL_ENTRY_IN and deal_obj.type == mt5.DEAL_TYPE_SELL: # Filled
                                    logging.info(f"    Short limit {ticket} confirmed FILLED via history_deals! DealID={deal_obj.ticket}, PosID={deal_obj.position_id}, Price={deal_obj.price:.{symbol_info.digits}f}")
                                    short_pos_details_from_limit = active_short_limit_orders[ticket]
                                    filled_short_position = mt5.positions_get(ticket=deal_obj.position_id)
                                    if filled_short_position and len(filled_short_position) > 0:
                                        actual_filled_short_pos = filled_short_position[0]
                                        active_short_positions[deal_obj.position_id] = {
                                            'entry_price': actual_filled_short_pos.price_open, 
                                            'tp_price': actual_filled_short_pos.tp,
                                            'comment': short_pos_details_from_limit['comment'].replace("Limit for", "Position for"),
                                            'open_time': actual_filled_short_pos.time
                                        }
                                        logging.info(f"    Short position {deal_obj.position_id} details: Entry={actual_filled_short_pos.price_open}, TP={actual_filled_short_pos.tp}, OpenTime={datetime.fromtimestamp(actual_filled_short_pos.time)}")
                                    else:
                                        logging.warning(f"    Could not immediately get position details for filled short {deal_obj.position_id}. Using deal price and limit TP.")
                                        active_short_positions[deal_obj.position_id] = {
                                            'entry_price': deal_obj.price, 
                                            'tp_price': short_pos_details_from_limit['tp_price'], 
                                            'comment': short_pos_details_from_limit['comment'].replace("Limit for", "Position for"),
                                            'open_time': deal_obj.time
                                        }
                                    filled_deal_found = True
                                    order_resolved = True
                                    break 
                        
                        if not filled_deal_found and not order_resolved : # If not filled, check history_orders for other terminal states
                            history_order_tuple = mt5.history_orders_get(ticket=ticket)
                            if history_order_tuple and len(history_order_tuple) > 0:
                                history_order = history_order_tuple[0]
                                if history_order.state in [mt5.ORDER_STATE_CANCELLED, mt5.ORDER_STATE_REJECTED, mt5.ORDER_STATE_EXPIRED]:
                                    logging.info(f"    Short limit order {ticket} found in history_orders with terminal state {history_order.state}. Removing.")
                                    order_resolved = True
                                else: # Found in history_orders but not in a final state (should be rare if not in orders_get)
                                    logging.warning(f"    Short limit order {ticket} in history_orders with state {history_order.state}, but not in active orders and not filled. Will re-check.")
                            else: # Not in active orders, not filled, not in history_orders
                                logging.warning(f"    Short limit order {ticket} not in active orders, not filled, and not found in history_orders. Will re-check.")
                    
                    if order_resolved:
                        del active_short_limit_orders[ticket]

                # --- 3. Monitor Active Short Positions ---
                for ticket in list(active_short_positions.keys()):
                    position_info_tuple = mt5.positions_get(ticket=ticket)

                    if position_info_tuple is None:
                        logging.warning(f"  Failed to get status for short position {ticket}. Error: {mt5.last_error()}. Will retry.")
                        continue # Keep it in active_short_positions and retry next cycle
                        
                    if len(position_info_tuple) > 0:
                        pos = position_info_tuple[0]
                        logging.info(f"  Active Short (Ticket: {pos.ticket}, Comment: {active_short_positions[ticket].get('comment', 'N/A')}): Vol={pos.volume}, OpenP={pos.price_open:.{symbol_info.digits}f}, TP={pos.tp:.{symbol_info.digits}f}, CurrP={pos.price_current:.{symbol_info.digits}f}, P/L={pos.profit:.2f}")
                    else:
                        # Position is no longer active (mt5.positions_get() returned empty tuple)
                        logging.info(f"  Short position (Ticket: {ticket}, Comment: {active_short_positions[ticket].get('comment', 'N/A')}) seems closed.")
                        deals = mt5.history_deals_get(position=ticket)
                        if deals:
                             for deal_obj in sorted(deals, key=lambda d: d.time_msc, reverse=True):
                                if deal_obj.entry == mt5.DEAL_ENTRY_OUT or deal_obj.entry == mt5.DEAL_ENTRY_INOUT:
                                    logging.info(f"    Closure Deal for Short {ticket}: DealID={deal_obj.ticket}, Price={deal_obj.price:.{symbol_info.digits}f}, Profit={deal_obj.profit:.2f}, Comment={deal_obj.comment}")
                                    break
                        del active_short_positions[ticket]

                # --- 4. Check for and Place Subsequent Long Trade ---
                if last_placed_long_entry_price is not None:
                    current_tick = mt5.symbol_info_tick(SYMBOL)
                    if current_tick and current_tick.ask > 0:
                        current_market_ask = current_tick.ask
                        target_new_long_price = last_placed_long_entry_price - SUBSEQUENT_LONG_ENTRY_DOLLAR_STEP
                        
                        logging.debug(f"Subsequent long check: Current Ask={current_market_ask:.{symbol_info.digits}f}, Last Long Entry={last_placed_long_entry_price:.{symbol_info.digits}f}, Target New Long Price={target_new_long_price:.{symbol_info.digits}f}")

                        if current_market_ask <= target_new_long_price:
                            logging.info(f"Condition met for SUBSEQUENT long trade: Current Ask ({current_market_ask:.{symbol_info.digits}f}) <= Target ({target_new_long_price:.{symbol_info.digits}f})")
                            
                            long_trade_counter += 1
                            new_long_comment = f"{ORDER_COMMENT_LONG} #{long_trade_counter}"
                            new_long_tp_price = round(current_market_ask + LONG_TP_OFFSET, symbol_info.digits) # TP based on current ask, will be refined by fill
                            
                            logging.info(f"Placing SUBSEQUENT LONG market order for {SYMBOL} at ~{current_market_ask}, Lot: {LONG_LOT_SIZE}, TP: {new_long_tp_price}, Comment: {new_long_comment}")
                            new_long_order_result = mt5_module.place_market_trade(SYMBOL, mt5.ORDER_TYPE_BUY, LONG_LOT_SIZE, take_profit_abs_price=new_long_tp_price, magic_number=MAGIC_NUMBER, comment=new_long_comment)

                            if new_long_order_result and new_long_order_result.retcode == mt5.TRADE_RETCODE_DONE:
                                new_long_ticket = new_long_order_result.order
                                logging.info(f"SUBSEQUENT LONG market order placed successfully. Ticket: {new_long_ticket}, Deal ID: {new_long_order_result.deal}")
                                
                                time.sleep(2) # Wait for position
                                new_positions = mt5.positions_get(ticket=new_long_ticket)
                                if new_positions and len(new_positions) > 0:
                                    new_pos_data = new_positions[0]
                                    last_placed_long_entry_price = new_pos_data.price_open # Update reference price
                                    actual_new_long_tp = new_pos_data.tp # Get actual TP if broker adjusted
                                    active_long_positions[new_long_ticket] = {
                                        'entry_price': new_pos_data.price_open, 
                                        'tp_price': actual_new_long_tp, 
                                        'comment': new_long_comment,
                                        'open_time': new_pos_data.time # Store MT5 server open time
                                    }
                                    logging.info(f"Actual fill price for subsequent long order {new_long_ticket}: {last_placed_long_entry_price}. Actual TP: {actual_new_long_tp}. Open Time: {datetime.fromtimestamp(new_pos_data.time)}")

                                    # Place paired short limit for this new subsequent long (using new offsets)
                                    new_short_limit_entry = round(last_placed_long_entry_price + NEW_SHORT_ENTRY_OFFSET, symbol_info.digits)
                                    new_short_limit_tp = round(new_short_limit_entry - NEW_SHORT_TP_OFFSET, symbol_info.digits)
                                    new_short_comment = f"PairSL LT#{long_trade_counter}" # Shortened comment
                                    logging.info(f"Placing PAIRED SHORT LIMIT for subsequent long: Entry: {new_short_limit_entry}, TP: {new_short_limit_tp}, Comment: {new_short_comment}")
                                    
                                    new_short_order_result = mt5_module.place_limit_order(SYMBOL, mt5.ORDER_TYPE_SELL_LIMIT, LONG_LOT_SIZE, new_short_limit_entry, take_profit_abs_price=new_short_limit_tp, magic_number=MAGIC_NUMBER, comment=new_short_comment)
                                    if new_short_order_result and new_short_order_result.retcode == mt5.TRADE_RETCODE_PLACED:
                                        active_short_limit_orders[new_short_order_result.order] = {
                                            'parent_long_ticket': new_long_ticket, 
                                            'limit_price': new_short_limit_entry, 
                                            'tp_price': new_short_limit_tp, 
                                            'comment': new_short_comment
                                        }
                                        logging.info(f"PAIRED SHORT LIMIT for subsequent long placed. Ticket: {new_short_order_result.order}")
                                    else:
                                        logging.error(f"Failed to place PAIRED SHORT LIMIT for subsequent long. Retcode: {new_short_order_result.retcode if new_short_order_result else 'N/A'}")
                                else:
                                    logging.error(f"Could not retrieve position details for subsequent long order {new_long_ticket}.")
                            else:
                                logging.error(f"Failed to place SUBSEQUENT LONG market order. Retcode: {new_long_order_result.retcode if new_long_order_result else 'N/A'}")
                        # else:
                        #    logging.debug("Condition for subsequent long not met.") # Too verbose for INFO
                    # else:
                    #    logging.warning("Could not get valid tick for subsequent long check.") # Too verbose for INFO
                
                # --- 5. Time-Based and P&L-Based Closure Rule ---
                current_server_time_for_duration_check = datetime.now().timestamp() # Using local time as approx for server time for duration calc
                
                # Check Long Positions for Timed Closure
                for ticket, details in list(active_long_positions.items()):
                    position_open_time = details.get('open_time')
                    if position_open_time:
                        duration_seconds = current_server_time_for_duration_check - position_open_time
                        if duration_seconds > MAX_TRADE_DURATION_SECONDS:
                            # Fetch fresh position data for P&L
                            pos_data_list = mt5.positions_get(ticket=ticket)
                            if pos_data_list and len(pos_data_list) > 0:
                                pos_data = pos_data_list[0]
                                current_net_pnl = pos_data.profit # Using net P&L
                                if current_net_pnl > PNL_THRESHOLD_FOR_TIMED_CLOSURE:
                                    logging.info(f"CLOSING Long Position (Ticket: {ticket}, Comment: {details.get('comment','N/A')}) due to: Duration > {MAX_TRADE_DURATION_SECONDS/60:.1f}min ({duration_seconds/60:.1f}min) AND Net P&L ({current_net_pnl:.2f}) > {PNL_THRESHOLD_FOR_TIMED_CLOSURE:.2f}")
                                    close_result = mt5_module.close_trade_by_ticket(ticket, comment="Timed Net P&L Closure")
                                    if close_result and close_result.retcode == mt5.TRADE_RETCODE_DONE:
                                        logging.info(f"  Successfully sent close order for long {ticket}.")
                                        # It will be removed from active_long_positions in the next monitoring cycle's section 1.
                                    else:
                                        logging.error(f"  Failed to send close order for long {ticket}. Retcode: {close_result.retcode if close_result else 'N/A'}")
                                # else:
                                #    logging.debug(f"Long {ticket} duration > {MAX_TRADE_DURATION_SECONDS/60:.1f}min but Net P&L ({current_net_pnl:.2f}) not > {PNL_THRESHOLD_FOR_TIMED_CLOSURE:.2f}")
                            # else:
                            #    logging.warning(f"Long {ticket} met duration but could not fetch fresh data for P&L check.")
                
                # Check Short Positions for Timed Closure
                for ticket, details in list(active_short_positions.items()):
                    position_open_time = details.get('open_time')
                    if position_open_time:
                        duration_seconds = current_server_time_for_duration_check - position_open_time
                        if duration_seconds > MAX_TRADE_DURATION_SECONDS:
                            pos_data_list = mt5.positions_get(ticket=ticket)
                            if pos_data_list and len(pos_data_list) > 0:
                                pos_data = pos_data_list[0]
                                current_net_pnl = pos_data.profit # Using net P&L
                                if current_net_pnl > PNL_THRESHOLD_FOR_TIMED_CLOSURE:
                                    logging.info(f"CLOSING Short Position (Ticket: {ticket}, Comment: {details.get('comment','N/A')}) due to: Duration > {MAX_TRADE_DURATION_SECONDS/60:.1f}min ({duration_seconds/60:.1f}min) AND Net P&L ({current_net_pnl:.2f}) > {PNL_THRESHOLD_FOR_TIMED_CLOSURE:.2f}")
                                    close_result = mt5_module.close_trade_by_ticket(ticket, comment="Timed Net P&L Closure")
                                    if close_result and close_result.retcode == mt5.TRADE_RETCODE_DONE:
                                        logging.info(f"  Successfully sent close order for short {ticket}.")
                                    else:
                                        logging.error(f"  Failed to send close order for short {ticket}. Retcode: {close_result.retcode if close_result else 'N/A'}")
                                # else:
                                #    logging.debug(f"Short {ticket} duration > {MAX_TRADE_DURATION_SECONDS/60:.1f}min but Net P&L ({current_net_pnl:.2f}) not > {PNL_THRESHOLD_FOR_TIMED_CLOSURE:.2f}")
                            # else:
                            #    logging.warning(f"Short {ticket} met duration but could not fetch fresh data for P&L check.")


                # --- 6. Loop Termination Condition ---
                if not active_long_positions and not active_short_limit_orders and not active_short_positions and last_placed_long_entry_price is not None:
                    logging.info("All trades and orders managed by this script are resolved. Exiting monitoring loop.")
                    break
                elif last_placed_long_entry_price is None and not active_long_positions and not active_short_limit_orders and not active_short_positions:
                    # This case handles if the very first trade attempt failed and nothing is active.
                    logging.info("Initial trade failed and no active trades/orders. Exiting.")
                    break

                time.sleep(MONITOR_INTERVAL_SECONDS)
            
    except KeyboardInterrupt:
        logging.info("Monitoring interrupted by user (Ctrl+C).")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)
    finally:
        logging.info("Disconnecting from MetaTrader 5...")
        mt5_module.disconnect_from_mt5()
        logging.info("--- Test Deployment Script Finished ---")

if __name__ == "__main__":
    main()
