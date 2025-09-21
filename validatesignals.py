import MetaTrader5 as mt5
import logging
from colorama import Fore, Style, init
import time
import json
import os
import difflib  # For closest matches in fallback

# Initialize colorama for colored console output
init()

# Configure Logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler()  # Console output only
    ]
)
logger = logging.getLogger(__name__)

# Suppress WebDriver-related logs (if any)
for name in ['webdriver_manager', 'selenium', 'urllib3', 'selenium.webdriver']:
    logging.getLogger(name).setLevel(logging.WARNING)

# Logging Helper Function
def log_and_print(message, level="INFO"):
    """Helper function to print formatted messages with color coding and spacing."""
    indent = "    "
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    level_colors = {
        "INFO": Fore.CYAN,
        "SUCCESS": Fore.GREEN,
        "WARNING": Fore.YELLOW,
        "ERROR": Fore.RED,
        "TITLE": Fore.MAGENTA,
        "DEBUG": Fore.LIGHTBLACK_EX
    }
    log_level = "INFO" if level in ["TITLE", "SUCCESS"] else level
    color = level_colors.get(level, Fore.WHITE)
    formatted_message = f"[ {timestamp} ] │ {level:7} │ {indent}{message}"
    print(f"{color}{formatted_message}{Style.RESET_ALL}")
    logger.log(getattr(logging, log_level), message)

# Configuration
LOGIN_ID = "101347351"
PASSWORD = "@Techknowdge12#"
SERVER = "DerivSVG-Server-02"
TERMINAL_PATH = r"C:\Program Files\MetaTrader 5\terminal64.exe"  # Update with your MT5 terminal path
BASE_OUTPUT_FOLDER = r"C:\xampp\htdocs\CIPHER\cipher trader\market\bouncestreamsignals.json"
MAX_RETRIES = 5
RETRY_DELAY = 3
TEST_ORDER_VOLUME = 0.01  # Volume for test orders

def initialize_mt5():
    """Initialize MT5 terminal and login."""
    log_and_print("Initializing MT5 terminal...", "INFO")
    
    # Ensure no existing MT5 connections interfere
    mt5.shutdown()

    # Initialize MT5 terminal with explicit path and timeout
    for attempt in range(MAX_RETRIES):
        if mt5.initialize(path=TERMINAL_PATH, timeout=60000):
            log_and_print("Successfully initialized MT5 terminal", "SUCCESS")
            break
        error_code, error_message = mt5.last_error()
        log_and_print(f"Attempt {attempt + 1}/{MAX_RETRIES}: Failed to initialize MT5 terminal. Error: {error_code}, {error_message}", "ERROR")
        time.sleep(RETRY_DELAY)
    else:
        log_and_print(f"Failed to initialize MT5 terminal after {MAX_RETRIES} attempts", "ERROR")
        return False

    # Wait for terminal to be fully ready
    for _ in range(5):
        if mt5.terminal_info() is not None:
            log_and_print("MT5 terminal fully initialized", "DEBUG")
            break
        log_and_print("Waiting for MT5 terminal to fully initialize...", "INFO")
        time.sleep(2)
    else:
        log_and_print("MT5 terminal not ready", "ERROR")
        mt5.shutdown()
        return False

    # Attempt login with retries
    for attempt in range(MAX_RETRIES):
        if mt5.login(login=int(LOGIN_ID), password=PASSWORD, server=SERVER, timeout=60000):
            log_and_print("Successfully logged in to MT5", "SUCCESS")
            return True
        error_code, error_message = mt5.last_error()
        log_and_print(f"Attempt {attempt + 1}/{MAX_RETRIES}: Failed to log in to MT5. Error code: {error_code}, Message: {error_message}", "ERROR")
        time.sleep(RETRY_DELAY)
    else:
        log_and_print(f"Failed to log in to MT5 after {MAX_RETRIES} attempts", "ERROR")
        mt5.shutdown()
        return False

def get_available_symbols():
    """Fetch and return all available symbols on the server."""
    try:
        symbols = mt5.symbols_get()
        if symbols:
            symbol_names = [s.name for s in symbols]
            log_and_print(f"Available symbols on server ({len(symbol_names)}): {', '.join(symbol_names)}", "DEBUG")
            return symbol_names
        else:
            log_and_print("No symbols retrieved from server", "ERROR")
            return []
    except Exception as e:
        log_and_print(f"Error fetching available symbols: {str(e)}", "ERROR")
        return []

def get_exact_symbol_match(json_symbol, available_symbols):
    """Find the exact server symbol matching the JSON symbol (case-insensitive)."""
    json_lower = json_symbol.lower()
    lower_available = [s.lower() for s in available_symbols]
    if json_lower in lower_available:
        # Get the exact case version
        index = lower_available.index(json_lower)
        exact = available_symbols[index]
        log_and_print(f"Matched '{json_symbol}' to exact server symbol: '{exact}'", "DEBUG")
        return exact
    else:
        # Fallback: Find closest matches for debugging
        close_matches = difflib.get_close_matches(json_symbol, available_symbols, n=3, cutoff=0.6)
        log_and_print(f"No exact match for '{json_symbol}'. Closest server symbols: {', '.join(close_matches) if close_matches else 'None'}", "WARNING")
        return None

def load_market_signals():
    try:
        if not os.path.exists(BASE_OUTPUT_FOLDER):
            log_and_print(f"JSON file not found at {BASE_OUTPUT_FOLDER}", "ERROR")
            return []
        with open(BASE_OUTPUT_FOLDER, 'r') as file:
            data = json.load(file)
        if not isinstance(data, dict) or 'orders' not in data:
            log_and_print("Invalid JSON structure: Expected a dictionary with 'orders' key", "ERROR")
            return []
        signals = data['orders']
        if not isinstance(signals, list):
            log_and_print("Invalid JSON structure: 'orders' must be a list", "ERROR")
            return []
        log_and_print(f"Loaded {len(signals)} signals from JSON 'orders' array, expected 387 per metadata", "INFO")
        return signals
    except json.JSONDecodeError as e:
        log_and_print(f"Error decoding JSON file: {str(e)}", "ERROR")
        return []
    except Exception as e:
        log_and_print(f"Error loading signals from JSON: {str(e)}", "ERROR")
        return []
      
def place_test_order(symbol):
    """Attempt to place and immediately cancel a test order to ensure symbol is in Market Watch."""
    try:
        # Ensure symbol is selected
        if not mt5.symbol_select(symbol, True):
            log_and_print(f"Failed to select {symbol} for test order, error: {mt5.last_error()}", "ERROR")
            return False

        # Get symbol information
        symbol_info = mt5.symbol_info(symbol)
        if symbol_info is None:
            log_and_print(f"Cannot retrieve info for {symbol}", "ERROR")
            return False

        # Check if symbol is tradeable
        if not symbol_info.trade_mode == mt5.SYMBOL_TRADE_MODE_FULL:
            log_and_print(f"Symbol {symbol} is not tradeable (trade mode: {symbol_info.trade_mode})", "ERROR")
            return False

        # Get current market price
        price = mt5.symbol_info_tick(symbol)
        if price is None:
            log_and_print(f"Cannot retrieve tick data for {symbol}, error: {mt5.last_error()}", "ERROR")
            return False

        # Prepare a test buy order
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": TEST_ORDER_VOLUME,
            "type": mt5.ORDER_TYPE_BUY,
            "price": price.ask,
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }

        # Send test order
        result = mt5.order_send(request)
        if result.retcode != mt5.TRADE_RETCODE_DONE:
            log_and_print(f"Test order for {symbol} failed, error: {result.retcode}, {mt5.last_error()}", "ERROR")
            return False

        # Order placed successfully, now cancel it
        log_and_print(f"Test order for {symbol} placed successfully (Order #{result.order})", "SUCCESS")

        # Prepare cancellation request
        cancel_request = {
            "action": mt5.TRADE_ACTION_REMOVE,
            "order": result.order
        }

        # Attempt to cancel the order
        cancel_result = mt5.order_send(cancel_request)
        if cancel_result.retcode != mt5.TRADE_RETCODE_DONE:
            log_and_print(f"Failed to cancel test order for {symbol} (Order #{result.order}), error: {cancel_result.retcode}, {mt5.last_error()}", "WARNING")
        else:
            log_and_print(f"Test order for {symbol} (Order #{result.order}) canceled successfully", "SUCCESS")

        return True

    except Exception as e:
        log_and_print(f"Error processing test order for {symbol}: {str(e)}", "ERROR")
        return False

def save_failed_orders(symbol, order_type, entry_price, profit_price, stop_loss, lot_size, allowed_risk, error_message, error_category="unknown", signal=None):
    """Save a single failed pending order to a categorized JSON file incrementally with timeframe-specific summary."""
    # Define output paths based on error category
    base_path = r"C:\xampp\htdocs\CIPHER\cipher trader\market\errors"
    output_paths = {
        "invalid_entry": os.path.join(base_path, "failedordersinvalidentry.json"),
        "stop_loss": os.path.join(base_path, "failedordersbystoploss.json"),
        "unknown": os.path.join(base_path, "failedpendingorders.json")
    }
    output_path = output_paths.get(error_category, output_paths["unknown"])

    # Define summary structure for all error categories
    summary_structure = {
        "total_failed_orders": 0,
        "5m_failed_orders": 0,
        "15m_failed_orders": 0,
        "30m_failed_orders": 0,
        "1h_failed_orders": 0,
        "4h_failed_orders": 0,
        "orders": []
    }

    # Extract timeframe from signal, default to "unknown" if not present
    timeframe = signal.get("timeframe", "unknown") if signal else "unknown"
    # Normalize timeframe to standard format
    timeframe_map = {
        "5m": "5m", "5 minutes": "5m", "5minutes": "5m", "M5": "5m",
        "15m": "15m", "15 minutes": "15m", "15minutes": "15m", "M15": "15m",
        "30m": "30m", "30 minutes": "30m", "30minutes": "30m", "M30": "30m",
        "1h": "1h", "1 hour": "1h", "1hour": "1h", "H1": "1h",
        "4h": "4h", "4 hours": "4h", "4hours": "4h", "H4": "4h"
    }
    normalized_timeframe = timeframe_map.get(timeframe.lower() if isinstance(timeframe, str) else timeframe, "unknown")

    # Create failed order entry
    failed_order = {
        "symbol": symbol,
        "order_type": order_type,
        "entry_price": entry_price,
        "profit_price": profit_price,
        "stop_loss": stop_loss,
        "lot_size": lot_size,
        "allowed_risk": allowed_risk,
        "error_message": error_message,
        "timeframe": normalized_timeframe,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }

    try:
        # Ensure the directory exists
        os.makedirs(base_path, exist_ok=True)
        
        # Load existing data or initialize with summary structure
        existing_data = summary_structure.copy()
        if os.path.exists(output_path):
            try:
                with open(output_path, 'r') as file:
                    existing_data = json.load(file)
                if not isinstance(existing_data, dict) or "orders" not in existing_data:
                    log_and_print(f"Corrupted JSON file at {output_path}, resetting with new structure", "WARNING")
                    existing_data = summary_structure.copy()
            except json.JSONDecodeError:
                log_and_print(f"Corrupted JSON file at {output_path}, starting fresh", "WARNING")
                existing_data = summary_structure.copy()

        # Append new failed order to orders list
        existing_data["orders"].append(failed_order)

        # Update summary counts
        existing_data["total_failed_orders"] = len(existing_data["orders"])
        
        # Update timeframe-specific count
        timeframe_key = {
            "5m": "5m_failed_orders",
            "15m": "15m_failed_orders",
            "30m": "30m_failed_orders",
            "1h": "1h_failed_orders",
            "4h": "4h_failed_orders"
        }.get(normalized_timeframe)
        
        if timeframe_key:
            existing_data[timeframe_key] = existing_data.get(timeframe_key, 0) + 1
        else:
            log_and_print(f"Unrecognized timeframe '{timeframe}' for {symbol}, not updating timeframe-specific count", "WARNING")

        # Save updated data to JSON file
        with open(output_path, 'w') as file:
            json.dump(existing_data, file, indent=4)
        log_and_print(
            f"Successfully saved failed order for {symbol} to {output_path} "
            f"(Category: {error_category}, Timeframe: {normalized_timeframe})",
            "SUCCESS"
        )
    except Exception as e:
        log_and_print(f"Error saving failed order for {symbol} to {output_path}: {str(e)}", "ERROR")

def filter_failed_orders():
    """Filter failed orders from invalid entry and stop-loss JSONs, save to filteredsignals.json, and remove them from bouncestreamsignals.json."""
    log_and_print("===== Filtering Failed Orders and Updating Bouncestream Signals =====", "TITLE")
    
    base_path = r"C:\xampp\htdocs\CIPHER\cipher trader\market\errors"
    invalid_entry_path = os.path.join(base_path, "failedordersinvalidentry.json")
    stop_loss_path = os.path.join(base_path, "failedordersbystoploss.json")
    output_path = os.path.join(base_path, "filteredsignals.json")
    bouncestream_path = BASE_OUTPUT_FOLDER  # Path to bouncestreamsignals.json
    
    # Initialize structure for filteredsignals.json
    filtered_data = {
        "stoplosserror_markets": [],
        "invalidentry_markets": []
    }
    
    # Initialize lists to store failed orders
    failed_orders = []
    
    # Process invalid entry JSON
    if os.path.exists(invalid_entry_path):
        try:
            with open(invalid_entry_path, 'r') as file:
                data = json.load(file)
            if isinstance(data, dict) and "orders" in data:
                for order in data["orders"]:
                    symbol = order.get("symbol", "unknown")
                    timeframe = order.get("timeframe", "unknown")
                    formatted_entry = f"{symbol} ({timeframe})"
                    if formatted_entry not in filtered_data["invalidentry_markets"]:
                        filtered_data["invalidentry_markets"].append(formatted_entry)
                    failed_orders.append({"pair": symbol, "timeframe": timeframe})
            log_and_print(f"Processed {len(filtered_data['invalidentry_markets'])} invalid entry markets", "INFO")
        except (json.JSONDecodeError, Exception) as e:
            log_and_print(f"Error reading {invalid_entry_path}: {str(e)}", "ERROR")
    
    # Process stop-loss JSON
    if os.path.exists(stop_loss_path):
        try:
            with open(stop_loss_path, 'r') as file:
                data = json.load(file)
            if isinstance(data, dict) and "orders" in data:
                for order in data["orders"]:
                    symbol = order.get("symbol", "unknown")
                    timeframe = order.get("timeframe", "unknown")
                    formatted_entry = f"{symbol} ({timeframe})"
                    if formatted_entry not in filtered_data["stoplosserror_markets"]:
                        filtered_data["stoplosserror_markets"].append(formatted_entry)
                    failed_orders.append({"pair": symbol, "timeframe": timeframe})
            log_and_print(f"Processed {len(filtered_data['stoplosserror_markets'])} stop-loss error markets", "INFO")
        except (json.JSONDecodeError, Exception) as e:
            log_and_print(f"Error reading {stop_loss_path}: {str(e)}", "ERROR")
    
    # Save filtered data to filteredsignals.json
    try:
        os.makedirs(base_path, exist_ok=True)
        with open(output_path, 'w') as file:
            json.dump(filtered_data, file, indent=4)
        log_and_print(f"Successfully saved filtered signals to {output_path}", "SUCCESS")
    except Exception as e:
        log_and_print(f"Error saving filtered signals to {output_path}: {str(e)}", "ERROR")
    
    # Update bouncestreamsignals.json by removing failed orders
    if os.path.exists(bouncestream_path):
        try:
            with open(bouncestream_path, 'r') as file:
                bouncestream_data = json.load(file)
            
            if not isinstance(bouncestream_data, dict) or 'orders' not in bouncestream_data:
                log_and_print(f"Invalid structure in {bouncestream_path}: Expected a dictionary with 'orders' key", "ERROR")
                return filtered_data
            
            # Normalize timeframe for matching
            timeframe_map = {
                "5m": "5m", "5 minutes": "5m", "5minutes": "5m", "M5": "5m",
                "15m": "15m", "15 minutes": "15m", "15minutes": "15m", "M15": "15m",
                "30m": "30m", "30 minutes": "30m", "30minutes": "30m", "M30": "30m",
                "1h": "1h", "1 hour": "1h", "1hour": "1h", "H1": "1h",
                "4h": "4h", "4 hours": "4h", "4hours": "4h", "H4": "4h"
            }
            
            # Create a set of failed orders for efficient lookup
            failed_set = {(order["pair"].lower(), timeframe_map.get(order["timeframe"].lower(), order["timeframe"].lower())) for order in failed_orders}
            
            # Filter out failed orders
            original_order_count = len(bouncestream_data["orders"])
            filtered_orders = []
            removed_count = 0
            
            for order in bouncestream_data["orders"]:
                if not isinstance(order, dict):
                    log_and_print(f"Skipping invalid order format in bouncestreamsignals.json: {order}", "WARNING")
                    continue
                pair = order.get("pair", "unknown").lower()
                timeframe = order.get("timeframe", "unknown")
                normalized_timeframe = timeframe_map.get(timeframe.lower(), timeframe.lower())
                if (pair, normalized_timeframe) not in failed_set:
                    filtered_orders.append(order)
                else:
                    removed_count += 1
                    log_and_print(f"Removed order for {pair} ({normalized_timeframe}) from bouncestreamsignals.json", "INFO")
            
            # Update orders list
            bouncestream_data["orders"] = filtered_orders
            
            # Update summary counts
            bouncestream_data["bouncestream_pendingorders"] = len(filtered_orders)
            
            # Recalculate timeframe-specific counts
            timeframe_counts = {
                "5m": 0,
                "15m": 0,
                "30m": 0,
                "1h": 0,
                "4h": 0
            }
            for order in filtered_orders:
                timeframe = order.get("timeframe", "unknown")
                normalized_timeframe = timeframe_map.get(timeframe.lower(), timeframe.lower())
                if normalized_timeframe in timeframe_counts:
                    timeframe_counts[normalized_timeframe] += 1
            
            # Update JSON structure with new counts
            bouncestream_data["5minutes pending orders"] = timeframe_counts["5m"]
            bouncestream_data["15minutes pending orders"] = timeframe_counts["15m"]
            bouncestream_data["30minutes pending orders"] = timeframe_counts["30m"]
            bouncestream_data["1Hour pending orders"] = timeframe_counts["1h"]
            bouncestream_data["4Hours pending orders"] = timeframe_counts["4h"]
            
            # Save updated bouncestreamsignals.json
            try:
                with open(bouncestream_path, 'w') as file:
                    json.dump(bouncestream_data, file, indent=4)
                log_and_print(
                    f"Successfully updated {bouncestream_path}: "
                    f"Removed {removed_count} failed orders, "
                    f"New total orders: {bouncestream_data['bouncestream_pendingorders']}, "
                    f"5m: {bouncestream_data['5minutes pending orders']}, "
                    f"15m: {bouncestream_data['15minutes pending orders']}, "
                    f"30m: {bouncestream_data['30minutes pending orders']}, "
                    f"1h: {bouncestream_data['1Hour pending orders']}, "
                    f"4h: {bouncestream_data['4Hours pending orders']}",
                    "SUCCESS"
                )
            except Exception as e:
                log_and_print(f"Error saving updated {bouncestream_path}: {str(e)}", "ERROR")
                
        except (json.JSONDecodeError, Exception) as e:
            log_and_print(f"Error reading or processing {bouncestream_path}: {str(e)}", "ERROR")
    
    return filtered_data

def place_pending_order(symbol, order_type, entry_price, profit_price, stop_loss, lot_size, allowed_risk):
    """Validate a pending order (buy_limit or sell_limit), adjust entry if needed to meet stops_level, 
    and mark as success if valid, otherwise save errors to JSON."""
    try:
        # Ensure symbol is selected and visible
        if not mt5.symbol_select(symbol, True):
            error_message = f"Failed to select {symbol} for pending order, error: {mt5.last_error()}"
            log_and_print(error_message, "ERROR")
            return False, None, error_message, "unknown"

        # Get symbol information
        symbol_info = mt5.symbol_info(symbol)
        if symbol_info is None:
            error_message = f"Cannot retrieve info for {symbol}"
            log_and_print(error_message, "ERROR")
            return False, None, error_message, "unknown"

        # Check if symbol is tradeable
        if not symbol_info.trade_mode == mt5.SYMBOL_TRADE_MODE_FULL:
            error_message = f"Symbol {symbol} is not tradeable (trade mode: {symbol_info.trade_mode})"
            log_and_print(error_message, "ERROR")
            return False, None, error_message, "unknown"

        # Get current market price
        tick = mt5.symbol_info_tick(symbol)
        if tick is None:
            error_message = f"Cannot retrieve tick data for {symbol}, error: {mt5.last_error()}"
            log_and_print(error_message, "ERROR")
            return False, None, error_message, "unknown"

        # Get symbol constraints
        tick_size = symbol_info.trade_tick_size
        point = symbol_info.point
        stops_level = symbol_info.trade_stops_level * point  # Minimum distance in price units
        current_bid = tick.bid
        current_ask = tick.ask

        # Normalize prices to tick size (initial)
        entry_price = round(float(entry_price) / tick_size) * tick_size
        profit_price = round(float(profit_price) / tick_size) * tick_size if profit_price else 0.0
        stop_loss = round(float(stop_loss) / tick_size) * tick_size if stop_loss else 0.0

        # Log initial price details for debugging
        log_and_print(
            f"Validating order for {symbol}: "
            f"Order Type={order_type}, Entry={entry_price}, TP={profit_price}, SL={stop_loss}, "
            f"Current Bid={current_bid}, Ask={current_ask}, Stops Level={stops_level}, Tick Size={tick_size}, "
            f"Allowed Risk={allowed_risk}",
            "DEBUG"
        )

        # Validate/adjust order type
        is_buy_limit = order_type.lower() == "buy_limit"
        is_sell_limit = order_type.lower() == "sell_limit"
        if not (is_buy_limit or is_sell_limit):
            error_message = f"Unsupported order type {order_type} for {symbol}"
            log_and_print(error_message, "ERROR")
            return False, None, error_message, "unknown"

        # Dynamically adjust entry price to meet stops_level if invalid
        adjustment_made = False
        original_entry = entry_price
        if is_buy_limit:
            # Buy limit: entry must be below current ask by at least stops_level
            min_price = current_ask - stops_level
            if entry_price > min_price:
                # Adjust entry down to the maximum valid level (just below ask by stops_level)
                entry_price = min_price
                adjustment_made = True
                log_and_print(
                    f"Adjusted buy_limit entry for {symbol} from {original_entry} to {entry_price} "
                    f"(to meet stops_level: <= {min_price})",
                    "WARNING"
                )
        elif is_sell_limit:
            # Sell limit: entry must be above current bid by at least stops_level
            max_price = current_bid + stops_level
            if entry_price < max_price:
                # Adjust entry up to the minimum valid level (just above bid by stops_level)
                entry_price = max_price
                adjustment_made = True
                log_and_print(
                    f"Adjusted sell_limit entry for {symbol} from {original_entry} to {entry_price} "
                    f"(to meet stops_level: >= {max_price})",
                    "WARNING"
                )

        # Re-normalize adjusted entry to tick size
        entry_price = round(entry_price / tick_size) * tick_size

        # Now validate/adjust SL and TP relative to ADJUSTED entry
        # (Re-normalize them again if needed)
        profit_price = round(float(profit_price) / tick_size) * tick_size if profit_price else 0.0
        stop_loss = round(float(stop_loss) / tick_size) * tick_size if stop_loss else 0.0

        if is_buy_limit:
            # For buy_limit: SL <= entry_price, TP >= entry_price
            if stop_loss and stop_loss > entry_price:
                # Adjust SL down to valid level (but check if it violates risk)
                original_sl = stop_loss
                stop_loss = entry_price  # Minimal adjustment; you could subtract a buffer
                if abs(entry_price - stop_loss) < stops_level:  # If still too close after adjust
                    error_message = (
                        f"Cannot adjust SL for {symbol} (buy_limit) without violating stops_level. "
                        f"Original SL: {original_sl}, Adjusted Entry: {entry_price}"
                    )
                    log_and_print(error_message, "ERROR")
                    return False, None, error_message, "adjusted_risk_violation"
                log_and_print(
                    f"Adjusted invalid SL for {symbol} (buy_limit) from {original_sl} to {stop_loss}",
                    "WARNING"
                )
            if profit_price and profit_price < entry_price:
                profit_price = entry_price + stops_level  # Minimal valid TP
                log_and_print(
                    f"Adjusted invalid TP for {symbol} (buy_limit) to {profit_price} (min distance)",
                    "WARNING"
                )
            # Check distances (post-adjustment)
            if stop_loss and abs(entry_price - stop_loss) < stops_level:
                error_message = (
                    f"SL too close to adjusted entry for {symbol}. "
                    f"SL: {stop_loss}, Entry: {entry_price}, Distance: {abs(entry_price - stop_loss)}, "
                    f"Required: >= {stops_level}"
                )
                log_and_print(error_message, "ERROR")
                return False, None, error_message, "stop_loss"
            if profit_price and abs(profit_price - entry_price) < stops_level:
                error_message = (
                    f"TP too close to adjusted entry for {symbol}. "
                    f"TP: {profit_price}, Entry: {entry_price}, Distance: {abs(profit_price - entry_price)}, "
                    f"Required: >= {stops_level}"
                )
                log_and_print(error_message, "ERROR")
                return False, None, error_message, "stop_loss"
        elif is_sell_limit:
            # For sell_limit: SL >= entry_price, TP <= entry_price
            if stop_loss and stop_loss < entry_price:
                # Adjust SL up to valid level
                original_sl = stop_loss
                stop_loss = entry_price  # Minimal adjustment
                if abs(entry_price - stop_loss) < stops_level:
                    error_message = (
                        f"Cannot adjust SL for {symbol} (sell_limit) without violating stops_level. "
                        f"Original SL: {original_sl}, Adjusted Entry: {entry_price}"
                    )
                    log_and_print(error_message, "ERROR")
                    return False, None, error_message, "adjusted_risk_violation"
                log_and_print(
                    f"Adjusted invalid SL for {symbol} (sell_limit) from {original_sl} to {stop_loss}",
                    "WARNING"
                )
            if profit_price and profit_price > entry_price:
                profit_price = entry_price - stops_level  # Minimal valid TP
                log_and_print(
                    f"Adjusted invalid TP for {symbol} (sell_limit) to {profit_price} (min distance)",
                    "WARNING"
                )
            # Check distances (post-adjustment)
            if stop_loss and abs(entry_price - stop_loss) < stops_level:
                error_message = (
                    f"SL too close to adjusted entry for {symbol}. "
                    f"SL: {stop_loss}, Entry: {entry_price}, Distance: {abs(entry_price - stop_loss)}, "
                    f"Required: >= {stops_level}"
                )
                log_and_print(error_message, "ERROR")
                return False, None, error_message, "stop_loss"
            if profit_price and abs(profit_price - entry_price) < stops_level:
                error_message = (
                    f"TP too close to adjusted entry for {symbol}. "
                    f"TP: {profit_price}, Entry: {entry_price}, Distance: {abs(profit_price - entry_price)}, "
                    f"Required: >= {stops_level}"
                )
                log_and_print(error_message, "ERROR")
                return False, None, error_message, "stop_loss"

        # If all validations pass (post-adjustment), mark as success
        adjustment_note = f" (entry adjusted from {original_entry} to {entry_price})" if adjustment_made else ""
        log_and_print(
            f"Pending {order_type} order for {symbol} validated successfully at {entry_price}{adjustment_note} "
            f"with TP {profit_price}, SL {stop_loss}, Allowed Risk={allowed_risk} (Order not sent as per request)",
            "SUCCESS"
        )
        return True, None, None, None

    except Exception as e:
        error_message = f"Error validating pending order for {symbol}: {str(e)}"
        log_and_print(error_message, "ERROR")
        return False, None, error_message, "unknown"
    
def add_to_watchlist_and_place_orders():
    """Add market symbols from JSON to MT5 watchlist and validate pending orders based on JSON signals."""
    log_and_print("===== Adding Market Symbols to Watchlist and Validating Pending Orders =====", "TITLE")
    
    if not initialize_mt5():
        log_and_print("Cannot proceed due to MT5 initialization or login failure", "ERROR")
        return

    # Fetch all available symbols
    available_symbols = get_available_symbols()
    if not available_symbols:
        log_and_print("No available symbols, aborting", "ERROR")
        return

    # Load signals from JSON
    signals = load_market_signals()
    if not signals:
        log_and_print("No signals loaded from JSON, aborting", "ERROR")
        return

    # Clear error JSON files at the start of the run
    error_files = [
        r"C:\xampp\htdocs\CIPHER\cipher trader\market\errors\failedordersinvalidentry.json",
        r"C:\xampp\htdocs\CIPHER\cipher trader\market\errors\failedordersbystoploss.json",
        r"C:\xampp\htdocs\CIPHER\cipher trader\market\errors\failedpendingorders.json",
        r"C:\xampp\htdocs\CIPHER\cipher trader\market\errors\filteredsignals.json"  # Added filteredsignals.json
    ]
    for f in error_files:
        if os.path.exists(f):
            try:
                with open(f, 'w') as file:
                    json.dump([], file)  # Clear the file
                log_and_print(f"Cleared error file {f}", "DEBUG")
            except Exception as e:
                log_and_print(f"Error clearing {f}: {str(e)}", "WARNING")

    added_symbols = []
    failed_symbols = []
    pending_orders_placed = []
    invalid_signals = []
    current_failed_orders = []  # Track failed orders in this run

    # Step 1: Add symbols to Market Watch
    unique_symbols = []
    for signal in signals:
        try:
            if not isinstance(signal, dict):
                log_and_print(f"Invalid signal format: {signal} (Expected a dictionary)", "ERROR")
                invalid_signals.append(signal)
                continue
            json_symbol = signal['pair']
            if json_symbol not in unique_symbols:
                unique_symbols.append(json_symbol)
        except (TypeError, KeyError) as e:
            log_and_print(f"Error processing signal: {signal}, Error: {str(e)}", "ERROR")
            invalid_signals.append(signal)
            continue

    for json_symbol in unique_symbols:
        try:
            server_symbol = get_exact_symbol_match(json_symbol, available_symbols)
            if server_symbol is None:
                log_and_print(f"Skipping {json_symbol}: No server match found", "ERROR")
                failed_symbols.append(json_symbol)
                continue

            # Attempt to select the symbol directly
            if mt5.symbol_select(server_symbol, True):
                log_and_print(f"Symbol {server_symbol} selected directly in Market Watch", "SUCCESS")
                added_symbols.append(server_symbol)
                continue

            # If direct selection fails, attempt test order
            log_and_print(f"Direct selection of {server_symbol} failed, attempting test order...", "WARNING")
            for attempt in range(MAX_RETRIES):
                if place_test_order(server_symbol):
                    log_and_print(f"Symbol {server_symbol} added to Market Watch via test order", "SUCCESS")
                    added_symbols.append(server_symbol)
                    break
                else:
                    log_and_print(f"Attempt {attempt + 1}/{MAX_RETRIES}: Test order for {server_symbol} failed, retrying...", "WARNING")
                    time.sleep(RETRY_DELAY)
            else:
                log_and_print(f"Symbol {server_symbol} could not be added to Market Watch after retries", "ERROR")
                failed_symbols.append(json_symbol)

        except Exception as e:
            log_and_print(f"Error processing symbol {json_symbol}: {str(e)}", "ERROR")
            failed_symbols.append(json_symbol)

    # Step 2: Validate pending orders for all signals
    log_and_print("===== Validating Pending Orders =====", "TITLE")
    for signal in signals:
        try:
            if not isinstance(signal, dict):
                log_and_print(f"Skipping invalid signal format: {signal} (Expected a dictionary)", "ERROR")
                invalid_signals.append(signal)
                continue

            json_symbol = signal['pair']
            order_type = signal['order_type']
            entry_price = signal['entry_price']
            profit_price = signal['profit_price'] if signal['profit_price'] else None
            stop_loss = signal['exit_price'] if signal['exit_price'] else None
            lot_size = signal['lot_size']
            allowed_risk = signal.get('allowed_risk', None)  # Extract allowed_risk with default None

            server_symbol = get_exact_symbol_match(json_symbol, available_symbols)
            if server_symbol is None:
                error_message = "No server symbol match found"
                log_and_print(f"Skipping validation for {json_symbol}: {error_message}", "WARNING")
                failed_symbols.append(json_symbol)
                save_failed_orders(json_symbol, order_type, entry_price, profit_price, stop_loss, lot_size, allowed_risk, error_message, "unknown", signal=signal)
                current_failed_orders.append(json_symbol)
                continue

            if server_symbol in added_symbols:
                success, order_id, error_message, error_category = place_pending_order(server_symbol, order_type, entry_price, profit_price, stop_loss, lot_size, allowed_risk)
                if success:
                    pending_orders_placed.append((server_symbol, order_id, order_type, entry_price, profit_price, stop_loss, allowed_risk))
                else:
                    log_and_print(f"Failed to validate pending order for {server_symbol}: {error_message}", "ERROR")
                    failed_symbols.append(json_symbol)
                    save_failed_orders(server_symbol, order_type, entry_price, profit_price, stop_loss, lot_size, allowed_risk, error_message or "Unknown error", error_category, signal=signal)
                    current_failed_orders.append(server_symbol)
            else:
                error_message = f"Symbol {server_symbol} not added to Market Watch"
                log_and_print(f"Skipping validation for {server_symbol}: {error_message}", "WARNING")
                save_failed_orders(server_symbol, order_type, entry_price, profit_price, stop_loss, lot_size, allowed_risk, error_message, "unknown", signal=signal)
                current_failed_orders.append(server_symbol)

        except (TypeError, KeyError) as e:
            log_and_print(f"Error processing signal for validation: {signal}, Error: {str(e)}", "ERROR")
            invalid_signals.append(signal)
            continue

    # Step 3: Filter failed orders and save to filteredsignals.json
    filtered_data = filter_failed_orders()

    # Output results
    log_and_print("===== Pending Order Validation Summary =====", "TITLE")
    if failed_symbols:
        log_and_print(f"Symbols failed to add or process: {', '.join(set(failed_symbols))}", "ERROR")
    if pending_orders_placed:
        log_and_print(
            f"Pending orders validated successfully: {', '.join([f'{sym} ({otype} at {entry}, TP {tp}, SL {sl}, Risk {risk})' for sym, oid, otype, entry, tp, sl, risk in pending_orders_placed])}",
            "SUCCESS"
        )
    else:
        log_and_print("No pending orders validated successfully", "INFO")
    if invalid_signals:
        log_and_print(f"Invalid signals skipped: {', '.join([str(s) for s in invalid_signals])}", "ERROR")
    log_and_print(f"Total: {len(pending_orders_placed)} pending orders validated, {len(current_failed_orders)} failed orders saved, {len(invalid_signals)} invalid signals", "INFO")
    log_and_print(
        f"Filtered signals: {len(filtered_data['invalidentry_markets'])} invalid entry markets, "
        f"{len(filtered_data['stoplosserror_markets'])} stop-loss error markets",
        "INFO"
    )

def main():
    """Main function to execute the watchlist addition and pending order placement process."""
    try:
        log_and_print("===== Starting MT5 Watchlist Addition and Pending Order Process =====", "TITLE")
        add_to_watchlist_and_place_orders()
        log_and_print("===== MT5 Watchlist Addition and Pending Order Process Completed =====", "TITLE")
    except Exception as e:
        log_and_print(f"Error in main process: {str(e)}", "ERROR")
    finally:
        mt5.shutdown()
        log_and_print("MT5 connection closed", "INFO")
        
if __name__ == "__main__":
    main()