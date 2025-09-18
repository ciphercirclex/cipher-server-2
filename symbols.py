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
    """Load market signals from the JSON file."""
    try:
        if not os.path.exists(BASE_OUTPUT_FOLDER):
            log_and_print(f"JSON file not found at {BASE_OUTPUT_FOLDER}", "ERROR")
            return []

        with open(BASE_OUTPUT_FOLDER, 'r') as file:
            data = json.load(file)
        
        log_and_print(f"Loaded {len(data)} signals from JSON", "INFO")
        return data

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

def place_pending_order(symbol, order_type, entry_price, profit_price, lot_size):
    """Place a pending order (buy_limit or sell_limit) with take-profit after validating price."""
    try:
        # Ensure symbol is selected and visible
        if not mt5.symbol_select(symbol, True):
            log_and_print(f"Failed to select {symbol} for pending order, error: {mt5.last_error()}", "ERROR")
            return False, None

        # Get symbol information
        symbol_info = mt5.symbol_info(symbol)
        if symbol_info is None:
            log_and_print(f"Cannot retrieve info for {symbol}", "ERROR")
            return False, None

        # Check if symbol is tradeable
        if not symbol_info.trade_mode == mt5.SYMBOL_TRADE_MODE_FULL:
            log_and_print(f"Symbol {symbol} is not tradeable (trade mode: {symbol_info.trade_mode})", "ERROR")
            return False, None

        # Get current market price
        tick = mt5.symbol_info_tick(symbol)
        if tick is None:
            log_and_print(f"Cannot retrieve tick data for {symbol}, error: {mt5.last_error()}", "ERROR")
            return False, None

        # Get symbol constraints
        tick_size = symbol_info.trade_tick_size
        stops_level = symbol_info.trade_stops_level * symbol_info.point  # Minimum distance in price units
        current_bid = tick.bid
        current_ask = tick.ask

        # Normalize prices to tick size
        entry_price = round(float(entry_price) / tick_size) * tick_size
        profit_price = round(float(profit_price) / tick_size) * tick_size if profit_price else 0.0

        # Validate and adjust entry price based on order type
        is_buy_limit = order_type.lower() == "buy_limit"
        is_sell_limit = order_type.lower() == "sell_limit"

        if is_buy_limit:
            # Buy limit: entry price must be below current ask price by at least stops_level
            min_price = current_ask - stops_level
            if entry_price > min_price:
                log_and_print(f"Adjusting buy_limit entry price for {symbol} from {entry_price} to {min_price} to meet minimum distance requirement", "WARNING")
                entry_price = round(min_price / tick_size) * tick_size
        elif is_sell_limit:
            # Sell limit: entry price must be above current bid price by at least stops_level
            max_price = current_bid + stops_level
            if entry_price < max_price:
                log_and_print(f"Adjusting sell_limit entry price for {symbol} from {entry_price} to {max_price} to meet minimum distance requirement", "WARNING")
                entry_price = round(max_price / tick_size) * tick_size
        else:
            log_and_print(f"Unsupported order type {order_type} for {symbol}", "ERROR")
            return False, None

        # Prepare pending order
        mt5_order_type = mt5.ORDER_TYPE_BUY_LIMIT if is_buy_limit else mt5.ORDER_TYPE_SELL_LIMIT
        request = {
            "action": mt5.TRADE_ACTION_PENDING,
            "symbol": symbol,
            "volume": float(lot_size),
            "type": mt5_order_type,
            "price": entry_price,
            "tp": profit_price if profit_price else 0.0,  # Set take-profit if provided
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }

        # Send pending order
        result = mt5.order_send(request)
        if result.retcode != mt5.TRADE_RETCODE_DONE:
            error_code, error_message = mt5.last_error()
            log_and_print(f"Pending order for {symbol} failed, error: {result.retcode}, {error_message}", "ERROR")
            if result.retcode == 10015:  # Invalid price
                log_and_print(f"Invalid price for {symbol}. Current bid: {current_bid}, ask: {current_ask}, required min distance: {stops_level}, attempted entry: {entry_price}", "ERROR")
            return False, None

        log_and_print(f"Pending {order_type} order for {symbol} placed successfully at {entry_price} with TP {profit_price} (Order #{result.order})", "SUCCESS")
        return True, result.order

    except Exception as e:
        log_and_print(f"Error placing pending order for {symbol}: {str(e)}", "ERROR")
        return False, None

def add_to_watchlist_and_place_orders():
    """Add market symbols from JSON to MT5 watchlist and place pending orders based on JSON signals."""
    log_and_print("===== Adding Market Symbols to Watchlist and Placing Pending Orders =====", "TITLE")
    
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

    added_symbols = []
    existing_symbols = []
    failed_symbols = []
    pending_orders_placed = []

    # Step 1: Add symbols to Market Watch (using case-insensitive matching)
    unique_symbols = list(set(signal['pair'] for signal in signals))
    for json_symbol in unique_symbols:
        try:
            # Find exact server symbol match (case-insensitive)
            server_symbol = get_exact_symbol_match(json_symbol, available_symbols)
            if server_symbol is None:
                log_and_print(f"Skipping {json_symbol}: No server match found", "ERROR")
                failed_symbols.append(json_symbol)
                continue

            # Check if symbol is already in Market Watch and visible
            symbol_info = mt5.symbol_info(server_symbol)
            if symbol_info is not None and symbol_info.visible:
                log_and_print(f"Symbol {server_symbol} already exists and is visible in Market Watch", "INFO")
                existing_symbols.append(server_symbol)
                continue

            # Try to select the symbol directly
            if mt5.symbol_select(server_symbol, True):
                log_and_print(f"Symbol {server_symbol} selected directly in Market Watch", "SUCCESS")
                added_symbols.append(server_symbol)
                continue

            # If direct selection fails, attempt a test order
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

    # Step 2: Place pending orders for all signals
    log_and_print("===== Placing Pending Orders =====", "TITLE")
    for signal in signals:
        json_symbol = signal['pair']
        order_type = signal['order_type']
        entry_price = signal['entry_price']
        profit_price = signal['profit_price'] if signal['profit_price'] else None
        lot_size = signal['lot_size']

        # Map to server symbol
        server_symbol = get_exact_symbol_match(json_symbol, available_symbols)
        if server_symbol is None:
            log_and_print(f"Skipping pending order for {json_symbol}: No server match found", "WARNING")
            failed_symbols.append(json_symbol)
            continue

        if server_symbol in added_symbols or server_symbol in existing_symbols:
            success, order_id = place_pending_order(server_symbol, order_type, entry_price, profit_price, lot_size)
            if success:
                pending_orders_placed.append((server_symbol, order_id, order_type, entry_price, profit_price))
            else:
                log_and_print(f"Failed to place pending order for {server_symbol}", "ERROR")
                failed_symbols.append(json_symbol)
        else:
            log_and_print(f"Skipping pending order for {server_symbol} as it was not added to Market Watch", "WARNING")

    # Output results
    log_and_print("===== Watchlist Addition and Pending Order Summary =====", "TITLE")
    if added_symbols:
        log_and_print(f"Symbols added to Market Watch: {', '.join(added_symbols)}", "SUCCESS")
    if existing_symbols:
        log_and_print(f"Symbols already in Market Watch: {', '.join(existing_symbols)}", "INFO")
    if failed_symbols:
        log_and_print(f"Symbols failed to add or process: {', '.join(set(failed_symbols))}", "ERROR")  # Use set to avoid duplicates
    if pending_orders_placed:
        log_and_print(f"Pending orders placed: {', '.join([f'{sym} ({otype} at {entry}, TP {tp}, Order #{oid})' for sym, oid, otype, entry, tp in pending_orders_placed])}", "SUCCESS")
    else:
        log_and_print("No pending orders placed", "INFO")
    log_and_print(f"Total: {len(added_symbols)} added, {len(existing_symbols)} already exist, {len(set(failed_symbols))} failed, {len(pending_orders_placed)} pending orders placed", "INFO")

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