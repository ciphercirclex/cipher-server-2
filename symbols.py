import MetaTrader5 as mt5
import logging
from colorama import Fore, Style, init
import time

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

# Suppress WebDriver-related logs
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

# Configuration (same as fetchandinsertorders.py)
LOGIN_ID = "101347351"
PASSWORD = "@Techknowdge12#"
SERVER = "DerivSVG-Server-02"
TERMINAL_PATH = r"C:\Program Files\MetaTrader 5\terminal64.exe"  # Update with your MT5 terminal path
MAX_RETRIES = 5
RETRY_DELAY = 3

# Market symbols to process (using exact names from available symbols)
MARKETS = [
    'Volatility 10 Index', 'Volatility 25 Index', 'Volatility 50 Index',
    'Volatility 75 Index', 'Volatility 100 Index', 'Drift Switch Index 10',
    'Drift Switch Index 20', 'Drift Switch Index 30', 'Multi Step 2 Index',
    'Multi Step 4 Index', 'Step Index', 'USDJPY', 'USDCAD', 'USDCHF',
    'EURUSD', 'GBPUSD', 'AUDUSD', 'NZDUSD', 'XAUUSD', 'US Tech 100',
    'Wall Street 30', 'AUDJPY', 'AUDNZD', 'EURCHF', 'EURGBP', 'EURJPY',
    'GBPJPY'
]

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

def add_to_watchlist():
    """Check and add market symbols to MT5 watchlist, report status."""
    log_and_print("===== Adding Market Symbols to Watchlist =====", "TITLE")
    
    if not initialize_mt5():
        log_and_print("Cannot proceed due to MT5 initialization or login failure", "ERROR")
        return

    # Fetch all available symbols
    available_symbols = get_available_symbols()

    added_symbols = []
    existing_symbols = []
    failed_symbols = []

    for market in MARKETS:
        try:
            # Check if symbol is available on the server
            if market not in available_symbols:
                log_and_print(f"Symbol {market} is not available on the server", "ERROR")
                failed_symbols.append(market)
                continue

            # Check if symbol is already in Market Watch and visible
            symbol_info = mt5.symbol_info(market)
            if symbol_info is not None and symbol_info.visible:
                log_and_print(f"Symbol {market} already exists and is visible in Market Watch", "INFO")
                existing_symbols.append(market)
                continue

            # Try to select the symbol to make it visible
            for _ in range(2):  # Retry once if selection fails
                if mt5.symbol_select(market, True):
                    log_and_print(f"Successfully added {market} to Market Watch (made visible)", "SUCCESS")
                    added_symbols.append(market)
                    break
                else:
                    log_and_print(f"Failed to select {market}, error: {mt5.last_error()}. Retrying...", "WARNING")
                    time.sleep(1)  # Brief delay before retry
            else:
                log_and_print(f"Symbol {market} could not be added to Market Watch after retries, error: {mt5.last_error()}", "ERROR")
                failed_symbols.append(market)

        except Exception as e:
            log_and_print(f"Error processing symbol {market}: {str(e)}", "ERROR")
            failed_symbols.append(market)

    # Output results
    log_and_print("===== Watchlist Addition Summary =====", "TITLE")
    if added_symbols:
        log_and_print(f"Symbols added to Market Watch: {', '.join(added_symbols)}", "SUCCESS")
    if existing_symbols:
        log_and_print(f"Symbols already in Market Watch: {', '.join(existing_symbols)}", "INFO")
    if failed_symbols:
        log_and_print(f"Symbols failed to add: {', '.join(failed_symbols)}", "ERROR")
    log_and_print(f"Total: {len(added_symbols)} added, {len(existing_symbols)} already exist, {len(failed_symbols)} failed", "INFO")

def main():
    """Main function to execute the watchlist addition process."""
    try:
        log_and_print("===== Starting MT5 Watchlist Addition Process =====", "TITLE")
        add_to_watchlist()
        log_and_print("===== MT5 Watchlist Addition Process Completed =====", "TITLE")
    except Exception as e:
        log_and_print(f"Error in main process: {str(e)}", "ERROR")
    finally:
        log_and_print("MT5 connection closed", "INFO")

if __name__ == "__main__":
    main()