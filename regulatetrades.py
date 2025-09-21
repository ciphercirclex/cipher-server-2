import connectwithinfinitydb as db
import MetaTrader5 as mt5
import os
import shutil
import json
import logging
import time
import asyncio
from datetime import datetime, timezone, timedelta
import pytz
from typing import List, Dict, Optional
from colorama import Fore, Style, init
import threading
import difflib
from collections import defaultdict

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

# Suppress unnecessary logs
for name in ['webdriver_manager', 'selenium', 'urllib3', 'selenium.webdriver']:
    logging.getLogger(name).setLevel(logging.WARNING)

# Thread-local storage for MT5 instances
thread_local = threading.local()

# Configuration Section
EXPORT_DIR = r'C:\xampp\htdocs\CIPHER\cipherdb\cipheruserdb'
BASE_MT5_DIR = r"C:\xampp\htdocs\CIPHER\metaTrader5\users"
ORIGINAL_MT5_DIR = r"C:\xampp\htdocs\CIPHER\metaTrader5\MetaTrader 5"
SIGNALS_FILE = r"C:\xampp\htdocs\CIPHER\cipher trader\market\bouncestreamsignals.json"
RUNNING_TRADES_DIR = r"C:\xampp\htdocs\CIPHER\cipher trader\market\runningtrades"
CLOSED_TRADES_DIR = r"C:\xampp\htdocs\CIPHER\cipher trader\market\closedtrades"
LIMIT_ORDERS_DIR = r"C:\xampp\htdocs\CIPHER\cipher trader\market\limitorders"
RETRY_MAX_ATTEMPTS = 3
RETRY_DELAY = 2
MAX_RETRIES = 5
MT5_RETRY_DELAY = 3
CHECK_INTERVAL = 10  # Seconds to wait between trade regulation checks
SL_ADJUSTMENT_PERCENT = 0.10  # Stop-loss adjustment percentage for entry price
SL_RR_05_PERCENT = 0.25  # Stop-loss adjustment percentage for 1:0.5 RR

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

# Configuration Manager Class
class ConfigManager:
    """Manages configuration settings for terminal copying and validation."""
    def __init__(self):
        self.main_export_dir: str = EXPORT_DIR
        self.base_mt5_dir: str = BASE_MT5_DIR
        self.original_mt5_dir: str = ORIGINAL_MT5_DIR
        self.running_trades_dir: str = RUNNING_TRADES_DIR
        self.closed_trades_dir: str = CLOSED_TRADES_DIR
        self.limit_orders_dir: str = LIMIT_ORDERS_DIR
        self.valid_programmes: List[str] = ['bouncestream']
        self.valid_brokers: List[str] = ['deriv', 'forex']
        self.min_capital_regular: float = 3.0
        self.min_capital_unique: float = 3.0

    def validate_directory(self) -> bool:
        """Validate all required directories exist and are writable."""
        for directory in [self.main_export_dir, self.running_trades_dir, self.closed_trades_dir, self.limit_orders_dir]:
            if not os.path.exists(directory):
                try:
                    os.makedirs(directory, exist_ok=True)
                    log_and_print(f"Created directory: {directory}", "INFO")
                except Exception as e:
                    log_and_print(f"Failed to create directory {directory}: {str(e)}", "ERROR")
                    return False
            if not os.access(directory, os.W_OK):
                log_and_print(f"Directory not writable: {directory}", "ERROR")
                return False
        return True

    def validate_mt5_directory(self) -> bool:
        """Validate the original MetaTrader 5 directory exists."""
        if not os.path.isdir(self.original_mt5_dir):
            log_and_print(f"Original MetaTrader 5 directory does not exist: {self.original_mt5_dir}", "ERROR")
            return False
        terminal_path = os.path.join(self.original_mt5_dir, "terminal64.exe")
        if not os.path.isfile(terminal_path):
            log_and_print(f"terminal64.exe not found in {self.original_mt5_dir}", "ERROR")
            return False
        return True

    def create_account_terminal(self, user_id: str, account_type: str) -> Optional[str]:
        """Copy the MetaTrader 5 directory to a new directory named with account type and user_id."""
        account_dir_name = f"MetaTrader 5 {account_type}-{user_id}"
        account_dir_path = os.path.join(self.base_mt5_dir, account_dir_name)
        new_terminal_path = os.path.join(account_dir_path, "terminal64.exe")

        if os.path.exists(new_terminal_path):
            log_and_print(f"Terminal already exists at {new_terminal_path}", "INFO")
            return new_terminal_path

        try:
            os.makedirs(account_dir_path, exist_ok=True)
            shutil.copytree(self.original_mt5_dir, account_dir_path, dirs_exist_ok=True)
            log_and_print(f"Successfully copied MetaTrader 5 directory to {account_dir_path}", "SUCCESS")
            if os.path.exists(new_terminal_path):
                return new_terminal_path
            else:
                log_and_print(f"terminal64.exe not found in copied directory {account_dir_path}", "ERROR")
                return None
        except Exception as e:
            log_and_print(f"Failed to copy MetaTrader 5 directory for user_{user_id} ({account_type}): {str(e)}", "ERROR")
            return None

    def validate_field(self, field: str, value: str, valid_values: List[str], field_name: str) -> Optional[str]:
        """Validate a field against a list of valid values, return None if invalid."""
        if value is None or str(value).lower().strip() == 'none':
            log_and_print(f"Invalid {field_name} '{value}' detected, skipping record", "DEBUG")
            return None
        value = str(value).lower().strip()
        if value not in valid_values:
            log_and_print(f"Invalid {field_name} '{value}' detected, skipping record", "DEBUG")
            return None
        return value

# MT5 Manager Class (Full Updated Version)
class MT5Manager:
    """Manages MT5 initialization and login, maintaining connections without shutdown."""
    def __init__(self):
        pass  # No need for mt5_instances dict

    def initialize_mt5(self, server: str, login: str, password: str, terminal_path: str, account_key: str) -> bool:
        """Initialize MT5 terminal and login with provided credentials using the specified terminal path."""
        log_and_print(f"Attempting MT5 login for {account_key} (server: {server}, login: {login}) using {terminal_path}", "INFO")
        
        try:
            if mt5.initialize(  # Use global mt5 directly
                path=terminal_path,
                login=int(login),
                password=password,
                server=server,
                portable=True,
                timeout=120000
            ):
                log_and_print(f"Successfully initialized MT5 terminal for {account_key}", "SUCCESS")
            else:
                error_code, error_message = mt5.last_error()
                log_and_print(f"Failed to initialize MT5 terminal for {account_key}. Error: {error_code}, {error_message}", "ERROR")
                return False
        except Exception as e:
            log_and_print(f"Exception during MT5 initialization for {account_key}: {str(e)}", "ERROR")
            return False

        for _ in range(5):
            if mt5.terminal_info() is not None:  # Use global mt5
                log_and_print(f"MT5 terminal fully initialized for {account_key}", "DEBUG")
                return True
            log_and_print(f"Waiting for MT5 terminal to fully initialize for {account_key}...", "INFO")
            time.sleep(2)
        else:
            log_and_print(f"MT5 terminal not ready for {account_key}", "ERROR")
            return False

# Trade Regulator Class
class TradeRegulator:
    def __init__(self):
        self.config = ConfigManager()
        self.mt5_manager = MT5Manager()
        self.valid_accounts = []
        self.processed_programme_ids = set()
        self.total_orders_adjusted = 0
        self.total_orders_failed = 0
        self.signals = []

    def get_available_symbols(self, mt5_instance) -> List[str]:
        """Retrieve all available symbols from the MT5 server."""
        try:
            symbols = mt5_instance.symbols_get()
            if not symbols:
                log_and_print("No symbols retrieved from MT5 server", "ERROR")
                return []
            return [symbol.name for symbol in symbols]
        except Exception as e:
            log_and_print(f"Error retrieving symbols from MT5 server: {str(e)}", "ERROR")
            return []

    def get_exact_symbol_match(self, json_symbol: str, available_symbols: List[str]) -> Optional[str]:
        """Find an exact case-insensitive match for the JSON symbol in available MT5 symbols."""
        try:
            json_lower = json_symbol.lower()
            lower_available = [s.lower() for s in available_symbols]
            if json_lower in lower_available:
                index = lower_available.index(json_lower)
                exact = available_symbols[index]
                #log_and_print(f"Matched '{json_symbol}' to exact server symbol: '{exact}'", "DEBUG")
                return exact
            else:
                close_matches = difflib.get_close_matches(json_lower, lower_available, n=3, cutoff=0.6)
                log_and_print(f"No exact match for '{json_symbol}'. Closest server symbols: {', '.join(close_matches) if close_matches else 'None'}", "WARNING")
                return None
        except Exception as e:
            log_and_print(f"Error in get_exact_symbol_match for '{json_symbol}': {str(e)}", "ERROR")
            return None

    def place_test_order(self, mt5_instance, symbol: str) -> bool:
        """Place a test order to add a symbol to the Market Watch."""
        try:
            symbol_info = mt5_instance.symbol_info(symbol)
            if not symbol_info:
                log_and_print(f"Cannot retrieve symbol info for test order: {symbol}", "ERROR")
                return False

            point = symbol_info.point
            price = mt5_instance.symbol_info_tick(symbol).ask
            request = {
                "action": mt5_instance.TRADE_ACTION_DEAL,
                "symbol": symbol,
                "volume": 0.01,  # Minimum volume for test order
                "type": mt5_instance.ORDER_TYPE_BUY,
                "price": price,
                "sl": price - 100 * point,
                "tp": price + 100 * point,
                "type_time": mt5_instance.ORDER_TIME_DAY,
                "type_filling": mt5_instance.ORDER_FILLING_IOC,
            }
            result = mt5_instance.order_send(request)
            if result.retcode == mt5_instance.TRADE_RETCODE_DONE:
                log_and_print(f"Test order placed successfully for {symbol}, closing position", "DEBUG")
                position_id = result.deal
                close_request = {
                    "action": mt5_instance.TRADE_ACTION_DEAL,
                    "symbol": symbol,
                    "volume": 0.01,
                    "type": mt5_instance.ORDER_TYPE_SELL,
                    "position": position_id,
                    "price": mt5_instance.symbol_info_tick(symbol).bid,
                    "type_time": mt5_instance.ORDER_TIME_DAY,
                    "type_filling": mt5_instance.ORDER_FILLING_IOC,
                }
                close_result = mt5_instance.order_send(close_request)
                if close_result.retcode == mt5_instance.TRADE_RETCODE_DONE:
                    log_and_print(f"Test order closed successfully for {symbol}", "DEBUG")
                    return True
                else:
                    log_and_print(f"Failed to close test order for {symbol}: {close_result.comment}", "ERROR")
                    return False
            else:
                log_and_print(f"Failed to place test order for {symbol}: {result.comment}", "ERROR")
                return False
        except Exception as e:
            log_and_print(f"Error placing test order for {symbol}: {str(e)}", "ERROR")
            return False

    def select_symbol(self, mt5_instance, symbol: str) -> bool:
        """Ensure a symbol is selected in the Market Watch, retrying or placing a test order if needed."""
        for attempt in range(1, MAX_RETRIES + 1):
            if mt5_instance.symbol_select(symbol, True):
                return True
            else:
                log_and_print(f"Failed to select {symbol} in Market Watch on attempt {attempt}", "WARNING")
                if attempt == MAX_RETRIES:
                    log_and_print(f"Attempting to add {symbol} via test order", "INFO")
                    if self.place_test_order(mt5_instance, symbol):
                        if mt5_instance.symbol_select(symbol, True):
                            log_and_print(f"Successfully selected {symbol} in Market Watch after test order", "DEBUG")
                            return True
                        else:
                            log_and_print(f"Failed to select {symbol} in Market Watch even after test order", "ERROR")
                            return False
                time.sleep(MT5_RETRY_DELAY)
        return False

    def normalize_row(self, row: Dict) -> Dict:
        """Normalize row data to handle string 'None' values and ensure correct types."""
        normalized = {}
        for key, value in row.items():
            if value == "None" or value is None:
                normalized[key] = None
            elif key in ['user_id', 'programme_id', 'up_user_id', 'subaccount_id']:
                normalized[key] = int(value) if value is not None else None
            else:
                normalized[key] = value
        return normalized

    def load_signals(self) -> bool:
        """Load signals from bouncestreamsignals.json."""
        try:
            with open(SIGNALS_FILE, 'r', encoding='utf-8') as file:
                data = json.load(file)
            if 'orders' not in data or not isinstance(data['orders'], list):
                log_and_print(f"Invalid format in {SIGNALS_FILE}: 'orders' key missing or not a list", "ERROR")
                return False
            self.signals = data['orders']
            log_and_print(f"Successfully loaded {len(self.signals)} signals from {SIGNALS_FILE}", "SUCCESS")
            return True
        except FileNotFoundError:
            log_and_print(f"Signals file not found: {SIGNALS_FILE}", "ERROR")
            return False
        except json.JSONDecodeError:
            log_and_print(f"Invalid JSON format in {SIGNALS_FILE}", "ERROR")
            return False
        except Exception as e:
            log_and_print(f"Error loading signals from {SIGNALS_FILE}: {str(e)}", "ERROR")
            return False

    def save_to_json(self, file_path: str, data: List[Dict], append: bool = True) -> bool:
        """Save data to a JSON file, creating parent directory if it doesn't exist."""
        try:
            # Ensure parent directory exists
            parent_dir = os.path.dirname(file_path)
            if parent_dir and not os.path.exists(parent_dir):
                os.makedirs(parent_dir, exist_ok=True)
                log_and_print(f"Created directory: {parent_dir}", "INFO")

            existing_data = []
            if append and os.path.exists(file_path):
                try:
                    with open(file_path, 'r', encoding='utf-8') as file:
                        existing_data = json.load(file)
                    if not isinstance(existing_data, list):
                        existing_data = []
                except json.JSONDecodeError:
                    log_and_print(f"Corrupted JSON file at {file_path}, starting fresh", "WARNING")
                    existing_data = []

            if append:
                existing_data.extend(data)
                data_to_save = existing_data
            else:
                data_to_save = data

            with open(file_path, 'w', encoding='utf-8') as file:
                json.dump(data_to_save, file, indent=4)
            log_and_print(f"Saved {len(data)} records to {file_path}", "INFO")
            return True
        except Exception as e:
            log_and_print(f"Error saving to {file_path}: {str(e)}", "ERROR")
            return False

    def update_bouncestream_signals(self, new_signal: Dict) -> bool:
        """Append a new signal to bouncestreamsignals.json."""
        try:
            signals_data = {"orders": self.signals}
            signals_data["orders"].append(new_signal)
            with open(SIGNALS_FILE, 'w', encoding='utf-8') as file:
                json.dump(signals_data, file, indent=4)
            log_and_print(f"Appended new signal to {SIGNALS_FILE}", "INFO")
            self.signals = signals_data["orders"]  # Update in-memory signals
            return True
        except Exception as e:
            log_and_print(f"Error updating {SIGNALS_FILE}: {str(e)}", "ERROR")
            return False

    def save_adjustment_error(self, account_key: str, symbol: str, order_id: int, error_message: str) -> None:
        """Save stop-loss adjustment errors to a JSON file."""
        error_dir = os.path.join(EXPORT_DIR, "errors")
        output_path = os.path.join(error_dir, "stoploss_adjustment_errors.json")
        
        error_entry = {
            "account_key": account_key,
            "symbol": symbol,
            "order_id": order_id,
            "error_message": error_message,
            "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00')
        }
        
        self.save_to_json(output_path, [error_entry])

    async def get_active_users(self) -> List[Dict[str, str]]:
        """Fetch active users from the users table."""
        sql_query = """
            SELECT id
            FROM users
            WHERE account_status = 'active'
        """
        log_and_print(f"Fetching active users with query: {sql_query}", "INFO")
        result = db.execute_query(sql_query)
        
        if result['status'] != 'success':
            log_and_print(f"Failed to fetch active users: {result.get('message', 'No message')}", "ERROR")
            return []
        
        if not isinstance(result['results'], list) or not result['results']:
            log_and_print("No active users found", "WARNING")
            return []
        
        log_and_print(f"Fetched {len(result['results'])} active users", "SUCCESS")
        return [{'id': str(row['id'])} for row in result['results']]

    async def validate_account(self, record: Dict, active_users: List[Dict[str, str]]) -> Optional[Dict]:
        """Validate a programme record for MT5 login eligibility."""
        programme_id = str(record['programme_id'])
        user_id = str(record['user_id'])
        subaccount_id = str(record['subaccount_id']) if record['subaccount_id'] and str(record['subaccount_id']) != 'NULL' else None
        programme = record['programme']
        broker = record['broker']
        broker_server = record['broker_server']
        broker_loginid = str(record['broker_loginid']) if record['broker_loginid'] else ''
        broker_password = str(record['broker_password']) if record['broker_password'] else ''
        programme_timeframe = record.get('programme_timeframe', None)

        log_and_print(f"Validating record: programme_id={programme_id}, user_id={user_id}, subaccount_id={subaccount_id}, "
                    f"programme={programme}, broker={broker}, programme_timeframe={programme_timeframe}", "DEBUG")

        programme = self.config.validate_field('programme', programme, self.config.valid_programmes, 'programme')
        broker = self.config.validate_field('broker', broker, self.config.valid_brokers, 'broker')
        programme_timeframe = self.config.validate_field('programme_timeframe', programme_timeframe, 
                                                        ['priority_timeframe', 'alltimeframes'], 'programme_timeframe')

        if not all([programme, broker, programme_timeframe]):
            log_and_print(f"Skipping record: programme_id={programme_id}, invalid programme, broker, or programme_timeframe", "DEBUG")
            return None

        if programme_id in self.processed_programme_ids:
            log_and_print(f"Skipping record: programme_id={programme_id}, already processed", "DEBUG")
            return None
        if not any(u['id'] == user_id for u in active_users):
            log_and_print(f"Skipping record: programme_id={programme_id}, user_id={user_id} not active", "DEBUG")
            return None

        if programme != 'bouncestream':
            log_and_print(f"Skipping record: programme_id={programme_id}, programme={programme} does not match bouncestream", "DEBUG")
            return None

        self.processed_programme_ids.add(programme_id)
        return {
            'programme_id': programme_id,
            'user_id': user_id,
            'subaccount_id': subaccount_id,
            'programme': programme,
            'broker': broker,
            'broker_loginid': broker_loginid,
            'broker_password': broker_password,
            'broker_server': broker_server,
            'programme_timeframe': programme_timeframe,
            'terminal_path': self.config.create_account_terminal(user_id, "sa" if subaccount_id else "ma")
        }

    async def fetch_user_programmes(self) -> Optional[List[Dict]]:
        """Fetch user programmes from the user_programmes table."""
        sql_query = """
            SELECT 
                u.id AS user_id, 
                u.account_status, 
                up.id AS programme_id, 
                up.user_id AS up_user_id, 
                up.subaccount_id, 
                up.programme, 
                up.broker,
                up.broker_server,
                up.broker_loginid,
                up.broker_password,
                up.programme_timeframe
            FROM 
                users u
            LEFT JOIN
                user_programmes up ON u.id = up.user_id
        """
        log_and_print(f"Sending query: {sql_query}", "INFO")
        
        for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
            try:
                result = db.execute_query(sql_query)
                log_and_print(f"Raw query result: {json.dumps(result, indent=2)}", "DEBUG")
                
                if not isinstance(result, dict):
                    log_and_print(f"Invalid result format on attempt {attempt}: Expected dict, got {type(result)}", "ERROR")
                    continue
                    
                if result.get('status') != 'success':
                    error_message = result.get('message', 'No message provided')
                    log_and_print(f"Query failed on attempt {attempt}: {error_message}", "ERROR")
                    continue
                    
                rows = None
                if 'data' in result and 'rows' in result['data'] and isinstance(result['data']['rows'], list):
                    rows = result['data']['rows']
                elif 'results' in result and isinstance(result['results'], list):
                    rows = result['results']
                else:
                    log_and_print(f"Invalid or missing rows in result on attempt {attempt}: {json.dumps(result, indent=2)}", "ERROR")
                    continue
                
                normalized_rows = [self.normalize_row(row) for row in rows]
                log_and_print(f"Fetched {len(normalized_rows)} rows from user_programmes", "SUCCESS")
                return normalized_rows
                
            except Exception as e:
                log_and_print(f"Exception on attempt {attempt}: {str(e)}", "ERROR")
                
            if attempt < RETRY_MAX_ATTEMPTS:
                delay = RETRY_DELAY * (2 ** (attempt - 1))
                log_and_print(f"Retrying after {delay} seconds...", "INFO")
                await asyncio.sleep(delay)
            else:
                log_and_print("Max retries reached for fetching user programmes", "ERROR")
                return None
        return None

    def get_timeframe(self, timeframe_str: str) -> Optional[int]:
        """Map signal timeframe string to MT5 timeframe constant."""
        timeframe_map = {
            '5minutes': mt5.TIMEFRAME_M5,
            '15minutes': mt5.TIMEFRAME_M15,
            '30minutes': mt5.TIMEFRAME_M30,
            '1hour': mt5.TIMEFRAME_H1,
            '4hour': mt5.TIMEFRAME_H4
        }
        return timeframe_map.get(timeframe_str.lower(), None)

    def load_account_json(self, file_path: str) -> List[Dict]:
        """Load existing JSON data for an account."""
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as file:
                    data = json.load(file)
                    return data if isinstance(data, list) else []
            except json.JSONDecodeError:
                log_and_print(f"Corrupted JSON file at {file_path}, starting fresh", "WARNING")
                return []
        return []

    def create_trade_record(self, position, mt5_instance) -> Dict:
        """Create a trade record structure from a position."""
        symbol_info = mt5_instance.symbol_info(position.symbol)
        point = symbol_info.point if symbol_info else 0.00001
        is_buy = position.type == mt5_instance.ORDER_TYPE_BUY
        entry_price = position.price_open
        # Estimate ratio prices if not in signals
        ratio_0_25_price = entry_price * (1 + SL_RR_05_PERCENT / 100) if is_buy else entry_price * (1 - SL_RR_05_PERCENT / 100)
        ratio_0_5_price = entry_price * (1 + 2 * SL_RR_05_PERCENT / 100) if is_buy else entry_price * (1 - 2 * SL_RR_05_PERCENT / 100)
        ratio_1_price = entry_price * (1 + 4 * SL_RR_05_PERCENT / 100) if is_buy else entry_price * (1 - 4 * SL_RR_05_PERCENT / 100)
        ratio_2_price = entry_price * (1 + 8 * SL_RR_05_PERCENT / 100) if is_buy else entry_price * (1 - 8 * SL_RR_05_PERCENT / 100)

        return {
            "pair": position.symbol.lower(),
            "order_type": "buy_limit" if is_buy else "sell_limit",
            "entry_price": str(entry_price),
            "ratio_0_25_price": str(ratio_0_25_price),
            "ratio_0_5_price": str(ratio_0_5_price),
            "ratio_1_price": str(ratio_1_price),
            "ratio_2_price": str(ratio_2_price),
            "timeframe": "1hour",  # Default timeframe, adjust as needed
            "ticket": position.ticket,
            "open_time": datetime.fromtimestamp(position.time, pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00')
        }

    def create_limit_order_record(self, order, mt5_instance) -> Dict:
        """Create a limit order record structure from an order."""
        is_buy = order.type in [mt5_instance.ORDER_TYPE_BUY_LIMIT, mt5_instance.ORDER_TYPE_BUY_STOP]
        return {
            "pair": order.symbol.lower(),
            "order_type": "buy_limit" if is_buy else "sell_limit",
            "entry_price": str(order.price_open),
            "timeframe": "1hour",  # Default timeframe, adjust as needed
            "ticket": order.ticket,
            "open_time": datetime.fromtimestamp(order.time_setup, pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00')
        }

        # Add this helper method to the TradeRegulator class
    
        # New method to add to the TradeRegulator class
    
    def is_market_open(self, mt5_instance, symbol: str) -> bool:
        """Check if the market is open for the given symbol based on recent tick data."""
        try:
            tick = mt5_instance.symbol_info_tick(symbol)
            if tick is None:
                log_and_print(f"No tick data available for {symbol}, assuming market closed", "DEBUG")
                return False

            # Get tick time in UTC
            tick_datetime = datetime.fromtimestamp(tick.time, tz=timezone.utc)
            now_utc = datetime.now(tz=timezone.utc)

            # If tick is older than 5 minutes, consider market closed
            time_diff = (now_utc - tick_datetime).total_seconds()
            if time_diff > 300:  # 5 minutes threshold
                log_and_print(f"Last tick for {symbol} is {time_diff:.0f}s old, assuming market closed", "DEBUG")
                return False

            log_and_print(f"Market open for {symbol} (last tick: {tick_datetime})", "DEBUG")
            return True
        except Exception as e:
            log_and_print(f"Error checking market status for {symbol}: {str(e)}", "WARNING")
            return False

    # Updated helper method in the TradeRegulator class
    async def cancel_order(self, mt5_instance, ticket: int, symbol: str) -> bool:
        """Cancel a pending order by ticket, only if market is open."""
        if not self.is_market_open(mt5_instance, symbol):
            log_and_print(f"Market closed for {symbol}, skipping cancel for order {ticket}", "INFO")
            return False

        try:
            request = {
                "action": mt5_instance.TRADE_ACTION_REMOVE,
                "order": ticket,
                "symbol": symbol
            }
            result = mt5_instance.order_send(request)
            if result.retcode == mt5_instance.TRADE_RETCODE_DONE:
                log_and_print(f"Successfully canceled order {ticket} for {symbol}", "SUCCESS")
                return True
            else:
                if result.retcode == 10018:  # TRADE_RETCODE_MARKET_CLOSED
                    log_and_print(f"Market closed for {symbol}, cannot cancel order {ticket}", "INFO")
                else:
                    log_and_print(f"Failed to cancel order {ticket} for {symbol}: {result.retcode} - {result.comment}", "ERROR")
                return False
        except Exception as e:
            log_and_print(f"Exception while canceling order {ticket} for {symbol}: {str(e)}", "ERROR")
            return False
    
    # Add this main method to the TradeRegulator class
    async def remove_duplicate_limit_orders(self, limit_orders: List[Dict], mt5_instance) -> List[Dict]:
        """Remove duplicate or too-close limit orders of the same type for the same pair.
        - Groups by (pair, order_type).
        - For sell_limit: Sort entries ascending (low to high). Keep first, remove subsequent if entry <= prev_sl or too close to prev_entry.
        - For buy_limit: Sort entries descending (high to low). Keep first, remove subsequent if entry >= prev_sl or too close to prev_entry.
        - Potential SL calculated using SL_ADJUSTMENT_PERCENT as risk (initial breakeven adjustment).
        - Too close: distance < 2 * min_stops_level (absolute price distance).
        - Cancels the actual orders in MT5 and filters the list.
        """
        if not limit_orders:
            return []

        available_symbols = self.get_available_symbols(mt5_instance)
        groups = defaultdict(list)
        for order in limit_orders:
            key = (order['pair'].lower(), order['order_type'].lower())
            groups[key].append(order)

        kept_orders = []
        risk_percent = SL_ADJUSTMENT_PERCENT / 100.0  # e.g., 0.001 for 0.10%
        min_close_factor = 2  # Too close if dist < min_stops_level * min_close_factor

        for (pair, order_type), group in groups.items():
            if len(group) <= 1:
                kept_orders.extend(group)
                continue

            server_symbol = self.get_exact_symbol_match(pair, available_symbols)
            if not server_symbol:
                log_and_print(f"Skipping deduplication for {pair} ({order_type}): No symbol match", "WARNING")
                kept_orders.extend(group)
                continue

            if not self.select_symbol(mt5_instance, server_symbol):
                log_and_print(f"Skipping deduplication for {pair} ({order_type}): Failed to select symbol", "WARNING")
                kept_orders.extend(group)
                continue

            # Add check for market open before processing group
            if not self.is_market_open(mt5_instance, server_symbol):
                log_and_print(f"Market closed for {server_symbol}, skipping deduplication for {pair} ({order_type})", "INFO")
                kept_orders.extend(group)
                continue

            symbol_info = mt5_instance.symbol_info(server_symbol)
            if not symbol_info:
                log_and_print(f"Skipping deduplication for {pair} ({order_type}): No symbol info", "WARNING")
                kept_orders.extend(group)
                continue

            point = symbol_info.point
            tick_size = symbol_info.trade_tick_size
            min_stops_dist = symbol_info.trade_stops_level * point
            min_close_dist = min_stops_dist * min_close_factor

            is_buy_limit = order_type == 'buy_limit'
            # Sort: ascending for sell (low entry first), descending for buy (high entry first)
            group.sort(key=lambda x: float(x['entry_price']), reverse=is_buy_limit)

            kept = [group[0]]  # Always keep the first (closest to current price directionally)
            log_and_print(f"Processing {len(group)} {order_type} orders for {pair} ({server_symbol})", "INFO")

            for candidate in group[1:]:
                entry = float(candidate['entry_price'])
                prev = kept[-1]
                prev_entry = float(prev['entry_price'])

                # Calculate prev_sl (potential initial SL for breakeven adjustment)
                if is_buy_limit:
                    prev_sl = prev_entry * (1 - risk_percent)
                else:
                    prev_sl = prev_entry * (1 + risk_percent)
                prev_sl = round(prev_sl / tick_size) * tick_size

                # Check too close to previous entry
                dist_to_prev = abs(entry - prev_entry)
                if dist_to_prev < min_close_dist:
                    log_and_print(f"Too close entry for {candidate['ticket']} ({pair}): dist {dist_to_prev:.5f} < {min_close_dist:.5f} to prev {prev_entry}", "INFO")
                    await self.cancel_order(mt5_instance, candidate['ticket'], server_symbol)
                    continue

                # Check against prev_sl (risk zone overlap)
                remove_due_to_risk = False
                if is_buy_limit:
                    # For buy: remove if not below prev_sl (i.e., entry >= prev_sl)
                    if entry >= prev_sl:
                        remove_due_to_risk = True
                else:
                    # For sell: remove if not above prev_sl (i.e., entry <= prev_sl)
                    if entry <= prev_sl:
                        remove_due_to_risk = True

                if remove_due_to_risk:
                    reason = "within previous order's risk zone (entry not sufficiently beyond SL)"
                    log_and_print(f"Removing {candidate['ticket']} ({pair}) {order_type}: {reason} (entry {entry}, prev_sl {prev_sl})", "INFO")
                    await self.cancel_order(mt5_instance, candidate['ticket'], server_symbol)
                    continue

                # All checks passed
                kept.append(candidate)
                log_and_print(f"Kept {candidate['ticket']} ({pair}) {order_type}: entry {entry} clear of prev_sl {prev_sl} and dist {dist_to_prev:.5f}", "DEBUG")

            kept_orders.extend(kept)
            removed_count = len(group) - len(kept)
            if removed_count > 0:
                log_and_print(f"Deduplicated {order_type} for {pair}: removed {removed_count}, kept {len(kept)}", "SUCCESS")

        log_and_print(f"Overall limit order deduplication: {len(limit_orders) - len(kept_orders)} removed", "INFO")
        return kept_orders

    def match_trade_details(self, order1: Dict, order2: Dict, available_symbols: List[str], tolerance: float = 0.0001) -> bool:
        """Check if two orders/trades match by pair, type, and entry price within tolerance."""
        if order1.get('order_type', '').lower() != order2.get('order_type', '').lower():
            return False
        
        sym1 = self.get_exact_symbol_match(order1['pair'].lower(), available_symbols)
        sym2 = self.get_exact_symbol_match(order2['pair'].lower(), available_symbols)
        if not (sym1 and sym2 and sym1.lower() == sym2.lower()):
            return False
        
        try:
            entry1 = float(order1['entry_price'])
            entry2 = float(order2['entry_price'])
            return abs(entry1 - entry2) < tolerance
        except (ValueError, KeyError):
            return False
        
    # Full Updated manage_trades_and_orders Method in TradeRegulator Class
    async def manage_trades_and_orders(self, account: Dict, mt5_instance) -> tuple[int, int]:
        """Manage running trades and pending orders, syncing with JSON files without duplicates."""
        account_key = f"user_{account['user_id']}_sub_{account['subaccount_id']}" if account['subaccount_id'] else f"user_{account['user_id']}"
        running_trades_file = os.path.join(self.config.running_trades_dir, f"{account_key}_runningtrades.json")
        closed_trades_file = os.path.join(self.config.closed_trades_dir, f"{account_key}_closedtrades.json")
        limit_orders_file = os.path.join(self.config.limit_orders_dir, f"{account_key}_limitorders.json")

        # Load existing JSON data
        running_trades = self.load_account_json(running_trades_file)
        closed_trades = self.load_account_json(closed_trades_file)
        limit_orders = self.load_account_json(limit_orders_file)

        # Fetch available symbols
        available_symbols = self.get_available_symbols(mt5_instance)

        # Fetch current positions and orders from MT5
        positions = mt5_instance.positions_get()
        orders = mt5_instance.orders_get()
        if not positions:
            positions = []
        if not orders:
            orders = []

        # Track tickets in JSON files
        running_trade_tickets = {trade['ticket'] for trade in running_trades}
        closed_trade_tickets = {trade['ticket'] for trade in closed_trades}
        limit_order_tickets = {order['ticket'] for order in limit_orders}

        current_order_tickets = {order.ticket for order in orders}

        # Ensure no running or closed trade tickets are in limit orders (ticket-based, for safety)
        filtered_limit_orders_by_ticket = [
            order for order in limit_orders
            if order['ticket'] not in running_trade_tickets and order['ticket'] not in closed_trade_tickets
        ]
        if len(filtered_limit_orders_by_ticket) < len(limit_orders):
            removed_count = len(limit_orders) - len(filtered_limit_orders_by_ticket)
            log_and_print(f"Removed {removed_count} limit orders with tickets matching running or closed trades for {account_key}", "INFO")
            limit_orders = filtered_limit_orders_by_ticket
            limit_order_tickets = {order['ticket'] for order in limit_orders}

        # Find and cancel stale limit orders that match running or closed trades by details
        all_historical_trades = running_trades + closed_trades
        stale_limit_tickets = set()
        canceled_count = 0
        for lo in limit_orders:
            if lo['ticket'] in current_order_tickets:  # Only if still pending in MT5
                for ht in all_historical_trades:
                    if self.match_trade_details(lo, ht, available_symbols):
                        server_symbol = self.get_exact_symbol_match(lo['pair'], available_symbols)
                        if server_symbol:
                            if self.select_symbol(mt5_instance, server_symbol):
                                if await self.cancel_order(mt5_instance, lo['ticket'], server_symbol):
                                    log_and_print(f"Canceled stale limit order {lo['ticket']} for {lo['pair']} (matches historical trade {ht['ticket']})", "SUCCESS")
                                    canceled_count += 1
                                else:
                                    log_and_print(f"Failed to cancel stale limit order {lo['ticket']} for {lo['pair']}", "WARNING")
                            else:
                                log_and_print(f"Failed to select symbol for canceling stale limit order {lo['ticket']}", "WARNING")
                        stale_limit_tickets.add(lo['ticket'])
                        break  # One match per limit order

        if canceled_count > 0:
            log_and_print(f"Canceled {canceled_count} stale limit orders matching historical trades for {account_key}", "INFO")

        # Filter limit orders to remove matched stale ones
        filtered_limit_orders = [lo for lo in limit_orders if lo['ticket'] not in stale_limit_tickets]
        limit_orders = filtered_limit_orders
        limit_order_tickets = {order['ticket'] for order in limit_orders}

        # Process running trades
        current_tickets = {position.ticket for position in positions}
        new_running_trades = []
        trades_to_close = []
        signals_to_remove = []  # Indices of signals to remove after processing

        for position in positions:
            json_symbol = position.symbol.lower()
            position_id = position.ticket
            order_type = position.type
            entry_price = position.price_open

            if order_type not in [mt5_instance.ORDER_TYPE_BUY, mt5_instance.ORDER_TYPE_SELL]:
                log_and_print(f"Skipping position {position_id} for {json_symbol}: Not a buy or sell order (type: {order_type})", "DEBUG")
                continue

            # Validate and match symbol
            server_symbol = self.get_exact_symbol_match(json_symbol, available_symbols)
            if not server_symbol:
                error_message = f"No exact symbol match for '{json_symbol}' in MT5 server symbols"
                log_and_print(error_message, "ERROR")
                self.save_adjustment_error(account_key, json_symbol, position_id, error_message)
                continue

            # Ensure symbol is selected in Market Watch
            if not self.select_symbol(mt5_instance, server_symbol):
                error_message = f"Failed to select symbol '{server_symbol}' in Market Watch"
                log_and_print(error_message, "ERROR")
                self.save_adjustment_error(account_key, json_symbol, position_id, error_message)
                continue

            # Skip if already in running trades or closed trades
            if position_id in running_trade_tickets:
                existing_trade = next((trade for trade in running_trades if trade['ticket'] == position_id), None)
                if existing_trade:
                    new_running_trades.append(existing_trade)
                continue
            if position_id in closed_trade_tickets:
                log_and_print(f"Position {position_id} for {server_symbol} found in closed trades, skipping", "DEBUG")
                continue

            is_buy = order_type == mt5_instance.ORDER_TYPE_BUY
            expected_order_type = 'buy_limit' if is_buy else 'sell_limit'

            # Find matching signal in bouncestreamsignals.json
            matching_signal = None
            matching_index = None
            for i, signal in enumerate(self.signals):
                signal_symbol = self.get_exact_symbol_match(signal['pair'].lower(), available_symbols)
                if (signal_symbol and signal_symbol.lower() == server_symbol.lower() and 
                    signal['order_type'].lower() == expected_order_type and 
                    abs(float(signal['entry_price']) - entry_price) < 0.0001):
                    matching_signal = signal
                    matching_index = i
                    break

            if matching_signal:
                trade_record = matching_signal.copy()
                trade_record['pair'] = server_symbol.lower()  # Use normalized symbol
                trade_record['ticket'] = position_id
                trade_record['open_time'] = datetime.fromtimestamp(position.time, pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00')
                new_running_trades.append(trade_record)
                log_and_print(f"Added position {position_id} for {server_symbol} to running trades", "INFO")
                if matching_index is not None:
                    signals_to_remove.append(matching_index)
            else:
                # For orphan positions, create record without adding to signals
                trade_record = self.create_trade_record(position, mt5_instance)
                trade_record['pair'] = server_symbol.lower()  # Use normalized symbol
                new_running_trades.append(trade_record)
                log_and_print(f"Added orphan position {position_id} for {server_symbol} to running trades (no signal match)", "INFO")

        # Remove matched signals for running trades
        for i in sorted(signals_to_remove, reverse=True):
            del self.signals[i]
        signals_to_remove = []  # Reset for orders

        # Check for stray running trades (limited to last 3 days)
        three_days_ago = datetime.now(pytz.timezone('Africa/Lagos')) - timedelta(days=3)
        for trade in running_trades:
            if trade['ticket'] not in current_tickets and trade['ticket'] not in closed_trade_tickets:
                history = mt5_instance.history_deals_get(position=trade['ticket'], date_from=three_days_ago)
                if history and any(d.entry == mt5_instance.DEAL_ENTRY_OUT for d in history):
                    closed_trade = trade.copy()
                    closed_trade['close_time'] = datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00')
                    trades_to_close.append(closed_trade)
                    log_and_print(f"Moved trade {trade['ticket']} for {trade['pair']} to closed trades (closure in last 3 days)", "INFO")
                else:
                    new_running_trades.append(trade)

        # Save updated running and closed trades
        self.save_to_json(running_trades_file, new_running_trades, append=False)
        if trades_to_close:
            existing_closed_trades = self.load_account_json(closed_trades_file)
            existing_closed_tickets = {trade['ticket'] for trade in existing_closed_trades}
            unique_trades_to_close = [trade for trade in trades_to_close if trade['ticket'] not in existing_closed_tickets]
            if unique_trades_to_close:
                self.save_to_json(closed_trades_file, unique_trades_to_close)
            else:
                log_and_print(f"No new closed trades to add for {account_key}", "DEBUG")

        # Process pending orders
        new_limit_orders = []

        for order in orders:
            json_symbol = order.symbol.lower()
            order_id = order.ticket
            order_type = order.type

            if order_type not in [mt5_instance.ORDER_TYPE_BUY_LIMIT, mt5_instance.ORDER_TYPE_SELL_LIMIT,
                                mt5_instance.ORDER_TYPE_BUY_STOP, mt5_instance.ORDER_TYPE_SELL_STOP]:
                log_and_print(f"Skipping order {order_id} for {json_symbol}: Not a limit or stop order (type: {order_type})", "DEBUG")
                continue

            # Validate and match symbol
            server_symbol = self.get_exact_symbol_match(json_symbol, available_symbols)
            if not server_symbol:
                error_message = f"No exact symbol match for '{json_symbol}' in MT5 server symbols"
                log_and_print(error_message, "ERROR")
                self.save_adjustment_error(account_key, json_symbol, order_id, error_message)
                continue

            # Ensure symbol is selected in Market Watch
            if not self.select_symbol(mt5_instance, server_symbol):
                error_message = f"Failed to select symbol '{server_symbol}' in Market Watch"
                log_and_print(error_message, "ERROR")
                self.save_adjustment_error(account_key, json_symbol, order_id, error_message)
                continue

            # Skip if already in limit orders
            if order_id in limit_order_tickets:
                existing_order = next((order_ for order_ in limit_orders if order_['ticket'] == order_id), None)
                if existing_order:
                    new_limit_orders.append(existing_order)
                continue

            is_buy = order_type in [mt5_instance.ORDER_TYPE_BUY_LIMIT, mt5_instance.ORDER_TYPE_BUY_STOP]
            expected_order_type = 'buy_limit' if is_buy else 'sell_limit'

            # Find matching signal in bouncestreamsignals.json
            matching_signal = None
            matching_index = None
            for i, signal in enumerate(self.signals):
                signal_symbol = self.get_exact_symbol_match(signal['pair'].lower(), available_symbols)
                if (signal_symbol and signal_symbol.lower() == server_symbol.lower() and 
                    signal['order_type'].lower() == expected_order_type and 
                    abs(float(signal['entry_price']) - order.price_open) < 0.0001):
                    matching_signal = signal
                    matching_index = i
                    break

            if matching_signal:
                order_record = matching_signal.copy()
                order_record['pair'] = server_symbol.lower()  # Use normalized symbol
                order_record['ticket'] = order_id
                order_record['open_time'] = datetime.fromtimestamp(order.time_setup, pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00')
                new_limit_orders.append(order_record)
                log_and_print(f"Added limit order {order_id} for {server_symbol} to limit orders", "INFO")
                if matching_index is not None:
                    signals_to_remove.append(matching_index)
            else:
                # For orphan orders, create record without adding to signals
                order_record = self.create_limit_order_record(order, mt5_instance)
                order_record['pair'] = server_symbol.lower()  # Use normalized symbol
                new_limit_orders.append(order_record)
                log_and_print(f"Added orphan limit order {order_id} for {server_symbol} to limit orders (no signal match)", "INFO")

        # Remove matched signals for limit orders
        for i in sorted(signals_to_remove, reverse=True):
            del self.signals[i]

        # New: Remove duplicates and too-close limit orders before handling expirations
        new_limit_orders = await self.remove_duplicate_limit_orders(new_limit_orders, mt5_instance)

        # Remove expired or canceled limit orders
        for order in limit_orders:
            if order['ticket'] not in current_order_tickets:
                log_and_print(f"Removed expired/canceled limit order {order['ticket']} for {order['pair']} from limit orders", "INFO")
                continue
            new_limit_orders.append(order)

        # Save updated limit orders
        self.save_to_json(limit_orders_file, new_limit_orders, append=False)

        # Save updated signals file after removals
        try:
            signals_data = {"orders": self.signals}
            with open(SIGNALS_FILE, 'w', encoding='utf-8') as file:
                json.dump(signals_data, file, indent=4)
            log_and_print(f"Updated {SIGNALS_FILE} with {len(self.signals)} signals after processing {account_key}", "INFO")
        except Exception as e:
            log_and_print(f"Error saving updated signals to {SIGNALS_FILE}: {str(e)}", "ERROR")

        return len(new_running_trades), len(new_limit_orders)

    # Full Updated regulate_trades Method in TradeRegulator Class
    async def regulate_trades(self, account: Dict) -> tuple[int, int]:
        """Regulate stop-loss for running market orders based on signals and timeframe."""
        account_key = f"user_{account['user_id']}_sub_{account['subaccount_id']}" if account['subaccount_id'] else f"user_{account['user_id']}"
        log_and_print(f"===== Regulating Trades for {account_key} =====", "TITLE")
        
        # FIX: Re-initialize connection for this specific account/terminal before processing
        if not self.mt5_manager.initialize_mt5(
            server=account['broker_server'],
            login=account['broker_loginid'],
            password=account['broker_password'],
            terminal_path=account['terminal_path'],
            account_key=account_key
        ):
            log_and_print(f"Failed to re-initialize MT5 for {account_key}, skipping", "ERROR")
            return 0, 0

        # Use global mt5 directly (now connected to this account's terminal)
        mt5_instance = mt5

        # Manage trades and orders first
        running_trades_count, limit_orders_count = await self.manage_trades_and_orders(account, mt5_instance)
        log_and_print(f"Managed {running_trades_count} running trades and {limit_orders_count} limit orders for {account_key}", "INFO")

        running_trades_file = os.path.join(self.config.running_trades_dir, f"{account_key}_runningtrades.json")
        running_trades = self.load_account_json(running_trades_file)

        adjusted_orders = 0
        failed_adjustments = 0

        # Fetch available symbols
        available_symbols = self.get_available_symbols(mt5_instance)

        try:
            positions = mt5_instance.positions_get()
            if not positions:
                log_and_print(f"No open positions found for {account_key}", "INFO")
                return 0, 0

            log_and_print(f"Found {len(positions)} open positions for {account_key}", "INFO")

            for position in positions:
                try:
                    json_symbol = position.symbol.lower()
                    position_id = position.ticket
                    order_type = position.type
                    entry_price = position.price_open
                    current_sl = position.sl
                    current_tp = position.tp

                    if order_type not in [mt5_instance.ORDER_TYPE_BUY, mt5_instance.ORDER_TYPE_SELL]:
                        log_and_print(f"Skipping position {position_id} for {json_symbol}: Not a buy or sell order (type: {order_type})", "DEBUG")
                        continue

                    # Validate and match symbol
                    server_symbol = self.get_exact_symbol_match(json_symbol, available_symbols)
                    if not server_symbol:
                        error_message = f"No exact symbol match for '{json_symbol}' in MT5 server symbols"
                        log_and_print(error_message, "ERROR")
                        self.save_adjustment_error(account_key, json_symbol, position_id, error_message)
                        failed_adjustments += 1
                        self.total_orders_failed += 1
                        continue

                    # Ensure symbol is selected in Market Watch
                    if not self.select_symbol(mt5_instance, server_symbol):
                        error_message = f"Failed to select symbol '{server_symbol}' in Market Watch"
                        log_and_print(error_message, "ERROR")
                        self.save_adjustment_error(account_key, json_symbol, position_id, error_message)
                        failed_adjustments += 1
                        self.total_orders_failed += 1
                        continue

                    is_buy = order_type == mt5_instance.ORDER_TYPE_BUY
                    is_sell = order_type == mt5_instance.ORDER_TYPE_SELL
                    expected_order_type = 'buy_limit' if is_buy else 'sell_limit'

                    # Find matching signal in running_trades JSON
                    matching_signal = None
                    for trade in running_trades:
                        trade_symbol = self.get_exact_symbol_match(trade['pair'].lower(), available_symbols)
                        if (trade_symbol and trade_symbol.lower() == server_symbol.lower() and 
                            trade['order_type'].lower() == expected_order_type and 
                            trade['ticket'] == position_id):
                            matching_signal = trade
                            break

                    if not matching_signal:
                        log_and_print(f"No matching running trade found for position {position_id} ({server_symbol}, entry: {entry_price})", "DEBUG")
                        continue

                    timeframe_str = matching_signal['timeframe']
                    timeframe = self.get_timeframe(timeframe_str)
                    if not timeframe:
                        error_message = f"Invalid timeframe '{timeframe_str}' for position {position_id} ({server_symbol})"
                        log_and_print(error_message, "ERROR")
                        self.save_adjustment_error(account_key, server_symbol, position_id, error_message)
                        failed_adjustments += 1
                        self.total_orders_failed += 1
                        continue

                    symbol_info = mt5_instance.symbol_info(server_symbol)
                    if not symbol_info:
                        error_message = f"Cannot retrieve symbol info for {server_symbol}"
                        log_and_print(error_message, "ERROR")
                        self.save_adjustment_error(account_key, server_symbol, position_id, error_message)
                        failed_adjustments += 1
                        self.total_orders_failed += 1
                        continue

                    tick = mt5_instance.symbol_info_tick(server_symbol)
                    if not tick:
                        error_message = f"Cannot retrieve tick data for {server_symbol}"
                        log_and_print(error_message, "ERROR")
                        self.save_adjustment_error(account_key, server_symbol, position_id, error_message)
                        failed_adjustments += 1
                        self.total_orders_failed += 1
                        continue

                    current_price = tick.bid if is_sell else tick.ask
                    tick_size = symbol_info.trade_tick_size
                    point = symbol_info.point
                    min_sl_distance = symbol_info.trade_stops_level * point

                    # FIX: Add buffer for synthetic indices (e.g., Drift Switch)
                    if 'drift' in server_symbol.lower() or 'synthetic' in server_symbol.lower():
                        min_sl_distance *= 1.2  # 20% extra buffer for high-vol synthetics
                        log_and_print(f"Synthetic index detected ({server_symbol}): Using buffered min SL distance {min_sl_distance}", "DEBUG")

                    log_and_print(f"Min SL distance required: {min_sl_distance}, Current price: {current_price}, Entry: {entry_price}", "DEBUG")

                    entry_price_signal = float(matching_signal['entry_price'])
                    ratio_0_5_price = float(matching_signal['ratio_0_5_price'])
                    ratio_1_price = float(matching_signal['ratio_1_price'])
                    ratio_2_price = float(matching_signal['ratio_2_price'])
                    ratio_0_25_price = float(matching_signal.get('ratio_0_25_price', 
                                                                entry_price_signal * (1 + SL_RR_05_PERCENT / 100) if is_buy else 
                                                                entry_price_signal * (1 - SL_RR_05_PERCENT / 100)))

                    new_sl = 0.0
                    adjustment_needed = False
                    adjustment_reason = ""
                    eligible_ratio = None
                    towards_threshold = None  # For descriptive messaging

                    # Determine price direction and eligibility for adjustment using current_price
                    is_price_toward_profit = (is_buy and current_price > entry_price_signal) or (is_sell and current_price < entry_price_signal)
                    is_price_toward_stoploss = (is_buy and current_price <= entry_price_signal) or (is_sell and current_price >= entry_price_signal)

                    if is_price_toward_stoploss:
                        if current_sl != 0.0:
                            # Check if between SL and entry
                            if is_buy:
                                if current_sl < current_price < entry_price_signal:
                                    dist_to_sl = current_price - current_sl
                                    dist_to_entry = entry_price_signal - current_price
                                    closer_to = "stop-loss" if dist_to_sl < dist_to_entry else "entry"
                                    log_and_print(f"Current price {current_price} is between stop-loss {current_sl} and entry {entry_price_signal}, "
                                                f"closer to {closer_to}, waiting for price to move {'above' if is_buy else 'below'} entry towards eligible ratios", "INFO")
                                    continue
                            else:  # sell
                                if entry_price_signal < current_price < current_sl:
                                    dist_to_entry = current_price - entry_price_signal
                                    dist_to_sl = current_sl - current_price
                                    closer_to = "entry" if dist_to_entry < dist_to_sl else "stop-loss"
                                    log_and_print(f"Current price {current_price} is between entry {entry_price_signal} and stop-loss {current_sl}, "
                                                f"closer to {closer_to}, waiting for price to move {'below' if is_sell else 'above'} entry towards eligible ratios", "INFO")
                                    continue
                        direction_word = "above" if is_sell else "below"
                        log_and_print(f"No stop-loss adjustment for position {position_id} ({server_symbol}): "
                                    f"Current price {current_price} is {direction_word} entry {entry_price_signal}, moving toward stop-loss, "
                                    f"waiting for price to move {'below' if is_sell else 'above'} entry toward ratios", "INFO")
                        continue

                    # Adjustment logic using current_price for ratio checks
                    direction_word = "above" if is_buy else "below"
                    opposite_direction_word = "below" if is_buy else "above"
                    if is_buy:
                        if current_price > ratio_2_price:
                            new_sl = round(ratio_1_price / tick_size) * tick_size
                            eligible_ratio = "1:2 RR"
                            towards_threshold = "1:1 RR"  # Already beyond 1:2, but for messaging if needed
                            if current_sl == 0.0 or (current_sl > 0 and current_sl < new_sl):
                                adjustment_needed = True
                                adjustment_reason = f"{direction_word} 1:2 RR (current: {current_price}, ratio_2: {ratio_2_price})"
                        elif current_price > ratio_1_price:
                            new_sl = round(ratio_0_5_price / tick_size) * tick_size
                            eligible_ratio = "1:1 RR"
                            towards_threshold = "1:2 RR"
                            if current_sl == 0.0 or (current_sl > 0 and current_sl < new_sl):
                                adjustment_needed = True
                                adjustment_reason = f"{direction_word} 1:1 RR (current: {current_price}, ratio_1: {ratio_1_price})"
                        elif current_price > ratio_0_5_price:
                            new_sl = round(ratio_0_25_price / tick_size) * tick_size
                            eligible_ratio = "1:0.5 RR"
                            towards_threshold = "1:1 RR"
                            if current_sl == 0.0 or (current_sl > 0 and current_sl < new_sl):
                                adjustment_needed = True
                                adjustment_reason = f"{direction_word} 1:0.5 RR (current: {current_price}, ratio_0_5: {ratio_0_5_price})"
                        elif current_price > entry_price_signal:
                            new_sl = round((entry_price * (1 + SL_ADJUSTMENT_PERCENT / 100)) / tick_size) * tick_size
                            eligible_ratio = "entry price"
                            towards_threshold = "1:0.5 RR"
                            if current_sl == 0.0 or (current_sl > 0 and current_sl < new_sl):
                                adjustment_needed = True
                                adjustment_reason = f"{direction_word} entry price (current: {current_price}, entry: {entry_price_signal})"

                    # Adjustment logic for sell orders (price below entry, moving toward profit)
                    elif is_sell:
                        if current_price < ratio_2_price:
                            new_sl = round(ratio_1_price / tick_size) * tick_size
                            eligible_ratio = "1:2 RR"
                            towards_threshold = "1:1 RR"
                            if current_sl == 0.0 or (current_sl > 0 and current_sl > new_sl):
                                adjustment_needed = True
                                adjustment_reason = f"{direction_word} 1:2 RR (current: {current_price}, ratio_2: {ratio_2_price})"
                        elif current_price < ratio_1_price:
                            new_sl = round(ratio_0_5_price / tick_size) * tick_size
                            eligible_ratio = "1:1 RR"
                            towards_threshold = "1:2 RR"
                            if current_sl == 0.0 or (current_sl > 0 and current_sl > new_sl):
                                adjustment_needed = True
                                adjustment_reason = f"{direction_word} 1:1 RR (current: {current_price}, ratio_1: {ratio_1_price})"
                        elif current_price < ratio_0_5_price:
                            new_sl = round(ratio_0_25_price / tick_size) * tick_size
                            eligible_ratio = "1:0.5 RR"
                            towards_threshold = "1:1 RR"
                            if current_sl == 0.0 or (current_sl > 0 and current_sl > new_sl):
                                adjustment_needed = True
                                adjustment_reason = f"{direction_word} 1:0.5 RR (current: {current_price}, ratio_0_5: {ratio_0_5_price})"
                        elif current_price < entry_price_signal:
                            new_sl = round((entry_price * (1 - SL_ADJUSTMENT_PERCENT / 100)) / tick_size) * tick_size
                            eligible_ratio = "entry price"
                            towards_threshold = "1:0.5 RR"
                            if current_sl == 0.0 or (current_sl > 0 and current_sl > new_sl):
                                adjustment_needed = True
                                adjustment_reason = f"{direction_word} entry price (current: {current_price}, entry: {entry_price_signal})"

                    if not adjustment_needed:
                        log_and_print(f"No stop-loss adjustment needed for position {position_id} ({server_symbol})", "DEBUG")
                        continue

                    # FIX: Enhanced validation with logging
                    actual_distance = abs(current_price - new_sl)
                    log_and_print(f"Proposed SL: {new_sl}, Actual distance to current: {actual_distance}, Required min: {min_sl_distance}", "DEBUG")

                    # Validate stop-loss distance against current price (not entry) - stricter check
                    too_close = False
                    if is_buy:
                        if new_sl >= current_price - min_sl_distance:
                            too_close = True
                    else:  # sell
                        if new_sl <= current_price + min_sl_distance:
                            too_close = True

                    if too_close:
                        threshold_desc = towards_threshold if towards_threshold else "entry price"
                        eligible_desc = f"break-even { '+' if is_buy else '-' }{SL_ADJUSTMENT_PERCENT}%" if eligible_ratio == "entry price" else eligible_ratio
                        towards_desc = towards_threshold if towards_threshold else 'profit ratios'
                        further_direction = direction_word
                        log_and_print(f"Stop-loss {new_sl} too close to current price {current_price} for {server_symbol} "
                                    f"({ 'buy' if is_buy else 'sell' }, min distance: {min_sl_distance}, actual: {actual_distance}), current price {direction_word} {threshold_desc} "
                                    f"and towards {towards_desc}, "
                                    f"waiting for price to move further {further_direction} {eligible_desc} ({new_sl}) to adjust stop-loss", "INFO")
                        continue  # Do not increment failed_adjustments

                    request = {
                        "action": mt5_instance.TRADE_ACTION_SLTP,
                        "position": position_id,
                        "symbol": server_symbol,
                        "sl": new_sl,
                        "tp": current_tp
                    }

                    log_and_print(f"Attempting to adjust stop-loss for position {position_id} ({server_symbol}) to {new_sl} ({adjustment_reason})", "INFO")

                    sl_buffer = min_sl_distance  # Buffer for retries
                    for attempt in range(1, MAX_RETRIES + 1):
                        # FIX: On retry for 10016, widen SL slightly
                        adjusted_sl = new_sl
                        if attempt > 1:
                            if is_buy:
                                adjusted_sl += sl_buffer  # Move SL further up for buys
                            else:
                                adjusted_sl -= sl_buffer  # Move SL further down for sells
                            adjusted_sl = round(adjusted_sl / tick_size) * tick_size
                            log_and_print(f"Retry {attempt}: Widened SL to {adjusted_sl} (buffer: {sl_buffer})", "INFO")
                            request["sl"] = adjusted_sl

                        result = mt5_instance.order_send(request)
                        if result.retcode == mt5_instance.TRADE_RETCODE_DONE:
                            log_and_print(f"Successfully adjusted stop-loss for position {position_id} ({server_symbol}) to {adjusted_sl or new_sl} ({adjustment_reason})", "SUCCESS")
                            adjusted_orders += 1
                            self.total_orders_adjusted += 1
                            break
                        else:
                            error_code = result.retcode
                            error_message = result.comment
                            full_error = f"Failed to adjust stop-loss for position {position_id} ({server_symbol}) on attempt {attempt}: {error_code}, {error_message}"
                            log_and_print(full_error, "ERROR")
                            self.save_adjustment_error(account_key, server_symbol, position_id, full_error)
                            
                            # FIX: Handle 10016 as market condition - no failure count, specific log
                            if error_code == 10016:  # Invalid stops - too close due to market conditions
                                threshold_desc = towards_threshold if towards_threshold else "entry price"
                                eligible_desc = f"break-even { '+' if is_buy else '-' }{SL_ADJUSTMENT_PERCENT}%" if eligible_ratio == "entry price" else eligible_ratio
                                further_direction = direction_word
                                log_and_print(f"Too close to modify SL for {server_symbol} (market condition: invalid stops). Waiting for current price to move further {further_direction} toward profit for adjustable SL at {eligible_desc} level.", "INFO")
                                break  # Stop retries; respect market - don't count as failed
                            
                            if attempt == MAX_RETRIES:
                                if error_code != 10016:  # Only count non-distance errors as failed
                                    failed_adjustments += 1
                                    self.total_orders_failed += 1
                            else:
                                log_and_print(f"Retrying adjustment after {MT5_RETRY_DELAY} seconds...", "INFO")
                                await asyncio.sleep(MT5_RETRY_DELAY)

                except Exception as e:
                    error_message = f"Error processing position {position_id} for {server_symbol}: {str(e)}"
                    log_and_print(error_message, "ERROR")
                    self.save_adjustment_error(account_key, server_symbol, position_id, error_message)
                    failed_adjustments += 1
                    self.total_orders_failed += 1

        except Exception as e:
            error_message = f"Error retrieving positions for {account_key}: {str(e)}"
            log_and_print(error_message, "ERROR")
            self.save_adjustment_error(account_key, "N/A", 0, error_message)
            failed_adjustments += 1
            self.total_orders_failed += 1

        log_and_print(f"Regulation summary for {account_key}: {adjusted_orders} positions adjusted, {failed_adjustments} failed", "INFO")
        return adjusted_orders, failed_adjustments

    async def initialize_accounts(self) -> bool:
        """Initialize MT5 connections for all valid accounts and load signals."""
        if not self.config.validate_directory() or not self.config.validate_mt5_directory():
            log_and_print("Aborting due to invalid directory configuration", "ERROR")
            return False

        if not self.load_signals():
            log_and_print("Failed to load signals, aborting", "ERROR")
            return False

        active_users = await self.get_active_users()
        if not active_users:
            log_and_print("No active users found", "WARNING")
            return False

        programmes = await self.fetch_user_programmes()
        if not programmes:
            log_and_print("No user programmes fetched, aborting", "ERROR")
            return False

        total_programmes = len(programmes)
        skipped_records = 0
        accounts_initialized = 0

        for programme in programmes:
            validated = await self.validate_account(programme, active_users)
            if validated and validated['terminal_path']:
                account_key = f"user_{validated['user_id']}_sub_{validated['subaccount_id']}" if validated['subaccount_id'] else f"user_{validated['user_id']}"
                for attempt in range(1, MAX_RETRIES + 1):
                    if self.mt5_manager.initialize_mt5(
                        server=validated['broker_server'],
                        login=validated['broker_loginid'],
                        password=validated['broker_password'],
                        terminal_path=validated['terminal_path'],
                        account_key=account_key
                    ):
                        self.valid_accounts.append(validated)
                        accounts_initialized += 1
                        log_and_print(f"MT5 initialized for {account_key}", "SUCCESS")
                        break
                    else:
                        error_message = f"MT5 initialization failed for {account_key} on attempt {attempt}"
                        log_and_print(error_message, "ERROR")
                        if attempt == MAX_RETRIES:
                            log_and_print(f"Max retries reached for {account_key}, skipping", "ERROR")
                        else:
                            log_and_print(f"Retrying after {MT5_RETRY_DELAY} seconds...", "INFO")
                            await asyncio.sleep(MT5_RETRY_DELAY)
            else:
                skipped_records += 1

        log_and_print(f"Validated {len(self.valid_accounts)} out of {total_programmes} programme records, {accounts_initialized} accounts initialized", "SUCCESS")
        log_and_print(f"Skipped {skipped_records} records", "INFO")
        return len(self.valid_accounts) > 0

    async def regulation_loop(self):
        """Main loop to continuously regulate trades for all valid accounts."""
        log_and_print("===== Starting Trade Regulation Loop =====", "TITLE")
        cycle_count = 0
        while True:
            cycle_count += 1
            log_and_print(f"===== Regulation Cycle {cycle_count} =====", "TITLE")
            total_adjusted = 0
            total_failed = 0

            if not self.load_signals():
                log_and_print("Failed to reload signals, skipping cycle", "ERROR")
                await asyncio.sleep(CHECK_INTERVAL)
                continue

            for account in self.valid_accounts:
                account_key = f"user_{account['user_id']}_sub_{account['subaccount_id']}" if account['subaccount_id'] else f"user_{account['user_id']}"
                log_and_print(f"Processing account: {account_key}", "INFO")
                adjusted, failed = await self.regulate_trades(account)
                total_adjusted += adjusted
                total_failed += failed
                # Log per-account summary
                log_and_print(f"Account {account_key} Summary: {adjusted} positions adjusted, {failed} failed adjustments", "INFO")

            # Log cycle summary and totals
            log_and_print(f"Cycle {cycle_count} Summary: {total_adjusted} positions adjusted, {total_failed} failed adjustments", "INFO")
            log_and_print(f"Total Positions Adjusted (All Cycles): {self.total_orders_adjusted}", "INFO")
            log_and_print(f"Total Failed Adjustments (All Cycles): {self.total_orders_failed}", "INFO")
            log_and_print(f"Next check in {CHECK_INTERVAL} seconds...", "INFO")
            await asyncio.sleep(CHECK_INTERVAL)

async def main():
    """Main function to initialize accounts and start the trade regulation loop."""
    print("\n")
    log_and_print("===== Server Trade Regulation Started =====", "TITLE")
    
    regulator = TradeRegulator()
    
    if not await regulator.initialize_accounts():
        log_and_print("No valid accounts initialized or signals loaded, aborting", "ERROR")
        print("\n")
        return

    try:
        await regulator.regulation_loop()
    except KeyboardInterrupt:
        log_and_print("Regulation loop terminated by user", "INFO")
    finally:
        log_and_print("===== Server Trade Regulation Completed =====", "TITLE")
        print("\n")

if __name__ == "__main__":
    asyncio.run(main())