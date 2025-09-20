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
CHECK_INTERVAL = 30  # Seconds to wait between trade regulation checks
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

# MT5 Manager Class
class MT5Manager:
    """Manages MT5 initialization and login, maintaining connections without shutdown."""
    def __init__(self):
        self.mt5_instances = {}  # Dictionary to store MT5 instances per account_key

    def initialize_mt5(self, server: str, login: str, password: str, terminal_path: str, account_key: str) -> bool:
        """Initialize MT5 terminal and login with provided credentials using the specified terminal path."""
        log_and_print(f"Attempting MT5 login for {account_key} (server: {server}, login: {login}) using {terminal_path}", "INFO")
        
        if account_key not in self.mt5_instances:
            self.mt5_instances[account_key] = mt5

        mt5_instance = self.mt5_instances[account_key]

        try:
            if mt5_instance.initialize(
                path=terminal_path,
                login=int(login),
                password=password,
                server=server,
                portable=True,
                timeout=120000
            ):
                log_and_print(f"Successfully initialized MT5 terminal for {account_key}", "SUCCESS")
            else:
                error_code, error_message = mt5_instance.last_error()
                log_and_print(f"Failed to initialize MT5 terminal for {account_key}. Error: {error_code}, {error_message}", "ERROR")
                return False
        except Exception as e:
            log_and_print(f"Exception during MT5 initialization for {account_key}: {str(e)}", "ERROR")
            return False

        for _ in range(5):
            if mt5_instance.terminal_info() is not None:
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
        """Save data to a JSON file, optionally appending to existing data."""
        try:
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

    async def manage_trades_and_orders(self, account: Dict, mt5_instance) -> tuple[int, int]:
        """Manage running trades and pending orders, syncing with JSON files."""
        account_key = f"user_{account['user_id']}_sub_{account['subaccount_id']}" if account['subaccount_id'] else f"user_{account['user_id']}"
        running_trades_file = os.path.join(self.config.running_trades_dir, f"{account_key}_runningtrades.json")
        closed_trades_file = os.path.join(self.config.closed_trades_dir, f"{account_key}_closedtrades.json")
        limit_orders_file = os.path.join(self.config.limit_orders_dir, f"{account_key}_limitorders.json")

        running_trades = self.load_account_json(running_trades_file)
        closed_trades = self.load_account_json(closed_trades_file)
        limit_orders = self.load_account_json(limit_orders_file)

        # Fetch current positions and orders from MT5
        positions = mt5_instance.positions_get()
        orders = mt5_instance.orders_get()
        if not positions:
            positions = []
        if not orders:
            orders = []

        # Process running trades
        current_tickets = {position.ticket for position in positions}
        running_trade_tickets = {trade['ticket'] for trade in running_trades}
        new_running_trades = []
        trades_to_close = []

        for position in positions:
            symbol = position.symbol.lower()
            position_id = position.ticket
            order_type = position.type
            entry_price = position.price_open

            if order_type not in [mt5_instance.ORDER_TYPE_BUY, mt5_instance.ORDER_TYPE_SELL]:
                log_and_print(f"Skipping position {position_id} for {symbol}: Not a buy or sell order (type: {order_type})", "DEBUG")
                continue

            is_buy = order_type == mt5_instance.ORDER_TYPE_BUY
            expected_order_type = 'buy_limit' if is_buy else 'sell_limit'

            # Find matching signal in bouncestreamsignals.json
            matching_signal = None
            for signal in self.signals:
                if (signal['pair'].lower() == symbol and 
                    signal['order_type'].lower() == expected_order_type and 
                    abs(float(signal['entry_price']) - entry_price) < 0.0001):
                    matching_signal = signal
                    break

            if matching_signal:
                # Copy signal to running trades
                trade_record = matching_signal.copy()
                trade_record['ticket'] = position_id
                trade_record['open_time'] = datetime.fromtimestamp(position.time, pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00')
                new_running_trades.append(trade_record)
                log_and_print(f"Moved signal for {symbol} (ticket: {position_id}) to {running_trades_file}", "INFO")
            else:
                # No matching signal, create a new signal and add to bouncestream
                new_signal = self.create_trade_record(position, mt5_instance)
                if self.update_bouncestream_signals(new_signal):
                    new_running_trades.append(new_signal)
                    log_and_print(f"Created and moved new signal for {symbol} (ticket: {position_id}) to {running_trades_file}", "INFO")
                else:
                    log_and_print(f"Failed to update bouncestream signals for {symbol} (ticket: {position_id})", "ERROR")
                    continue

        # Check for stray running trades (not in current positions)
        for trade in running_trades:
            if trade['ticket'] not in current_tickets:
                # Check trade history to confirm if closed
                history = mt5_instance.history_deals_get(position=trade['ticket'])
                if history and any(deal.entry == mt5_instance.DEAL_ENTRY_OUT for deal in history):
                    closed_trade = trade.copy()
                    closed_trade['close_time'] = datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00')
                    trades_to_close.append(closed_trade)
                    log_and_print(f"Moved trade {trade['ticket']} for {trade['pair']} to {closed_trades_file} (closed)", "INFO")
                else:
                    new_running_trades.append(trade)  # Keep in running trades if not confirmed closed

        # Save updated running and closed trades
        self.save_to_json(running_trades_file, new_running_trades)
        if trades_to_close:
            self.save_to_json(closed_trades_file, trades_to_close)

        # Process pending orders
        current_order_tickets = {order.ticket for order in orders}
        limit_order_tickets = {order['ticket'] for order in limit_orders}
        new_limit_orders = []

        for order in orders:
            symbol = order.symbol.lower()
            order_id = order.ticket
            order_type = order.type

            if order_type not in [mt5_instance.ORDER_TYPE_BUY_LIMIT, mt5_instance.ORDER_TYPE_SELL_LIMIT,
                                mt5_instance.ORDER_TYPE_BUY_STOP, mt5_instance.ORDER_TYPE_SELL_STOP]:
                log_and_print(f"Skipping order {order_id} for {symbol}: Not a limit or stop order (type: {order_type})", "DEBUG")
                continue

            is_buy = order_type in [mt5_instance.ORDER_TYPE_BUY_LIMIT, mt5_instance.ORDER_TYPE_BUY_STOP]
            expected_order_type = 'buy_limit' if is_buy else 'sell_limit'

            # Find matching signal in bouncestreamsignals.json
            matching_signal = None
            for signal in self.signals:
                if (signal['pair'].lower() == symbol and 
                    signal['order_type'].lower() == expected_order_type and 
                    abs(float(signal['entry_price']) - order.price_open) < 0.0001):
                    matching_signal = signal
                    break

            if matching_signal:
                # Copy signal to limit orders
                order_record = matching_signal.copy()
                order_record['ticket'] = order_id
                order_record['open_time'] = datetime.fromtimestamp(order.time_setup, pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00')
                new_limit_orders.append(order_record)
                log_and_print(f"Moved limit order for {symbol} (ticket: {order_id}) to {limit_orders_file}", "INFO")
            else:
                # No matching signal, create a new signal and add to bouncestream
                new_signal = self.create_limit_order_record(order, mt5_instance)
                if self.update_bouncestream_signals(new_signal):
                    new_limit_orders.append(new_signal)
                    log_and_print(f"Created and moved new limit order for {symbol} (ticket: {order_id}) to {limit_orders_file}", "INFO")
                else:
                    log_and_print(f"Failed to update bouncestream signals for {symbol} (ticket: {order_id})", "ERROR")
                    continue

        # Remove expired or canceled limit orders
        for order in limit_orders:
            if order['ticket'] not in current_order_tickets:
                log_and_print(f"Removed expired/canceled limit order {order['ticket']} for {order['pair']} from {limit_orders_file}", "INFO")
                continue
            new_limit_orders.append(order)

        # Save updated limit orders
        self.save_to_json(limit_orders_file, new_limit_orders)

        return len(new_running_trades), len(new_limit_orders)

    async def regulate_trades(self, account: Dict) -> tuple[int, int]:
        """Regulate stop-loss for running market orders based on signals and timeframe."""
        account_key = f"user_{account['user_id']}_sub_{account['subaccount_id']}" if account['subaccount_id'] else f"user_{account['user_id']}"
        log_and_print(f"===== Regulating Trades for {account_key} =====", "TITLE")
        
        mt5_instance = self.mt5_manager.mt5_instances.get(account_key)
        if not mt5_instance:
            log_and_print(f"No MT5 instance found for {account_key}", "ERROR")
            return 0, 0

        # Manage trades and orders first
        running_trades_count, limit_orders_count = await self.manage_trades_and_orders(account, mt5_instance)
        log_and_print(f"Managed {running_trades_count} running trades and {limit_orders_count} limit orders for {account_key}", "INFO")

        running_trades_file = os.path.join(self.config.running_trades_dir, f"{account_key}_runningtrades.json")
        running_trades = self.load_account_json(running_trades_file)

        adjusted_orders = 0
        failed_adjustments = 0

        try:
            positions = mt5_instance.positions_get()
            if not positions:
                log_and_print(f"No open positions found for {account_key}", "INFO")
                return 0, 0

            log_and_print(f"Found {len(positions)} open positions for {account_key}", "INFO")

            for position in positions:
                try:
                    symbol = position.symbol.lower()
                    position_id = position.ticket
                    order_type = position.type
                    entry_price = position.price_open
                    current_sl = position.sl
                    current_tp = position.tp

                    if order_type not in [mt5_instance.ORDER_TYPE_BUY, mt5_instance.ORDER_TYPE_SELL]:
                        log_and_print(f"Skipping position {position_id} for {symbol}: Not a buy or sell order (type: {order_type})", "DEBUG")
                        continue

                    is_buy = order_type == mt5_instance.ORDER_TYPE_BUY
                    is_sell = order_type == mt5_instance.ORDER_TYPE_SELL
                    expected_order_type = 'buy_limit' if is_buy else 'sell_limit'

                    # Find matching signal in running_trades JSON
                    matching_signal = None
                    for trade in running_trades:
                        if (trade['pair'].lower() == symbol and 
                            trade['order_type'].lower() == expected_order_type and 
                            trade['ticket'] == position_id):
                            matching_signal = trade
                            break

                    if not matching_signal:
                        log_and_print(f"No matching running trade found for position {position_id} ({symbol}, entry: {entry_price})", "DEBUG")
                        continue

                    timeframe_str = matching_signal['timeframe']
                    timeframe = self.get_timeframe(timeframe_str)
                    if not timeframe:
                        error_message = f"Invalid timeframe '{timeframe_str}' for position {position_id} ({symbol})"
                        log_and_print(error_message, "ERROR")
                        self.save_adjustment_error(account_key, symbol, position_id, error_message)
                        failed_adjustments += 1
                        self.total_orders_failed += 1
                        continue

                    symbol_info = mt5_instance.symbol_info(symbol)
                    if not symbol_info:
                        error_message = f"Cannot retrieve symbol info for {symbol}"
                        log_and_print(error_message, "ERROR")
                        self.save_adjustment_error(account_key, symbol, position_id, error_message)
                        failed_adjustments += 1
                        self.total_orders_failed += 1
                        continue

                    tick = mt5_instance.symbol_info_tick(symbol)
                    if not tick:
                        error_message = f"Cannot retrieve tick data for {symbol}"
                        log_and_print(error_message, "ERROR")
                        self.save_adjustment_error(account_key, symbol, position_id, error_message)
                        failed_adjustments += 1
                        self.total_orders_failed += 1
                        continue

                    current_price = tick.bid if is_sell else tick.ask
                    tick_size = symbol_info.trade_tick_size
                    point = symbol_info.point
                    stops_level = symbol_info.trade_stops_level * point

                    rates = mt5_instance.copy_rates_from_pos(symbol, timeframe, 1, 1)
                    if not rates or len(rates) == 0:
                        error_message = f"Failed to retrieve {timeframe_str} candle data for {symbol}"
                        log_and_print(error_message, "ERROR")
                        self.save_adjustment_error(account_key, symbol, position_id, error_message)
                        failed_adjustments += 1
                        self.total_orders_failed += 1
                        continue

                    close_price = rates[0]['close']

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

                    if is_buy:
                        if close_price > ratio_2_price:
                            new_sl = round(ratio_1_price / tick_size) * tick_size
                            if current_sl == 0.0 or (current_sl > 0 and current_sl < new_sl):
                                adjustment_needed = True
                                adjustment_reason = f"beyond 1:2 RR (close: {close_price}, ratio_2: {ratio_2_price})"
                        elif close_price > ratio_1_price:
                            new_sl = round(ratio_0_5_price / tick_size) * tick_size
                            if current_sl == 0.0 or (current_sl > 0 and current_sl < new_sl):
                                adjustment_needed = True
                                adjustment_reason = f"beyond 1:1 RR (close: {close_price}, ratio_1: {ratio_1_price})"
                        elif close_price > ratio_0_5_price:
                            new_sl = round(ratio_0_25_price / tick_size) * tick_size
                            if current_sl == 0.0 or (current_sl > 0 and current_sl < new_sl):
                                adjustment_needed = True
                                adjustment_reason = f"beyond 1:0.5 RR (close: {close_price}, ratio_0_5: {ratio_0_5_price})"
                        elif close_price > entry_price_signal:
                            new_sl = round((entry_price * (1 + SL_ADJUSTMENT_PERCENT / 100)) / tick_size) * tick_size
                            if current_sl == 0.0 or (current_sl > 0 and current_sl < new_sl):
                                adjustment_needed = True
                                adjustment_reason = f"above entry price (close: {close_price}, entry: {entry_price_signal})"
                    elif is_sell:
                        if close_price < ratio_2_price:
                            new_sl = round(ratio_1_price / tick_size) * tick_size
                            if current_sl == 0.0 or (current_sl > 0 and current_sl > new_sl):
                                adjustment_needed = True
                                adjustment_reason = f"beyond 1:2 RR (close: {close_price}, ratio_2: {ratio_2_price})"
                        elif close_price < ratio_1_price:
                            new_sl = round(ratio_0_5_price / tick_size) * tick_size
                            if current_sl == 0.0 or (current_sl > 0 and current_sl > new_sl):
                                adjustment_needed = True
                                adjustment_reason = f"beyond 1:1 RR (close: {close_price}, ratio_1: {ratio_1_price})"
                        elif close_price < ratio_0_5_price:
                            new_sl = round(ratio_0_25_price / tick_size) * tick_size
                            if current_sl == 0.0 or (current_sl > 0 and current_sl > new_sl):
                                adjustment_needed = True
                                adjustment_reason = f"beyond 1:0.5 RR (close: {close_price}, ratio_0_5: {ratio_0_5_price})"
                        elif close_price < entry_price_signal:
                            new_sl = round((entry_price * (1 - SL_ADJUSTMENT_PERCENT / 100)) / tick_size) * tick_size
                            if current_sl == 0.0 or (current_sl > 0 and current_sl > new_sl):
                                adjustment_needed = True
                                adjustment_reason = f"below entry price (close: {close_price}, entry: {entry_price_signal})"

                    if not adjustment_needed:
                        log_and_print(f"No stop-loss adjustment needed for position {position_id} ({symbol})", "DEBUG")
                        continue

                    if is_sell:
                        if new_sl < entry_price + stops_level:
                            error_message = f"New stop-loss {new_sl} too close to entry {entry_price} for {symbol} (sell, min distance: {stops_level})"
                            log_and_print(error_message, "WARNING")
                            self.save_adjustment_error(account_key, symbol, position_id, error_message)
                            failed_adjustments += 1
                            self.total_orders_failed += 1
                            continue
                    elif is_buy:
                        if new_sl > entry_price - stops_level:
                            error_message = f"New stop-loss {new_sl} too close to entry {entry_price} for {symbol} (buy, min distance: {stops_level})"
                            log_and_print(error_message, "WARNING")
                            self.save_adjustment_error(account_key, symbol, position_id, error_message)
                            failed_adjustments += 1
                            self.total_orders_failed += 1
                            continue

                    request = {
                        "action": mt5_instance.TRADE_ACTION_SLTP,
                        "position": position_id,
                        "symbol": symbol,
                        "sl": new_sl,
                        "tp": current_tp
                    }

                    log_and_print(f"Attempting to adjust stop-loss for position {position_id} ({symbol}) to {new_sl} ({adjustment_reason})", "INFO")

                    for attempt in range(1, MAX_RETRIES + 1):
                        result = mt5_instance.order_send(request)
                        if result.retcode == mt5_instance.TRADE_RETCODE_DONE:
                            log_and_print(f"Successfully adjusted stop-loss for position {position_id} ({symbol}) to {new_sl} ({adjustment_reason})", "SUCCESS")
                            adjusted_orders += 1
                            self.total_orders_adjusted += 1
                            break
                        else:
                            error_message = f"Failed to adjust stop-loss for position {position_id} ({symbol}) on attempt {attempt}: {result.retcode}, {result.comment}"
                            log_and_print(error_message, "ERROR")
                            if attempt == MAX_RETRIES:
                                self.save_adjustment_error(account_key, symbol, position_id, error_message)
                                failed_adjustments += 1
                                self.total_orders_failed += 1
                            else:
                                log_and_print(f"Retrying adjustment after {MT5_RETRY_DELAY} seconds...", "INFO")
                                await asyncio.sleep(MT5_RETRY_DELAY)

                except Exception as e:
                    error_message = f"Error processing position {position_id} for {symbol}: {str(e)}"
                    log_and_print(error_message, "ERROR")
                    self.save_adjustment_error(account_key, symbol, position_id, error_message)
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
                continue

            for account in self.valid_accounts:
                adjusted, failed = await self.regulate_trades(account)
                total_adjusted += adjusted
                total_failed += failed

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