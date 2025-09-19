import connectwithinfinitydb as db
import validatesignals
import MetaTrader5 as mt5
import os
import shutil
from typing import List, Dict, Optional
from colorama import Fore, Style, init
import logging
import time
import asyncio
from datetime import datetime, timezone,  timedelta
import pytz
import json
import threading
import difflib


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
RETRY_MAX_ATTEMPTS = 3
RETRY_DELAY = 2
ORIGINAL_MT5_DIR = r"C:\xampp\htdocs\CIPHER\metaTrader5\MetaTrader 5"  # Source MetaTrader 5 directory
BASE_MT5_DIR = r"C:\xampp\htdocs\CIPHER\metaTrader5\users"  # Base directory for account-specific MT5 installations
MAX_RETRIES = 5
MT5_RETRY_DELAY = 3

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

BASE_LOTSIZE_FOLDER = r"C:\xampp\htdocs\CIPHER\cipher trader\market"
async def fetchlotsizeandriskallowed(json_dir: str = BASE_LOTSIZE_FOLDER) -> bool:
    """Fetch all lot size and allowed risk data from ciphercontracts_lotsizeandrisk table and save to lotsizeandrisk.json."""
    log_and_print("Fetching all lot size and allowed risk data", "INFO")
    
    # Initialize error log list
    error_log = []
    
    # Define error log file path using BASE_LOTSIZE_FOLDER
    error_json_path = os.path.join(json_dir, "fetchlotsizeandriskerror.json")
    
    # Helper function to save errors to JSON
    def save_errors():
        try:
            with open(error_json_path, 'w', encoding='utf-8') as f:
                json.dump(error_log, f, indent=4)
            log_and_print(f"Errors saved to {error_json_path}", "INFO")
        except Exception as e:
            log_and_print(f"Failed to save errors to {error_json_path}: {str(e)}", "ERROR")
    
    # SQL query to fetch all rows
    sql_query = """
        SELECT id, pair, timeframe, lot_size, allowed_risk, created_at
        FROM ciphercontracts_lotsizeandrisk
    """
    
    # Create output directory if it doesn't exist
    if not os.path.exists(json_dir):
        try:
            os.makedirs(json_dir, exist_ok=True)
            log_and_print(f"Created output directory: {json_dir}", "INFO")
        except Exception as e:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Error creating directory {json_dir}: {str(e)}"
            })
            save_errors()
            log_and_print(f"Error creating directory {json_dir}: {str(e)}", "ERROR")
            return False
    
    # Execute query with retries
    for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
        try:
            result = db.execute_query(sql_query)
            log_and_print(f"Raw query result for lot size and risk: {json.dumps(result, indent=2)}", "DEBUG")
            
            if not isinstance(result, dict):
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"Invalid result format on attempt {attempt}: Expected dict, got {type(result)}"
                })
                save_errors()
                log_and_print(f"Invalid result format on attempt {attempt}: Expected dict, got {type(result)}", "ERROR")
                continue
                
            if result.get('status') != 'success':
                error_message = result.get('message', 'No message provided')
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"Query failed on attempt {attempt}: {error_message}"
                })
                save_errors()
                log_and_print(f"Query failed on attempt {attempt}: {error_message}", "ERROR")
                continue
                
            # Handle both 'data' and 'results' keys
            rows = None
            if 'data' in result and 'rows' in result['data'] and isinstance(result['data']['rows'], list):
                rows = result['data']['rows']
            elif 'results' in result and isinstance(result['results'], list):
                rows = result['results']
            else:
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"Invalid or missing rows in result on attempt {attempt}: {json.dumps(result, indent=2)}"
                })
                save_errors()
                log_and_print(f"Invalid or missing rows in result on attempt {attempt}: {json.dumps(result, indent=2)}", "ERROR")
                continue
            
            # Prepare data for single JSON file
            data = []
            for row in rows:
                data.append({
                    'id': int(row.get('id', 0)),
                    'pair': row.get('pair', 'N/A'),
                    'timeframe': row.get('timeframe', 'N/A'),
                    'lot_size': float(row.get('lot_size', 0.0)) if row.get('lot_size') is not None else None,
                    'allowed_risk': float(row.get('allowed_risk', 0.0)) if row.get('allowed_risk') is not None else None,
                    'created_at': row.get('created_at', 'N/A')
                })
            
            # Define output path for single JSON file
            output_json_path = os.path.join(json_dir, "lotsizeandrisk.json")
            
            # Save to JSON with overwrite, no deletion attempt
            try:
                with open(output_json_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=4)
                # Set file permissions to ensure accessibility
                os.chmod(output_json_path, 0o666)  # Read/write for owner, group, others
                log_and_print(f"Lot size and allowed risk data saved to {output_json_path}", "SUCCESS")
                return True
            except Exception as e:
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"Error saving {output_json_path}: {str(e)}"
                })
                save_errors()
                log_and_print(f"Error saving {output_json_path}: {str(e)}", "ERROR")
                return False
                
        except Exception as e:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Exception on attempt {attempt}: {str(e)}"
            })
            save_errors()
            log_and_print(f"Exception on attempt {attempt}: {str(e)}", "ERROR")
            
        if attempt < RETRY_MAX_ATTEMPTS:
            delay = RETRY_DELAY * (2 ** (attempt - 1))
            log_and_print(f"Retrying after {delay} seconds...", "INFO")
            await asyncio.sleep(delay)
        else:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": "Max retries reached for fetching lot size and risk data"
            })
            save_errors()
            log_and_print("Max retries reached for fetching lot size and risk data", "ERROR")
            return False
    
    error_log.append({
        "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
        "error": "Function exited without success"
    })
    save_errors()
    return False
async def executefetchlotsizeandrisk():
    """Execute the fetchlotsizeandriskallowed function and handle its result."""
    log_and_print("Starting lot size and risk data fetch", "INFO")
    if not await fetchlotsizeandriskallowed():
        log_and_print("Failed to fetch lot size and allowed risk data. Exiting.", "ERROR")
        return False
    log_and_print("Successfully fetched lot size and allowed risk data", "SUCCESS")
    return True
#bouncestream signals
async def fetch_bouncestream_signals(json_dir: str = BASE_LOTSIZE_FOLDER) -> bool:
    """Fetch bouncestream signals, add summary with timeframe counts, and store them in bouncestreamsignals.json with lot size and allowed risk."""
    log_and_print("===== Fetching Bouncestream Signals =====", "TITLE")

    # Initialize error log list
    error_log = []
    
    # Define error log file path
    error_json_path = os.path.join(json_dir, "fetchbouncestreamsignalserror.json")
    
    # Helper function to save errors to JSON
    def save_errors():
        try:
            with open(error_json_path, 'w', encoding='utf-8') as f:
                json.dump(error_log, f, indent=4)
            log_and_print(f"Errors saved to {error_json_path}", "INFO")
        except Exception as e:
            log_and_print(f"Failed to save errors to {error_json_path}: {str(e)}", "ERROR")

    # Load lot size and risk data from JSON
    lotsize_json_path = os.path.join(json_dir, "lotsizeandrisk.json")
    if not os.path.exists(lotsize_json_path):
        error_log.append({
            "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
            "error": f"Lot size JSON file not found at {lotsize_json_path}"
        })
        save_errors()
        log_and_print(f"Lot size JSON file not found at {lotsize_json_path}", "ERROR")
        return False

    try:
        with open(lotsize_json_path, 'r', encoding='utf-8') as f:
            lotsize_data = json.load(f)
        log_and_print(f"Loaded lot size data from {lotsize_json_path}", "INFO")
    except Exception as e:
        error_log.append({
            "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
            "error": f"Failed to load lot size JSON: {str(e)}"
        })
        save_errors()
        log_and_print(f"Failed to load lot size JSON: {str(e)}", "ERROR")
        return False

    # SQL query to fetch signals
    sql_query = """
        SELECT id, pair, timeframe, order_type, entry_price, exit_price, ratio_0_5_price, ratio_1_price, 
            ratio_2_price, profit_price, created_at
        FROM cipherbouncestream_signals
    """
    log_and_print(f"Fetching signals with query: {sql_query}", "INFO")

    # Execute query with retries
    signals = []
    for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
        try:
            result = db.execute_query(sql_query)
            log_and_print(f"Raw query result for signals: {json.dumps(result, indent=2)}", "DEBUG")

            if not isinstance(result, dict):
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"Invalid result format on attempt {attempt}: Expected dict, got {type(result)}"
                })
                save_errors()
                log_and_print(f"Invalid result format on attempt {attempt}: Expected dict, got {type(result)}", "ERROR")
                continue

            if result.get('status') != 'success':
                error_message = result.get('message', 'No message provided')
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"Query failed on attempt {attempt}: {error_message}"
                })
                save_errors()
                log_and_print(f"Query failed on attempt {attempt}: {error_message}", "ERROR")
                continue

            rows = None
            if 'data' in result and 'rows' in result['data'] and isinstance(result['data']['rows'], list):
                rows = result['data']['rows']
            elif 'results' in result and isinstance(result['results'], list):
                rows = result['results']
            else:
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"Invalid or missing rows in result on attempt {attempt}: {json.dumps(result, indent=2)}"
                })
                save_errors()
                log_and_print(f"Invalid or missing rows in result on attempt {attempt}: {json.dumps(result, indent=2)}", "ERROR")
                continue

            signals = [
                {
                    'pair': row.get('pair', '').lower(),
                    'timeframe': row.get('timeframe', '').lower(),
                    'order_type': row.get('order_type', '').lower(),
                    'entry_price': float(row.get('entry_price', 0.0)),
                    'exit_price': float(row.get('exit_price', 0.0)),
                    'ratio_0_5_price': float(row.get('ratio_0_5_price', 0.0)),
                    'ratio_1_price': float(row.get('ratio_1_price', 0.0)),
                    'ratio_2_price': float(row.get('ratio_2_price', 0.0)),
                    'profit_price': float(row.get('profit_price', 0.0)),
                    'created_at': row.get('created_at', 'N/A')
                } for row in rows
            ]
            log_and_print(f"Fetched {len(signals)} signals from cipherbouncestream_signals", "SUCCESS")
            break

        except Exception as e:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Exception on attempt {attempt}: {str(e)}"
            })
            save_errors()
            log_and_print(f"Exception on attempt {attempt}: {str(e)}", "ERROR")
            if attempt < RETRY_MAX_ATTEMPTS:
                delay = RETRY_DELAY * (2 ** (attempt - 1))
                log_and_print(f"Retrying after {delay} seconds...", "INFO")
                await asyncio.sleep(delay)
            else:
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": "Max retries reached for fetching signals"
                })
                save_errors()
                log_and_print("Max retries reached for fetching signals", "ERROR")
                return False

    if not signals:
        error_log.append({
            "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
            "error": "No signals found to process"
        })
        save_errors()
        log_and_print("No signals found to process", "WARNING")
        return False

    # Add lot size and allowed risk to each signal
    for signal in signals:
        lot_size = None
        allowed_risk = None
        # Normalize timeframe for matching with lotsize_data
        normalized_timeframe = signal['timeframe'].lower().replace('4hour', '4hours')
        for item in lotsize_data:
            item_timeframe = item['timeframe'].lower().replace('4hour', '4hours')
            if item['pair'].lower() == signal['pair'] and item_timeframe == normalized_timeframe:
                lot_size = float(item.get('lot_size', 0.0)) if item.get('lot_size') is not None else 0.0
                allowed_risk = float(item.get('allowed_risk', 0.0)) if item.get('allowed_risk') is not None else 0.0
                break

        signal['lot_size'] = lot_size if lot_size is not None else 0.0
        signal['allowed_risk'] = allowed_risk if allowed_risk is not None else 0.0

    # Calculate summary statistics
    total_pending = len(signals)
    timeframe_counts = {
        '5minutes': 0,
        '15minutes': 0,
        '30minutes': 0,
        '1hour': 0,
        '4hours': 0
    }
    for signal in signals:
        timeframe = signal['timeframe'].lower().replace('4hour', '4hours')
        if timeframe == '5minutes':
            timeframe_counts['5minutes'] += 1
        elif timeframe == '15minutes':
            timeframe_counts['15minutes'] += 1
        elif timeframe == '30minutes':
            timeframe_counts['30minutes'] += 1
        elif timeframe == '1hour':
            timeframe_counts['1hour'] += 1
        elif timeframe == '4hours':
            timeframe_counts['4hours'] += 1

    # Create output JSON structure with summary
    output_data = {
        "bouncestream_pendingorders": total_pending,
        "5minutes pending orders": timeframe_counts['5minutes'],
        "15minutes pending orders": timeframe_counts['15minutes'],
        "30minutes pending orders": timeframe_counts['30minutes'],
        "1Hour pending orders": timeframe_counts['1hour'],
        "4Hours pending orders": timeframe_counts['4hours'],
        "orders": signals
    }

    # Define output path for signals JSON
    output_json_path = os.path.join(json_dir, "bouncestreamsignals.json")

    # Create output directory if it doesn't exist
    if not os.path.exists(json_dir):
        try:
            os.makedirs(json_dir, exist_ok=True)
            log_and_print(f"Created output directory: {json_dir}", "INFO")
        except Exception as e:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Error creating directory {json_dir}: {str(e)}"
            })
            save_errors()
            log_and_print(f"Error creating directory {json_dir}: {str(e)}", "ERROR")
            return False

    # Delete existing file if it exists to overwrite
    if os.path.exists(output_json_path):
        try:
            os.remove(output_json_path)
            log_and_print(f"Existing {output_json_path} deleted", "INFO")
        except Exception as e:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Error deleting existing {output_json_path}: {str(e)}"
            })
            save_errors()
            log_and_print(f"Error deleting existing {output_json_path}: {str(e)}", "ERROR")
            return False

    # Save signals with summary to JSON
    try:
        with open(output_json_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=4)
        # Set file permissions to ensure accessibility
        os.chmod(output_json_path, 0o666)  # Read/write for owner, group, others
        log_and_print(f"Bouncestream signals with summary saved to {output_json_path}", "SUCCESS")
        return True
    except Exception as e:
        error_log.append({
            "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
            "error": f"Error saving {output_json_path}: {str(e)}"
        })
        save_errors()
        log_and_print(f"Error saving {output_json_path}: {str(e)}", "ERROR")
        return False
async def execute_fetch_bouncestream_signals():
    """Execute the fetch_bouncestream_signals function and handle its result."""
    log_and_print("Starting bouncestream signals fetch", "INFO")
    if not await fetch_bouncestream_signals():
        log_and_print("Failed to fetch bouncestream signals. Exiting.", "ERROR")
        return False
    log_and_print("Successfully fetched bouncestream signals", "SUCCESS")
    return True
def run_validatesignals_main():
    try:
        validatesignals.main()
    except Exception as e:
        print(f"Error in analysechart_m (M15): {e}")


# Configuration Manager Class
class ConfigManager:
    """Manages configuration settings for terminal copying and validation."""
    def __init__(self):
        self.main_export_dir: str = EXPORT_DIR
        self.base_mt5_dir: str = BASE_MT5_DIR
        self.original_mt5_dir: str = ORIGINAL_MT5_DIR
        self.valid_programmes: List[str] = ['bouncestream']  # Only bouncestream allowed
        self.valid_brokers: List[str] = ['deriv', 'forex']
        self.min_capital_regular: float = 3.0
        self.min_capital_unique: float = 3.0

    def validate_directory(self) -> bool:
        """Validate the export directory exists and is writable."""
        if not os.path.exists(self.main_export_dir) or not os.access(self.main_export_dir, os.W_OK):
            log_and_print(f"Invalid or inaccessible directory: {self.main_export_dir}", "ERROR")
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

        # Check if the terminal already exists for this account
        if os.path.exists(new_terminal_path):
            log_and_print(f"Terminal already exists at {new_terminal_path}", "INFO")
            return new_terminal_path

        try:
            # Copy the entire MetaTrader 5 directory
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
    """Manversations MT5 initialization and login."""
    def initialize_mt5(self, server: str, login: str, password: str, terminal_path: str) -> bool:
        """Initialize MT5 terminal and login with provided credentials using the specified terminal path."""
        log_and_print(f"Attempting MT5 login for server: {server}, login: {login} using {terminal_path}", "INFO")
        
        # Use thread-local MT5 instance to avoid conflicts
        if not hasattr(thread_local, 'mt5'):
            thread_local.mt5 = mt5

        # Ensure no existing MT5 connections interfere
        try:
            #thread_local.mt5.shutdown() hey grok dont uncomment this, its on purpose
            log_and_print("Previous MT5 connections closed", "DEBUG")
        except Exception as e:
            log_and_print(f"Error during MT5 shutdown: {str(e)}", "WARNING")

        # Initialize MT5 terminal with portable=True and extended timeout
        try:
            if thread_local.mt5.initialize(
                path=terminal_path,
                login=int(login),
                password=password,
                server=server,
                portable=True,
                timeout=120000  # Extended to 120 seconds for stability
            ):
                log_and_print("Successfully initialized MT5 terminal", "SUCCESS")
            else:
                error_code, error_message = thread_local.mt5.last_error()
                log_and_print(f"Failed to initialize MT5 terminal. Error: {error_code}, {error_message}", "ERROR")
                return False
        except Exception as e:
            log_and_print(f"Exception during MT5 initialization: {str(e)}", "ERROR")
            return False

        # Wait for terminal to be fully ready
        for _ in range(5):
            if thread_local.mt5.terminal_info() is not None:
                log_and_print("MT5 terminal fully initialized", "DEBUG")
                break
            log_and_print("Waiting for MT5 terminal to fully initialize...", "INFO")
            time.sleep(2)
        else:
            log_and_print("MT5 terminal not ready", "ERROR")
            #thread_local.mt5.shutdown() hey grok dont uncomment this, its on purpose
            return False

        log_and_print(f"Successfully logged in to MT5 for server: {server}, login: {login}", "SUCCESS")
        return True

# Programme Fetcher Class
class ProgrammeFetcher:
    def __init__(self):
        self.config = ConfigManager()
        self.mt5_manager = MT5Manager()
        self.processed_programme_ids: set[str] = set()
        self.total_signals_loaded: int = 0
        self.total_failed_orders: int = 0
        self.total_alltimeframes_orders: int = 0  # New counter for alltimeframes orders
        self.total_priority_timeframes_orders: int = 0  

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

        # Validate fields
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

        # Ensure programme matches bouncestream
        if programme != 'bouncestream':
            log_and_print(f"Skipping record: programme_id={programme_id}, programme={programme} does not match bouncestream", "DEBUG")
            return None

        return {
            'programme_id': programme_id,
            'user_id': user_id,
            'subaccount_id': subaccount_id,
            'programme': programme,
            'broker': broker,
            'broker_loginid': broker_loginid,
            'broker_password': broker_password,
            'broker_server': broker_server,
            'programme_timeframe': programme_timeframe
        }

    async def fetch_user_programmes(self) -> Optional[List[Dict]]:
        """Fetch user programmes from the user_programmes table, including broker details and programme_timeframe."""
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

    async def add_symbols_to_watchlist(self, account: Dict, terminal_path: str) -> bool:
        """Add all available broker symbols to the MT5 Market Watch for the given account."""
        account_key = f"user_{account['user_id']}_sub_{account['subaccount_id']}" if account['subaccount_id'] else f"user_{account['user_id']}"
        log_and_print(f"Adding symbols to Market Watch for {account_key}", "INFO")

        # Initialize MT5 for the account
        if not self.mt5_manager.initialize_mt5(
            server=account['broker_server'],
            login=account['broker_loginid'],
            password=account['broker_password'],
            terminal_path=terminal_path
        ):
            log_and_print(f"Failed to initialize MT5 for {account_key} to add symbols", "ERROR")
            return False

        try:
            # Get all available symbols
            symbols = thread_local.mt5.symbols_get()
            if not symbols:
                error_code, error_message = thread_local.mt5.last_error()
                log_and_print(f"Failed to retrieve symbols for {account_key}. Error: {error_code}, {error_message}", "ERROR")
                return False

            log_and_print(f"Retrieved {len(symbols)} symbols for {account_key}", "INFO")

            # Add each symbol to Market Watch
            for symbol in symbols:
                if not thread_local.mt5.symbol_select(symbol.name, True):
                    error_code, error_message = thread_local.mt5.last_error()
                    log_and_print(f"Failed to add symbol {symbol.name} to Market Watch for {account_key}. Error: {error_code}, {error_message}", "WARNING")
                else:
                    log_and_print(f"Added symbol {symbol.name} to Market Watch for {account_key}", "DEBUG")

            log_and_print(f"Successfully added all symbols to Market Watch for {account_key}", "SUCCESS")
            return True

        except Exception as e:
            log_and_print(f"Exception while adding symbols for {account_key}: {str(e)}", "ERROR")
            return False

    async def batch_update_programme_startdate(self, programme_ids: List[str]) -> int:
        """Batch update programmetrade_startdate for programmes with successful orders if the date is NULL or empty."""
        log_and_print(f"Received {len(programme_ids)} programme IDs for start date update", "DEBUG")
        if not programme_ids:
            log_and_print("No programme IDs to update for programmetrade_startdate", "INFO")
            return 0

        update_queries = []
        current_date = datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S')

        for programme_id in programme_ids:
            # Verify programme_id exists and programmetrade_startdate is NULL
            check_query = f"""
                SELECT programmetrade_startdate
                FROM user_programmes
                WHERE id = '{programme_id}'
            """
            log_and_print(f"Verifying programme_id={programme_id} with query: {check_query}", "DEBUG")
            result = db.execute_query(check_query)
            
            if result['status'] != 'success' or not isinstance(result['results'], list) or not result['results']:
                log_and_print(f"Skipping update for programme_id={programme_id}: ID does not exist or query failed", "ERROR")
                continue

            current_startdate = result['results'][0].get('programmetrade_startdate')
            if current_startdate is not None:
                log_and_print(f"Skipping update for programme_id={programme_id}: programmetrade_startdate already set to {current_startdate}", "INFO")
                continue

            # Build update query
            sql_query = f"""
                UPDATE user_programmes
                SET programmetrade_startdate = '{current_date}'
                WHERE id = '{programme_id}' AND (programmetrade_startdate IS NULL OR programmetrade_startdate = '')
            """
            update_queries.append((sql_query, programme_id))

        if not update_queries:
            log_and_print("No valid updates to process for programmetrade_startdate", "WARNING")
            return 0

        log_and_print(f"Executing {len(update_queries)} batched start date update queries", "INFO")
        success_count = 0
        for sql_query, programme_id in update_queries:
            for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
                log_and_print(f"Executing start date update query (attempt {attempt}/{RETRY_MAX_ATTEMPTS}) for programme_id={programme_id}: {sql_query}", "DEBUG")
                result = db.execute_query(sql_query)
                if result['status'] == 'success' and isinstance(result['results'], dict) and result['results'].get('affected_rows', 0) > 0:
                    log_and_print(f"Successfully updated programmetrade_startdate for programme_id={programme_id}", "SUCCESS")
                    success_count += 1
                    break
                else:
                    error_message = result.get('message', 'No message provided')
                    affected_rows = result['results'].get('affected_rows', 0) if isinstance(result['results'], dict) else 'N/A'
                    log_and_print(f"Failed to update programme_id={programme_id} on attempt {attempt}: {error_message}, affected_rows={affected_rows}", "ERROR")
                    if attempt < RETRY_MAX_ATTEMPTS:
                        delay = RETRY_DELAY * (2 ** (attempt - 1))
                        log_and_print(f"Retrying after {delay} seconds...", "INFO")
                        await asyncio.sleep(delay)
                    else:
                        log_and_print(f"Max retries reached for programme_id={programme_id}. Start date update failed.", "ERROR")

        log_and_print(f"Successfully updated programmetrade_startdate for {success_count}/{len(update_queries)} programmes", "INFO")
        return success_count

    async def process_account_initialization(self, valid_accounts: List[Dict]) -> tuple[int, int, int, int]:
        """Process MT5 initialization, symbol addition, and order placement for valid bouncestream accounts in batches."""
        log_and_print("===== Processing MT5 Initialization, Symbol Addition, and Order Placement for Bouncestream Accounts =====", "TITLE")

        if not valid_accounts:
            log_and_print("No valid bouncestream accounts to process for initialization", "WARNING")
            return 0, 0, 0, 0

        if not self.config.validate_mt5_directory():
            log_and_print("Aborting initialization due to invalid MetaTrader 5 directory", "ERROR")
            return 0, 0, 0, 0

        # Load signals from JSON
        signals_json_path = os.path.join(BASE_LOTSIZE_FOLDER, "bouncestreamsignals.json")
        try:
            with open(signals_json_path, 'r') as file:
                signals_data = json.load(file)
            signals = signals_data.get('orders', [])
            self.total_signals_loaded += len(signals)
            log_and_print(f"Loaded {len(signals)} signals from {signals_json_path}", "INFO")
        except Exception as e:
            log_and_print(f"Error loading signals from {signals_json_path}: {str(e)}", "ERROR")
            return 0, 0, 0, 0

        # Group signals by symbol
        symbol_signals = {}
        for signal in signals:
            symbol = signal['pair'].lower()
            if symbol not in symbol_signals:
                symbol_signals[symbol] = []
            symbol_signals[symbol].append(signal)

        # Update programmetrade_startdate for all valid accounts before processing
        programme_ids_to_update = []
        for account in valid_accounts:
            programme_id = account['programme_id']
            check_query = f"""
                SELECT programmetrade_startdate
                FROM user_programmes
                WHERE id = '{programme_id}'
            """
            log_and_print(f"Checking programmetrade_startdate for programme_id={programme_id}", "DEBUG")
            result = db.execute_query(check_query)
            
            if result['status'] != 'success' or not isinstance(result['results'], list) or not result['results']:
                log_and_print(f"Skipping start date check for programme_id={programme_id}: ID does not exist or query failed", "ERROR")
                continue

            current_startdate = result['results'][0].get('programmetrade_startdate')
            if current_startdate is not None:
                log_and_print(f"programmetrade_startdate already set for programme_id={programme_id}: {current_startdate}", "INFO")
                continue

            programme_ids_to_update.append(programme_id)

        if programme_ids_to_update:
            log_and_print(f"Updating programmetrade_startdate for {len(programme_ids_to_update)} programmes", "INFO")
            startdate_updated = await self.batch_update_programme_startdate(programme_ids_to_update)
            log_and_print(f"Updated programmetrade_startdate for {startdate_updated} programmes", "SUCCESS" if startdate_updated > 0 else "WARNING")
        else:
            log_and_print("No programmes need programmetrade_startdate updates", "INFO")

        accounts_logged_in = 0
        accounts_with_symbols = 0
        total_orders_placed = 0
        accounts_with_orders = set()
        batch_size = 20

        # Process accounts in batches of 20
        for batch_start in range(0, len(valid_accounts), batch_size):
            batch_accounts = valid_accounts[batch_start:batch_start + batch_size]
            log_and_print(f"Processing batch {batch_start // batch_size + 1} with {len(batch_accounts)} accounts", "INFO")

            # For each symbol, process all accounts in the batch
            for symbol, symbol_specific_signals in symbol_signals.items():
                log_and_print(f"Processing orders for symbol {symbol} across batch", "INFO")
                for account in batch_accounts:
                    user_id = account['user_id']
                    subaccount_id = account['subaccount_id']
                    account_type = "sa" if subaccount_id else "ma"
                    account_key = f"user_{user_id}_sub_{subaccount_id}" if subaccount_id else f"user_{user_id}"

                    broker_details = {
                        'broker_server': account.get('broker_server'),
                        'broker_loginid': account.get('broker_loginid'),
                        'broker_password': account.get('broker_password')
                    }

                    if not all([broker_details['broker_server'], broker_details['broker_loginid'], broker_details['broker_password']]):
                        error_message = "Missing broker details (server, login, or password)"
                        log_and_print(f"Skipping initialization for {account_key}: {error_message}", "ERROR")
                        self.save_account_order_error(account_key, "N/A", error_message)
                        continue

                    terminal_path = self.config.create_account_terminal(user_id, account_type)
                    if not terminal_path:
                        error_message = "Failed to create MetaTrader 5 terminal directory"
                        log_and_print(f"Skipping initialization for {account_key}: {error_message}", "ERROR")
                        self.save_account_order_error(account_key, "N/A", error_message)
                        continue

                    # Initialize MT5 and get available symbols
                    for attempt in range(1, MAX_RETRIES + 1):
                        try:
                            if self.mt5_manager.initialize_mt5(
                                server=account['broker_server'],
                                login=account['broker_loginid'],
                                password=account['broker_password'],
                                terminal_path=terminal_path
                            ):
                                log_and_print(f"MT5 initialization successful for {account_key} for symbol {symbol}", "SUCCESS")
                                accounts_logged_in += 1
                                break
                            else:
                                error_message = f"MT5 initialization failed: {thread_local.mt5.last_error()}"
                                log_and_print(f"MT5 initialization failed for {account_key} on attempt {attempt}: {error_message}", "ERROR")
                                if attempt == MAX_RETRIES:
                                    self.save_account_order_error(account_key, "N/A", error_message)
                                    continue
                                log_and_print(f"Retrying MT5 initialization after {MT5_RETRY_DELAY} seconds...", "INFO")
                                await asyncio.sleep(MT5_RETRY_DELAY)
                        except Exception as e:
                            error_message = f"Exception during MT5 initialization: {str(e)}"
                            log_and_print(f"Exception during MT5 initialization for {account_key} on attempt {attempt}: {error_message}", "ERROR")
                            if attempt == MAX_RETRIES:
                                self.save_account_order_error(account_key, "N/A", error_message)
                                continue
                            log_and_print(f"Retrying MT5 initialization after {MT5_RETRY_DELAY} seconds...", "INFO")
                            await asyncio.sleep(MT5_RETRY_DELAY)

                    available_symbols = self.get_available_symbols()
                    if not available_symbols:
                        error_message = "No available symbols retrieved from server"
                        log_and_print(f"No available symbols for {account_key}, aborting order placement: {error_message}", "ERROR")
                        self.save_account_order_error(account_key, "N/A", error_message)
                        continue

                    # Place orders for this symbol
                    symbols_added, orders_placed = await self.place_orders_for_account(account, terminal_path, symbol_specific_signals, available_symbols)
                    if symbols_added > 0:
                        accounts_with_symbols += 1
                    if orders_placed > 0:
                        accounts_with_orders.add(account_key)
                    total_orders_placed += orders_placed

        return accounts_logged_in, accounts_with_symbols, total_orders_placed, len(accounts_with_orders)

    def get_available_symbols(self) -> List[str]:
        """Fetch and return all available symbols on the server."""
        try:
            symbols = thread_local.mt5.symbols_get()
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

    def get_exact_symbol_match(self, json_symbol: str, available_symbols: List[str]) -> Optional[str]:
        """Find the exact server symbol matching the JSON symbol (case-insensitive)."""
        json_lower = json_symbol.lower()
        lower_available = [s.lower() for s in available_symbols]
        if json_lower in lower_available:
            index = lower_available.index(json_lower)
            exact = available_symbols[index]
            log_and_print(f"Matched '{json_symbol}' to exact server symbol: '{exact}'", "DEBUG")
            return exact
        else:
            close_matches = difflib.get_close_matches(json_symbol, available_symbols, n=3, cutoff=0.6)
            log_and_print(f"No exact match for '{json_symbol}'. Closest server symbols: {', '.join(close_matches) if close_matches else 'None'}", "WARNING")
            return None

    def save_failed_orders(self, symbol: str, order_type: str, entry_price: float, profit_price: float, stop_loss: float, 
                        lot_size: float, allowed_risk: float, error_message: str, error_category: str = "unknown") -> None:
        """Save a single failed pending order to a categorized JSON file incrementally."""
        base_path = os.path.join(BASE_LOTSIZE_FOLDER, "errors")
        output_paths = {
            "invalid_entry": os.path.join(base_path, "failedordersinvalidentry.json"),
            "stop_loss": os.path.join(base_path, "failedordersbystoploss.json"),
            "unknown": os.path.join(base_path, "failedpendingorders.json")
        }
        output_path = output_paths.get(error_category, output_paths["unknown"])

        failed_order = {
            "symbol": symbol,
            "order_type": order_type,
            "entry_price": entry_price,
            "profit_price": profit_price,
            "stop_loss": stop_loss,
            "lot_size": lot_size,
            "allowed_risk": allowed_risk,
            "error_message": error_message,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        try:
            os.makedirs(base_path, exist_ok=True)
            existing_data = []
            if os.path.exists(output_path):
                try:
                    with open(output_path, 'r') as file:
                        existing_data = json.load(file)
                    if not isinstance(existing_data, list):
                        existing_data = []
                except json.JSONDecodeError:
                    log_and_print(f"Corrupted JSON file at {output_path}, starting fresh", "WARNING")
                    existing_data = []

            existing_data.append(failed_order)
            with open(output_path, 'w') as file:
                json.dump(existing_data, file, indent=4)
            log_and_print(f"Successfully saved failed order for {symbol} to {output_path} (Category: {error_category})", "SUCCESS")
        except Exception as e:
            log_and_print(f"Error saving failed order for {symbol} to {output_path}: {str(e)}", "ERROR")

    async def alltimeframesorder(self, account: Dict, terminal_path: str, signals: List[Dict], available_symbols: List[str]) -> tuple[int, int]:
        """Place one order per timeframe (15m, 30m, 1h, 4h) for each symbol if available, except M5 unless no other timeframes exist."""
        account_key = f"user_{account['user_id']}_sub_{account['subaccount_id']}" if account['subaccount_id'] else f"user_{account['user_id']}"
        log_and_print(f"===== Placing All-Timeframes Orders for {account_key} =====", "TITLE")

        # Define timeframe order (excluding M5 unless necessary)
        timeframe_order = ['15minutes', '30minutes', '1hour', '4hours', '5minutes']
        log_and_print(f"Timeframe order for alltimeframes: {', '.join(timeframe_order)}", "INFO")

        # Initialize MT5 for the account
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                if self.mt5_manager.initialize_mt5(
                    server=account['broker_server'],
                    login=account['broker_loginid'],
                    password=account['broker_password'],
                    terminal_path=terminal_path
                ):
                    log_and_print(f"MT5 initialization successful for {account_key} on attempt {attempt}", "SUCCESS")
                    break
                else:
                    error_message = f"MT5 initialization failed: {thread_local.mt5.last_error()}"
                    log_and_print(f"MT5 initialization failed for {account_key} on attempt {attempt}: {error_message}", "ERROR")
                    if attempt == MAX_RETRIES:
                        self.save_account_order_error(account_key, "N/A", error_message)
                        return 0, 0
                    log_and_print(f"Retrying MT5 initialization after {MT5_RETRY_DELAY} seconds...", "INFO")
                    await asyncio.sleep(MT5_RETRY_DELAY)
            except Exception as e:
                error_message = f"Exception during MT5 initialization: {str(e)}"
                log_and_print(f"Exception during MT5 initialization for {account_key} on attempt {attempt}: {error_message}", "ERROR")
                if attempt == MAX_RETRIES:
                    self.save_account_order_error(account_key, "N/A", error_message)
                    return 0, 0
                log_and_print(f"Retrying MT5 initialization after {MT5_RETRY_DELAY} seconds...", "INFO")
                await asyncio.sleep(MT5_RETRY_DELAY)

        # Get account balance
        try:
            account_info = thread_local.mt5.account_info()
            if account_info is None:
                error_message = "Failed to retrieve account info"
                log_and_print(f"Error for {account_key}: {error_message}", "ERROR")
                self.save_account_order_error(account_key, "N/A", error_message)
                return 0, 0
            balance = float(account_info.balance)
            log_and_print(f"Account balance for {account_key}: {balance}", "INFO")
        except Exception as e:
            error_message = f"Error retrieving account balance: {str(e)}"
            log_and_print(f"Error for {account_key}: {error_message}", "ERROR")
            self.save_account_order_error(account_key, "N/A", error_message)
            return 0, 0

        # Determine allowed risk levels and multipliers based on balance
        allowed_risk_levels = []
        risk_multipliers = {}
        if balance < 96:
            allowed_risk_levels = [4.0]
            risk_multipliers[4.0] = 1
        elif balance >= 96 and balance < 144:
            allowed_risk_levels = [4.0, 8.0]
            risk_multipliers[4.0] = 4
            risk_multipliers[8.0] = 1
        else:
            allowed_risk_levels = [4.0, 8.0, 16.0]
            base_multiplier = 3 + max(0, int((balance - 144) // 48))
            risk_multipliers[4.0] = base_multiplier
            risk_multipliers[8.0] = 2
            risk_multipliers[16.0] = 1

        log_and_print(f"Allowed risk levels for {account_key}: {allowed_risk_levels}", "INFO")
        log_and_print(f"Risk multipliers for {account_key}: {risk_multipliers}", "INFO")

        added_symbols = []
        failed_symbols = []
        pending_orders_placed = []

        # Step 1: Add symbols to Market Watch
        unique_symbols = list(set(signal['pair'] for signal in signals))
        for json_symbol in unique_symbols:
            try:
                server_symbol = self.get_exact_symbol_match(json_symbol, available_symbols)
                if server_symbol is None:
                    error_message = "No server symbol match found"
                    log_and_print(f"Skipping {json_symbol} for {account_key}: {error_message}", "ERROR")
                    failed_symbols.append(json_symbol)
                    self.save_account_order_error(account_key, json_symbol, error_message)
                    self.total_failed_orders += sum(1 for signal in signals if signal['pair'] == json_symbol)
                    continue

                for attempt in range(1, MAX_RETRIES + 1):
                    if thread_local.mt5.symbol_select(server_symbol, True):
                        log_and_print(f"Symbol {server_symbol} selected directly in Market Watch for {account_key}", "SUCCESS")
                        added_symbols.append(server_symbol)
                        break
                    else:
                        log_and_print(f"Direct selection of {server_symbol} failed for {account_key}, attempting test order on attempt {attempt}", "WARNING")
                        if self.place_test_order(server_symbol):
                            log_and_print(f"Symbol {server_symbol} added to Market Watch via test order for {account_key}", "SUCCESS")
                            added_symbols.append(server_symbol)
                            break
                        else:
                            log_and_print(f"Attempt {attempt}/{MAX_RETRIES}: Test order for {server_symbol} failed for {account_key}", "WARNING")
                            if attempt == MAX_RETRIES:
                                error_message = f"Failed to add {server_symbol} to Market Watch after retries: {thread_local.mt5.last_error()}"
                                log_and_print(error_message, "ERROR")
                                failed_symbols.append(json_symbol)
                                self.save_account_order_error(account_key, json_symbol, error_message)
                                self.total_failed_orders += sum(1 for signal in signals if signal['pair'] == json_symbol)
                                continue
                            log_and_print(f"Retrying test order after {MT5_RETRY_DELAY} seconds...", "INFO")
                            await asyncio.sleep(MT5_RETRY_DELAY)

            except Exception as e:
                error_message = f"Error processing symbol {json_symbol}: {str(e)}"
                log_and_print(error_message, "ERROR")
                failed_symbols.append(json_symbol)
                self.save_account_order_error(account_key, json_symbol, error_message)
                self.total_failed_orders += sum(1 for signal in signals if signal['pair'] == json_symbol)

        # Step 2: Group signals by symbol
        symbol_signals = {}
        for signal in signals:
            symbol = signal['pair'].lower()
            if symbol not in symbol_signals:
                symbol_signals[symbol] = []
            signal_allowed_risk = float(signal.get('allowed_risk', 0.0)) if signal.get('allowed_risk') is not None else 0.0
            if signal_allowed_risk in allowed_risk_levels:
                symbol_signals[symbol].append(signal)

        # Step 3: Process signals per symbol
        for json_symbol in unique_symbols:
            server_symbol = self.get_exact_symbol_match(json_symbol, available_symbols)
            if server_symbol is None or server_symbol not in added_symbols:
                log_and_print(f"Skipping orders for {json_symbol} (server: {server_symbol}) as it was not added to Market Watch", "WARNING")
                continue

            log_and_print(f"Processing orders for symbol {json_symbol} (server: {server_symbol})", "INFO")

            # Get signals for this symbol
            symbol_specific_signals = symbol_signals.get(json_symbol.lower(), [])

            # Group signals by timeframe
            signals_by_timeframe = {}
            for signal in symbol_specific_signals:
                timeframe = signal['timeframe'].lower().replace('4hour', '4hours')
                if timeframe not in signals_by_timeframe:
                    signals_by_timeframe[timeframe] = []
                signals_by_timeframe[timeframe].append(signal)

            # Check if any non-M5 timeframes exist
            has_non_m5 = any(tf in signals_by_timeframe for tf in ['15minutes', '30minutes', '1hour', '4hours'])

            # Process one order per timeframe (15m, 30m, 1h, 4h), or M5 only if no others
            for timeframe in timeframe_order:
                if timeframe == '5minutes' and has_non_m5:
                    log_and_print(f"Skipping M5 orders for {json_symbol} as other timeframes are available", "INFO")
                    continue
                if timeframe not in signals_by_timeframe:
                    log_and_print(f"No signals for {json_symbol} on {timeframe}", "INFO")
                    continue

                log_and_print(f"Processing one order for {json_symbol} on {timeframe}", "INFO")
                signal = signals_by_timeframe[timeframe][0]  # Take the first signal for this timeframe

                try:
                    signal_allowed_risk = float(signal.get('allowed_risk', 0.0)) if signal.get('allowed_risk') is not None else 0.0
                    if signal_allowed_risk not in allowed_risk_levels:
                        error_message = f"Signal risk {signal_allowed_risk} not allowed for account balance {balance}"
                        log_and_print(f"Skipping order for {json_symbol} ({timeframe}) for {account_key}: {error_message}", "WARNING")
                        self.save_account_order_error(account_key, json_symbol, error_message)
                        self.total_failed_orders += 1
                        continue

                    order_type = signal['order_type']
                    entry_price = signal['entry_price']
                    profit_price = signal['profit_price'] if signal['profit_price'] else None
                    stop_loss = signal['exit_price'] if signal['exit_price'] else None
                    lot_size = signal['lot_size']
                    multiplier = risk_multipliers.get(signal_allowed_risk, 1)
                    adjusted_lot_size = float(lot_size) * multiplier

                    # Pre-validate signal data
                    if not all([json_symbol, order_type, entry_price, lot_size]):
                        error_message = "Invalid signal data: Missing required fields"
                        log_and_print(f"Skipping order for {json_symbol} ({timeframe}) for {account_key}: {error_message}", "ERROR")
                        self.save_account_order_error(account_key, json_symbol, error_message)
                        self.total_failed_orders += 1
                        continue
                    if order_type not in ['buy_limit', 'sell_limit']:
                        error_message = f"Unsupported order type {order_type}"
                        log_and_print(f"Skipping order for {json_symbol} ({timeframe}) for {account_key}: {error_message}", "ERROR")
                        self.save_account_order_error(account_key, json_symbol, error_message)
                        self.total_failed_orders += 1
                        continue
                    if adjusted_lot_size <= 0.0:
                        error_message = f"Invalid adjusted lot size {adjusted_lot_size} (original: {lot_size}, multiplier: {multiplier})"
                        log_and_print(f"Skipping order for {json_symbol} ({timeframe}) for {account_key}: {error_message}", "ERROR")
                        self.save_account_order_error(account_key, json_symbol, error_message)
                        self.total_failed_orders += 1
                        continue

                    success, order_id, error_message, error_category = self.place_pending_order(
                        server_symbol, order_type, entry_price, profit_price, stop_loss, adjusted_lot_size, signal_allowed_risk
                    )
                    if success:
                        pending_orders_placed.append((server_symbol, order_id, order_type, entry_price, profit_price, stop_loss, signal_allowed_risk, multiplier))
                        self.total_alltimeframes_orders += 1  # Increment alltimeframes counter
                        log_and_print(f"Order placed for {json_symbol} ({timeframe}) for {account_key}: {order_type} at {entry_price}, lot_size={adjusted_lot_size} (multiplier={multiplier})", "SUCCESS")
                    else:
                        log_and_print(f"Failed to place order for {json_symbol} ({timeframe}) for {account_key}: {error_message}", "ERROR")
                        failed_symbols.append(json_symbol)
                        self.save_account_order_error(account_key, server_symbol, error_message)
                        self.total_failed_orders += 1
                        self.save_failed_orders(
                            server_symbol, order_type, entry_price, profit_price, stop_loss,
                            adjusted_lot_size, signal_allowed_risk, error_message, error_category
                        )

                except Exception as e:
                    error_message = f"Error processing signal for {json_symbol} ({timeframe}): {str(e)}"
                    log_and_print(error_message, "ERROR")
                    failed_symbols.append(json_symbol)
                    self.save_account_order_error(account_key, json_symbol, error_message)
                    self.total_failed_orders += 1

        # Log summary
        log_and_print(f"===== All-Timeframes Order Placement Summary for {account_key} =====", "TITLE")
        if added_symbols:
            log_and_print(f"Symbols added to Market Watch: {', '.join(added_symbols)}", "SUCCESS")
        if failed_symbols:
            log_and_print(f"Symbols failed to add or process: {', '.join(set(failed_symbols))}", "ERROR")
        if pending_orders_placed:
            log_and_print(
                f"Pending orders placed: {', '.join([f'{sym} ({otype} at {entry}, TP {tp}, SL {sl}, Risk {risk}, Multiplier {multi})' for sym, oid, otype, entry, tp, sl, risk, multi in pending_orders_placed])}",
                "SUCCESS"
            )
        else:
            log_and_print("No pending orders placed", "INFO")
        log_and_print(f"Total: {len(added_symbols)} symbols added, {len(set(failed_symbols))} failed, {len(pending_orders_placed)} pending orders placed", "INFO")

        return len(added_symbols), len(pending_orders_placed)

    def place_pending_order(self, symbol: str, order_type: str, entry_price: float, profit_price: float, stop_loss: float, 
                        lot_size: float, allowed_risk: float) -> tuple[bool, Optional[int], Optional[str], Optional[str]]:
        """Place a pending order with validation."""
        try:
            if not thread_local.mt5.symbol_select(symbol, True):
                error_message = f"Failed to select {symbol} for pending order, error: {thread_local.mt5.last_error()}"
                log_and_print(error_message, "ERROR")
                return False, None, error_message, "unknown"

            symbol_info = thread_local.mt5.symbol_info(symbol)
            if symbol_info is None:
                error_message = f"Cannot retrieve info for {symbol}"
                log_and_print(error_message, "ERROR")
                return False, None, error_message, "unknown"

            if not symbol_info.trade_mode == thread_local.mt5.SYMBOL_TRADE_MODE_FULL:
                error_message = f"Symbol {symbol} is not tradeable (trade mode: {symbol_info.trade_mode})"
                log_and_print(error_message, "ERROR")
                return False, None, error_message, "unknown"

            tick = thread_local.mt5.symbol_info_tick(symbol)
            if tick is None:
                error_message = f"Cannot retrieve tick data for {symbol}, error: {thread_local.mt5.last_error()}"
                log_and_print(error_message, "ERROR")
                return False, None, error_message, "unknown"

            tick_size = symbol_info.trade_tick_size
            point = symbol_info.point
            stops_level = symbol_info.trade_stops_level * point
            current_bid = tick.bid
            current_ask = tick.ask

            entry_price = round(float(entry_price) / tick_size) * tick_size
            profit_price = round(float(profit_price) / tick_size) * tick_size if profit_price else 0.0
            stop_loss = round(float(stop_loss) / tick_size) * tick_size if stop_loss else 0.0

            log_and_print(
                f"Validating order for {symbol}: "
                f"Order Type={order_type}, Entry={entry_price}, TP={profit_price}, SL={stop_loss}, "
                f"Current Bid={current_bid}, Ask={current_ask}, Stops Level={stops_level}, Tick Size={tick_size}, "
                f"Allowed Risk={allowed_risk}",
                "DEBUG"
            )

            is_buy_limit = order_type.lower() == "buy_limit"
            is_sell_limit = order_type.lower() == "sell_limit"

            if is_buy_limit:
                min_price = current_ask - stops_level
                if entry_price > min_price:
                    error_message = (
                        f"Invalid buy_limit entry price for {symbol}. "
                        f"Entry: {entry_price}, must be <= {min_price} (ask: {current_ask}, stops_level: {stops_level})"
                    )
                    log_and_print(error_message, "ERROR")
                    return False, None, error_message, "invalid_entry"
            elif is_sell_limit:
                max_price = current_bid + stops_level
                if entry_price < max_price:
                    error_message = (
                        f"Invalid sell_limit entry price for {symbol}. "
                        f"Entry: {entry_price}, must be >= {max_price} (bid: {current_bid}, stops_level: {stops_level})"
                    )
                    log_and_print(error_message, "ERROR")
                    return False, None, error_message, "invalid_entry"
            else:
                error_message = f"Unsupported order type {order_type} for {symbol}"
                log_and_print(error_message, "ERROR")
                return False, None, error_message, "unknown"

            if is_buy_limit:
                if stop_loss and stop_loss > entry_price:
                    error_message = f"Invalid stop-loss for {symbol} (buy_limit). SL: {stop_loss}, must be <= {entry_price}"
                    log_and_print(error_message, "ERROR")
                    return False, None, error_message, "stop_loss"
                if profit_price and profit_price < entry_price:
                    error_message = f"Invalid take-profit for {symbol} (buy_limit). TP: {profit_price}, must be >= {entry_price}"
                    log_and_print(error_message, "ERROR")
                    return False, None, error_message, "stop_loss"
                if stop_loss and abs(entry_price - stop_loss) < stops_level:
                    error_message = (
                        f"Stop-loss too close to entry price for {symbol}. "
                        f"SL: {stop_loss}, Entry: {entry_price}, Distance: {abs(entry_price - stop_loss)}, "
                        f"Required: >= {stops_level}"
                    )
                    log_and_print(error_message, "ERROR")
                    return False, None, error_message, "stop_loss"
                if profit_price and abs(profit_price - entry_price) < stops_level:
                    error_message = (
                        f"Take-profit too close to entry price for {symbol}. "
                        f"TP: {profit_price}, Entry: {entry_price}, Distance: {abs(profit_price - entry_price)}, "
                        f"Required: >= {stops_level}"
                    )
                    log_and_print(error_message, "ERROR")
                    return False, None, error_message, "stop_loss"
            elif is_sell_limit:
                if stop_loss and stop_loss < entry_price:
                    error_message = f"Invalid stop-loss for {symbol} (sell_limit). SL: {stop_loss}, must be >= {entry_price}"
                    log_and_print(error_message, "ERROR")
                    return False, None, error_message, "stop_loss"
                if profit_price and profit_price > entry_price:
                    error_message = f"Invalid take-profit for {symbol} (sell_limit). TP: {profit_price}, must be <= {entry_price}"
                    log_and_print(error_message, "ERROR")
                    return False, None, error_message, "stop_loss"
                if stop_loss and abs(entry_price - stop_loss) < stops_level:
                    error_message = (
                        f"Stop-loss too close to entry price for {symbol}. "
                        f"SL: {stop_loss}, Entry: {entry_price}, Distance: {abs(entry_price - stop_loss)}, "
                        f"Required: >= {stops_level}"
                    )
                    log_and_print(error_message, "ERROR")
                    return False, None, error_message, "stop_loss"
                if profit_price and abs(profit_price - entry_price) < stops_level:
                    error_message = (
                        f"Take-profit too close to entry price for {symbol}. "
                        f"TP: {profit_price}, Entry: {entry_price}, Distance: {abs(profit_price - entry_price)}, "
                        f"Required: >= {stops_level}"
                    )
                    log_and_print(error_message, "ERROR")
                    return False, None, error_message, "stop_loss"

            mt5_order_type = thread_local.mt5.ORDER_TYPE_BUY_LIMIT if is_buy_limit else thread_local.mt5.ORDER_TYPE_SELL_LIMIT
            request = {
                "action": thread_local.mt5.TRADE_ACTION_PENDING,
                "symbol": symbol,
                "volume": float(lot_size),
                "type": mt5_order_type,
                "price": entry_price,
                "sl": stop_loss if stop_loss else 0.0,
                "tp": profit_price if profit_price else 0.0,
                "type_time": thread_local.mt5.ORDER_TIME_GTC,
                "type_filling": thread_local.mt5.ORDER_FILLING_IOC,
            }

            log_and_print(f"Sending order for {symbol}: {request}, Allowed Risk={allowed_risk}", "DEBUG")

            result = thread_local.mt5.order_send(request)
            if result.retcode != thread_local.mt5.TRADE_RETCODE_DONE:
                error_code, error_message = thread_local.mt5.last_error()
                full_error = f"Pending order for {symbol} failed, error: {result.retcode}, {error_message}"
                log_and_print(full_error, "ERROR")
                error_category = "invalid_entry" if result.retcode == 10015 else "stop_loss" if result.retcode == 10016 else "unknown"
                return False, None, full_error, error_category

            log_and_print(
                f"Pending {order_type} order for {symbol} placed successfully at {entry_price} "
                f"with TP {profit_price}, SL {stop_loss}, Allowed Risk={allowed_risk} (Order #{result.order})",
                "SUCCESS"
            )
            return True, result.order, None, None

        except Exception as e:
            error_message = f"Error placing pending order for {symbol}: {str(e)}"
            log_and_print(error_message, "ERROR")
            return False, None, error_message, "unknown"

    def save_account_order_error(self, account_key: str, market: str, error_message: str) -> None:
        """Save account-specific order placement errors to accountsordersissues.json."""
        error_dir = os.path.join(BASE_LOTSIZE_FOLDER, "errors")
        output_path = os.path.join(error_dir, "accountsordersissues.json")
        
        error_entry = {
            "account_key": account_key,
            "market": market if market else "N/A",
            "pending_order_status": f"fails({error_message})"
        }
        
        try:
            os.makedirs(error_dir, exist_ok=True)
            existing_data = []
            if os.path.exists(output_path):
                try:
                    with open(output_path, 'r', encoding='utf-8') as file:
                        existing_data = json.load(file)
                    if not isinstance(existing_data, list):
                        existing_data = []
                except json.JSONDecodeError:
                    log_and_print(f"Corrupted JSON file at {output_path}, starting fresh", "WARNING")
                    existing_data = []
            
            existing_data.append(error_entry)
            with open(output_path, 'w', encoding='utf-8') as file:
                json.dump(existing_data, file, indent=4)
            log_and_print(f"Saved error for {account_key} (market: {market}) to {output_path}", "INFO")
        except Exception as e:
            log_and_print(f"Error saving to {output_path}: {str(e)}", "ERROR")

    async def place_orders_for_account(self, account: Dict, terminal_path: str, signals: List[Dict], available_symbols: List[str]) -> tuple[int, int]:
        """Place pending orders for a valid account based on provided signals, using priority_timeframe or alltimeframes logic."""
        programme_timeframe = account.get('programme_timeframe', 'priority_timeframe')
        log_and_print(f"Using {programme_timeframe} order placement strategy for account", "INFO")

        if programme_timeframe == 'alltimeframes':
            symbols_added, orders_placed = await self.alltimeframesorder(account, terminal_path, signals, available_symbols)
            return symbols_added, orders_placed
        
        # Original priority_timeframe logic
        account_key = f"user_{account['user_id']}_sub_{account['subaccount_id']}" if account['subaccount_id'] else f"user_{account['user_id']}"
        log_and_print(f"===== Placing Priority-Timeframe Orders for {account_key} =====", "TITLE")

        # Define timeframe priority order
        timeframe_priority = ['15minutes', '4hours', '1hour', '30minutes', '5minutes']
        log_and_print(f"Timeframe priority order: {', '.join(timeframe_priority)}", "INFO")

        # Initialize MT5 for the account
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                if self.mt5_manager.initialize_mt5(
                    server=account['broker_server'],
                    login=account['broker_loginid'],
                    password=account['broker_password'],
                    terminal_path=terminal_path
                ):
                    log_and_print(f"MT5 initialization successful for {account_key} on attempt {attempt}", "SUCCESS")
                    break
                else:
                    error_message = f"MT5 initialization failed: {thread_local.mt5.last_error()}"
                    log_and_print(f"MT5 initialization failed for {account_key} on attempt {attempt}: {error_message}", "ERROR")
                    if attempt == MAX_RETRIES:
                        self.save_account_order_error(account_key, "N/A", error_message)
                        return 0, 0
                    log_and_print(f"Retrying MT5 initialization after {MT5_RETRY_DELAY} seconds...", "INFO")
                    await asyncio.sleep(MT5_RETRY_DELAY)
            except Exception as e:
                error_message = f"Exception during MT5 initialization: {str(e)}"
                log_and_print(f"Exception during MT5 initialization for {account_key} on attempt {attempt}: {error_message}", "ERROR")
                if attempt == MAX_RETRIES:
                    self.save_account_order_error(account_key, "N/A", error_message)
                    return 0, 0
                log_and_print(f"Retrying MT5 initialization after {MT5_RETRY_DELAY} seconds...", "INFO")
                await asyncio.sleep(MT5_RETRY_DELAY)

        # Get account balance
        try:
            account_info = thread_local.mt5.account_info()
            if account_info is None:
                error_message = "Failed to retrieve account info"
                log_and_print(f"Error for {account_key}: {error_message}", "ERROR")
                self.save_account_order_error(account_key, "N/A", error_message)
                return 0, 0
            balance = float(account_info.balance)
            log_and_print(f"Account balance for {account_key}: {balance}", "INFO")
        except Exception as e:
            error_message = f"Error retrieving account balance: {str(e)}"
            log_and_print(f"Error for {account_key}: {error_message}", "ERROR")
            self.save_account_order_error(account_key, "N/A", error_message)
            return 0, 0

        # Determine allowed risk levels and multipliers based on balance
        allowed_risk_levels = []
        risk_multipliers = {}
        if balance < 96:
            allowed_risk_levels = [4.0]
            risk_multipliers[4.0] = 1
        elif balance >= 96 and balance < 144:
            allowed_risk_levels = [4.0, 8.0]
            risk_multipliers[4.0] = 4
            risk_multipliers[8.0] = 1
        else:
            allowed_risk_levels = [4.0, 8.0, 16.0]
            base_multiplier = 3 + max(0, int((balance - 144) // 48))
            risk_multipliers[4.0] = base_multiplier
            risk_multipliers[8.0] = 2
            risk_multipliers[16.0] = 1

        log_and_print(f"Allowed risk levels for {account_key}: {allowed_risk_levels}", "INFO")
        log_and_print(f"Risk multipliers for {account_key}: {risk_multipliers}", "INFO")

        added_symbols = []
        failed_symbols = []
        pending_orders_placed = []

        # Step 1: Add symbols to Market Watch
        unique_symbols = list(set(signal['pair'] for signal in signals))
        for json_symbol in unique_symbols:
            try:
                server_symbol = self.get_exact_symbol_match(json_symbol, available_symbols)
                if server_symbol is None:
                    error_message = "No server symbol match found"
                    log_and_print(f"Skipping {json_symbol} for {account_key}: {error_message}", "ERROR")
                    failed_symbols.append(json_symbol)
                    self.save_account_order_error(account_key, json_symbol, error_message)
                    self.total_failed_orders += sum(1 for signal in signals if signal['pair'] == json_symbol)
                    continue

                for attempt in range(1, MAX_RETRIES + 1):
                    if thread_local.mt5.symbol_select(server_symbol, True):
                        log_and_print(f"Symbol {server_symbol} selected directly in Market Watch for {account_key}", "SUCCESS")
                        added_symbols.append(server_symbol)
                        break
                    else:
                        log_and_print(f"Direct selection of {server_symbol} failed for {account_key}, attempting test order on attempt {attempt}", "WARNING")
                        if self.place_test_order(server_symbol):
                            log_and_print(f"Symbol {server_symbol} added to Market Watch via test order for {account_key}", "SUCCESS")
                            added_symbols.append(server_symbol)
                            break
                        else:
                            log_and_print(f"Attempt {attempt}/{MAX_RETRIES}: Test order for {server_symbol} failed for {account_key}", "WARNING")
                            if attempt == MAX_RETRIES:
                                error_message = f"Failed to add {server_symbol} to Market Watch after retries: {thread_local.mt5.last_error()}"
                                log_and_print(error_message, "ERROR")
                                failed_symbols.append(json_symbol)
                                self.save_account_order_error(account_key, json_symbol, error_message)
                                self.total_failed_orders += sum(1 for signal in signals if signal['pair'] == json_symbol)
                                continue
                            log_and_print(f"Retrying test order after {MT5_RETRY_DELAY} seconds...", "INFO")
                            await asyncio.sleep(MT5_RETRY_DELAY)

            except Exception as e:
                error_message = f"Error processing symbol {json_symbol}: {str(e)}"
                log_and_print(error_message, "ERROR")
                failed_symbols.append(json_symbol)
                self.save_account_order_error(account_key, json_symbol, error_message)
                self.total_failed_orders += sum(1 for signal in signals if signal['pair'] == json_symbol)

        # Step 2: Group signals by symbol
        symbol_signals = {}
        for signal in signals:
            symbol = signal['pair'].lower()
            if symbol not in symbol_signals:
                symbol_signals[symbol] = []
            signal_allowed_risk = float(signal.get('allowed_risk', 0.0)) if signal.get('allowed_risk') is not None else 0.0
            if signal_allowed_risk in allowed_risk_levels:
                symbol_signals[symbol].append(signal)

        # Step 3: Process signals per symbol, respecting timeframe priority
        for json_symbol in unique_symbols:
            server_symbol = self.get_exact_symbol_match(json_symbol, available_symbols)
            if server_symbol is None or server_symbol not in added_symbols:
                log_and_print(f"Skipping orders for {json_symbol} (server: {server_symbol}) as it was not added to Market Watch", "WARNING")
                continue

            log_and_print(f"Processing orders for symbol {json_symbol} (server: {server_symbol})", "INFO")

            # Get signals for this symbol
            symbol_specific_signals = symbol_signals.get(json_symbol.lower(), [])

            # Group signals by timeframe
            signals_by_timeframe = {}
            for signal in symbol_specific_signals:
                timeframe = signal['timeframe'].lower().replace('4hour', '4hours')
                if timeframe not in signals_by_timeframe:
                    signals_by_timeframe[timeframe] = []
                signals_by_timeframe[timeframe].append(signal)

            # Process signals based on timeframe priority
            signals_processed = False
            for timeframe in timeframe_priority:
                normalized_timeframe = timeframe.lower().replace('4hour', '4hours')
                if normalized_timeframe in signals_by_timeframe:
                    log_and_print(f"Found {len(signals_by_timeframe[normalized_timeframe])} signals for {json_symbol} on {timeframe}, processing...", "INFO")
                    for signal in signals_by_timeframe[normalized_timeframe]:
                        try:
                            signal_allowed_risk = float(signal.get('allowed_risk', 0.0)) if signal.get('allowed_risk') is not None else 0.0
                            if signal_allowed_risk not in allowed_risk_levels:
                                error_message = f"Signal risk {signal_allowed_risk} not allowed for account balance {balance}"
                                log_and_print(f"Skipping order for {json_symbol} ({timeframe}) for {account_key}: {error_message}", "WARNING")
                                self.save_account_order_error(account_key, json_symbol, error_message)
                                self.total_failed_orders += 1
                                continue

                            order_type = signal['order_type']
                            entry_price = signal['entry_price']
                            profit_price = signal['profit_price'] if signal['profit_price'] else None
                            stop_loss = signal['exit_price'] if signal['exit_price'] else None
                            lot_size = signal['lot_size']
                            multiplier = risk_multipliers.get(signal_allowed_risk, 1)
                            adjusted_lot_size = float(lot_size) * multiplier

                            # Pre-validate signal data
                            if not all([json_symbol, order_type, entry_price, lot_size]):
                                error_message = "Invalid signal data: Missing required fields"
                                log_and_print(f"Skipping order for {json_symbol} ({timeframe}) for {account_key}: {error_message}", "ERROR")
                                self.save_account_order_error(account_key, json_symbol, error_message)
                                self.total_failed_orders += 1
                                continue
                            if order_type not in ['buy_limit', 'sell_limit']:
                                error_message = f"Unsupported order type {order_type}"
                                log_and_print(f"Skipping order for {json_symbol} ({timeframe}) for {account_key}: {error_message}", "ERROR")
                                self.save_account_order_error(account_key, json_symbol, error_message)
                                self.total_failed_orders += 1
                                continue
                            if adjusted_lot_size <= 0.0:
                                error_message = f"Invalid adjusted lot size {adjusted_lot_size} (original: {lot_size}, multiplier: {multiplier})"
                                log_and_print(f"Skipping order for {json_symbol} ({timeframe}) for {account_key}: {error_message}", "ERROR")
                                self.save_account_order_error(account_key, json_symbol, error_message)
                                self.total_failed_orders += 1
                                continue

                            success, order_id, error_message, error_category = self.place_pending_order(
                                server_symbol, order_type, entry_price, profit_price, stop_loss, adjusted_lot_size, signal_allowed_risk
                            )
                            if success:
                                pending_orders_placed.append((server_symbol, order_id, order_type, entry_price, profit_price, stop_loss, signal_allowed_risk, multiplier))
                                self.total_priority_timeframes_orders += 1  # Increment priority timeframes counter
                                log_and_print(f"Order placed for {json_symbol} ({timeframe}) for {account_key}: {order_type} at {entry_price}, lot_size={adjusted_lot_size} (multiplier={multiplier})", "SUCCESS")
                            else:
                                log_and_print(f"Failed to place order for {json_symbol} ({timeframe}) for {account_key}: {error_message}", "ERROR")
                                failed_symbols.append(json_symbol)
                                self.save_account_order_error(account_key, server_symbol, error_message)
                                self.total_failed_orders += 1
                                self.save_failed_orders(
                                    server_symbol, order_type, entry_price, profit_price, stop_loss,
                                    adjusted_lot_size, signal_allowed_risk, error_message, error_category
                                )

                        except Exception as e:
                            error_message = f"Error processing signal for {json_symbol} ({timeframe}): {str(e)}"
                            log_and_print(error_message, "ERROR")
                            failed_symbols.append(json_symbol)
                            self.save_account_order_error(account_key, json_symbol, error_message)
                            self.total_failed_orders += 1

                    # Mark signals as processed for this symbol and skip other timeframes
                    signals_processed = True
                    log_and_print(f"Completed processing {timeframe} signals for {json_symbol}, skipping other timeframes", "INFO")
                    break

                if not signals_processed:
                    log_and_print(f"No valid signals found for {json_symbol} in any priority timeframe", "WARNING")

        # Log summary
        log_and_print(f"===== Priority-Timeframe Order Placement Summary for {account_key} =====", "TITLE")
        if added_symbols:
            log_and_print(f"Symbols added to Market Watch: {', '.join(added_symbols)}", "SUCCESS")
        if failed_symbols:
            log_and_print(f"Symbols failed to add or process: {', '.join(set(failed_symbols))}", "ERROR")
        if pending_orders_placed:
            log_and_print(
                f"Pending orders placed: {', '.join([f'{sym} ({otype} at {entry}, TP {tp}, SL {sl}, Risk {risk}, Multiplier {multi})' for sym, oid, otype, entry, tp, sl, risk, multi in pending_orders_placed])}",
                "SUCCESS"
            )
        else:
            log_and_print("No pending orders placed", "INFO")
        log_and_print(f"Total: {len(added_symbols)} symbols added, {len(set(failed_symbols))} failed, {len(pending_orders_placed)} pending orders placed", "INFO")

        return len(added_symbols), len(pending_orders_placed)

async def main():
    """Main function to fetch lot size/risk data, bouncestream signals, initialize MT5, add symbols to watchlist, and place orders for bouncestream accounts."""
    print("\n")
    log_and_print("===== Server Bouncestream Processing Started =====", "TITLE")
    
    # Fetch lot size and allowed risk data first
    if not await executefetchlotsizeandrisk():
        log_and_print("Aborting due to failure in fetching lot size and risk data", "ERROR")
        print("\n")
        return

    # Fetch bouncestream signals
    if not await execute_fetch_bouncestream_signals():
        log_and_print("Aborting due to failure in fetching bouncestream signals", "ERROR")
        print("\n")
        return
    run_validatesignals_main()

    fetcher = ProgrammeFetcher()

    if not fetcher.config.validate_directory():
        log_and_print("Aborting due to invalid directory", "ERROR")
        print("\n")
        return

    active_users = await fetcher.get_active_users()
    if not active_users:
        log_and_print("No active users found", "WARNING")
        print("\n")
        return

    programmes = await fetcher.fetch_user_programmes()
    if not programmes:
        log_and_print("No user programmes fetched, aborting", "ERROR")
        print("\n")
        return

    total_programmes = len(programmes)
    skipped_records = 0
    valid_accounts = []
    for programme in programmes:
        validated = await fetcher.validate_account(programme, active_users)
        if validated:
            valid_accounts.append(validated)
        else:
            skipped_records += 1
    log_and_print(f"Validated {len(valid_accounts)} out of {total_programmes} programme records", "SUCCESS")

    accounts_initialized, accounts_with_symbols, total_orders_placed, accounts_with_orders = await fetcher.process_account_initialization(valid_accounts)

    print("\n")
    log_and_print("===== Processing Summary =====", "TITLE")
    log_and_print(f"Total programme records processed: {total_programmes}", "INFO")
    log_and_print(f"Total bouncestream accounts passed verification: {len(valid_accounts)}", "INFO")
    log_and_print(f"Total accounts MT5 initialized: {accounts_initialized}", "INFO" if accounts_initialized > 0 else "WARNING")
    log_and_print(f"Total accounts with symbols added to Market Watch: {accounts_with_symbols}", "INFO" if accounts_with_symbols > 0 else "WARNING")
    log_and_print(f"Total accounts with pending orders placed: {accounts_with_orders}", "INFO" if accounts_with_orders > 0 else "WARNING")
    log_and_print(f"Total pending orders loaded: {fetcher.total_signals_loaded}", "INFO")
    log_and_print(f"Total pending orders placed: {total_orders_placed}", "INFO" if total_orders_placed > 0 else "WARNING")
    log_and_print(f"Total Alltimeframes orders placed: {fetcher.total_alltimeframes_orders}", "INFO" if fetcher.total_alltimeframes_orders > 0 else "WARNING")
    log_and_print(f"Total Priority-timeframes orders placed: {fetcher.total_priority_timeframes_orders}", "INFO" if fetcher.total_priority_timeframes_orders > 0 else "WARNING")
    log_and_print(f"Total failed pending orders placed: {fetcher.total_failed_orders}", "INFO" if fetcher.total_failed_orders == 0 else "WARNING")
    log_and_print(f"Skipped records: {skipped_records}", "INFO")

    print("\n")
    log_and_print("===== Server Bouncestream Processing completed =====")

if __name__ == "__main__":
    asyncio.run(main())
    