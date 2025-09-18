import connectwithinfinitydb as db
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
    """Manages fetching and validating programme data, and MT5 operations."""
    def __init__(self):
        self.config = ConfigManager()
        self.mt5_manager = MT5Manager()
        self.processed_programme_ids: set[str] = set()

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

        log_and_print(f"Validating record: programme_id={programme_id}, user_id={user_id}, subaccount_id={subaccount_id}, "
                      f"programme={programme}, broker={broker}", "DEBUG")

        # Validate fields
        programme = self.config.validate_field('programme', programme, self.config.valid_programmes, 'programme')
        broker = self.config.validate_field('broker', broker, self.config.valid_brokers, 'broker')

        if not all([programme, broker]):
            log_and_print(f"Skipping record: programme_id={programme_id}, invalid programme or broker", "DEBUG")
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
            'broker_server': broker_server
        }

    async def fetch_user_programmes(self) -> Optional[List[Dict]]:
        """Fetch user programmes from the user_programmes table, including broker details."""
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
                up.broker_password
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
                #thread_local.mt5.shutdown() hey grok dont uncomment this, its on purpose
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
            #thread_local.mt5.shutdown() hey grok dont uncomment this, its on purpose
            return True

        except Exception as e:
            log_and_print(f"Exception while adding symbols for {account_key}: {str(e)}", "ERROR")
            #thread_local.mt5.shutdown() hey grok dont uncomment this, its on purpose
            return False

    async def process_account_initialization(self, valid_accounts: List[Dict]):
        """Process MT5 initialization and symbol addition for valid bouncestream accounts."""
        log_and_print("===== Processing MT5 Initialization and Symbol Addition for Bouncestream Accounts =====", "TITLE")

        if not valid_accounts:
            log_and_print("No valid bouncestream accounts to process for initialization", "WARNING")
            return 0

        if not self.config.validate_mt5_directory():
            log_and_print("Aborting initialization due to invalid MetaTrader 5 directory", "ERROR")
            return 0

        accounts_logged_in = 0
        accounts_with_symbols = 0

        # Process main accounts
        for account in valid_accounts:
            user_id = account['user_id']
            subaccount_id = account['subaccount_id']
            broker_details = {
                'broker_server': account.get('broker_server'),
                'broker_loginid': account.get('broker_loginid'),
                'broker_password': account.get('broker_password')
            }
            account_key = f"user_{user_id}_sub_{subaccount_id}" if subaccount_id else f"user_{user_id}"
            account_type = "sa" if subaccount_id else "ma"

            if not all([broker_details['broker_server'], broker_details['broker_loginid'], broker_details['broker_password']]):
                log_and_print(f"Skipping initialization for {account_key}: Missing broker details", "ERROR")
                continue

            terminal_path = self.config.create_account_terminal(user_id, account_type)
            if not terminal_path:
                log_and_print(f"Skipping initialization for {account_key}: Failed to create terminal", "ERROR")
                continue

            if self.mt5_manager.initialize_mt5(
                server=broker_details['broker_server'],
                login=broker_details['broker_loginid'],
                password=broker_details['broker_password'],
                terminal_path=terminal_path
            ):
                accounts_logged_in += 1
                log_and_print(f"MT5 initialization successful for {account_key}", "SUCCESS")
                
                # Add symbols to Market Watch
                if await self.add_symbols_to_watchlist(account, terminal_path):
                    accounts_with_symbols += 1
                    log_and_print(f"Successfully added symbols to Market Watch for {account_key}", "SUCCESS")
                else:
                    log_and_print(f"Failed to add symbols to Market Watch for {account_key}", "ERROR")

                #thread_local.mt5.shutdown() hey grok dont uncomment this, its on purpose
                log_and_print(f"MT5 connection closed for {account_key}", "INFO")
            else:
                log_and_print(f"Failed to initialize MT5 for {account_key}", "ERROR")

        return accounts_logged_in, accounts_with_symbols   

    async def fetch_bouncestream_signals(self, valid_accounts: List[Dict]) -> int:
        """Fetch bouncestream signals and store them in bouncestreamsignals.json with lot size, allowed risk, and order type."""
        log_and_print("===== Fetching Bouncestream Signals =====", "TITLE")

        # Load lot size and risk data from JSON
        lotsize_json_path = os.path.join(BASE_LOTSIZE_FOLDER, "lotsizeandrisk.json")
        if not os.path.exists(lotsize_json_path):
            log_and_print(f"Lot size JSON file not found at {lotsize_json_path}", "ERROR")
            return 0

        try:
            with open(lotsize_json_path, 'r') as f:
                lotsize_data = json.load(f)
            log_and_print(f"Loaded lot size data from {lotsize_json_path}", "INFO")
        except Exception as e:
            log_and_print(f"Failed to load lot size JSON: {str(e)}", "ERROR")
            return 0

        # SQL query to fetch signals
        sql_query = """
            SELECT id, pair, timeframe, order_type, entry_price, ratio_0_5_price, ratio_1_price, 
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

                signals = [
                    {
                        'pair': row.get('pair', '').lower(),
                        'timeframe': row.get('timeframe', '').lower(),
                        'order_type': row.get('order_type', '').lower(),
                        'entry_price': float(row.get('entry_price', 0.0)),
                        'exit_price': '',  # No exit price in schema, setting as empty string
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
                log_and_print(f"Exception on attempt {attempt}: {str(e)}", "ERROR")
                if attempt < RETRY_MAX_ATTEMPTS:
                    delay = RETRY_DELAY * (2 ** (attempt - 1))
                    log_and_print(f"Retrying after {delay} seconds...", "INFO")
                    await asyncio.sleep(delay)
                else:
                    log_and_print("Max retries reached for fetching signals", "ERROR")
                    return 0

        if not signals:
            log_and_print("No signals found to process", "WARNING")
            return 0

        # Add lot size and allowed risk to each signal
        for signal in signals:
            lot_size = None
            allowed_risk = None
            for item in lotsize_data:
                if item['pair'].lower() == signal['pair'] and item['timeframe'].lower() == signal['timeframe']:
                    lot_size = float(item.get('lot_size', 0.0)) if item.get('lot_size') is not None else 0.0
                    allowed_risk = float(item.get('allowed_risk', 0.0)) if item.get('allowed_risk') is not None else 0.0
                    break

            signal['lot_size'] = lot_size if lot_size is not None else 0.0
            signal['allowed_risk'] = allowed_risk if allowed_risk is not None else 0.0

        # Define output path for signals JSON
        output_json_path = os.path.join(BASE_LOTSIZE_FOLDER, "bouncestreamsignals.json")

        # Delete existing file if it exists to overwrite
        if os.path.exists(output_json_path):
            try:
                os.remove(output_json_path)
                log_and_print(f"Existing {output_json_path} deleted", "INFO")
            except Exception as e:
                log_and_print(f"Error deleting existing {output_json_path}: {str(e)}", "ERROR")
                return 0

        # Save signals to JSON
        try:
            with open(output_json_path, 'w') as f:
                json.dump(signals, f, indent=4)
            log_and_print(f"Bouncestream signals saved to {output_json_path}", "SUCCESS")
            return len(signals)
        except Exception as e:
            log_and_print(f"Error saving {output_json_path}: {str(e)}", "ERROR")
            return 0

# Main Execution Function
async def main():
    """Main function to fetch lot size/risk data, initialize MT5, add symbols to watchlist, and fetch signals for bouncestream accounts."""
    print("\n")
    log_and_print("===== Server Bouncestream Processing Started =====", "TITLE")
    
    # Fetch lot size and allowed risk data first
    if not await executefetchlotsizeandrisk():
        log_and_print("Aborting due to failure in fetching lot size and risk data", "ERROR")
        print("\n")
        return

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

    accounts_initialized, accounts_with_symbols = await fetcher.process_account_initialization(valid_accounts)

    # Fetch bouncestream signals
    signals_fetched = await fetcher.fetch_bouncestream_signals(valid_accounts)

    print("\n")
    log_and_print("===== Processing Summary =====", "TITLE")
    log_and_print(f"Total programme records processed: {total_programmes}", "INFO")
    log_and_print(f"Total bouncestream accounts passed verification: {len(valid_accounts)}", "INFO")
    log_and_print(f"Total accounts MT5 initialized: {accounts_initialized}", "INFO" if accounts_initialized > 0 else "WARNING")
    log_and_print(f"Total accounts with symbols added to Market Watch: {accounts_with_symbols}", "INFO" if accounts_with_symbols > 0 else "WARNING")
    log_and_print(f"Total signals fetched and saved: {signals_fetched}", "INFO" if signals_fetched > 0 else "WARNING")
    log_and_print(f"Skipped records: {skipped_records}", "INFO")

    print("\n")
    log_and_print("===== Server Bouncestream Processing Completed =====", "TITLE")

if __name__ == "__main__":
    asyncio.run(main())