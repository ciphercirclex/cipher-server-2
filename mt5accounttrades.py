import connectwithinfinitydb as db
import MetaTrader5 as mt5
import os
import shutil
from typing import List, Dict, Optional
from colorama import Fore, Style, init
import logging
import time
import asyncio
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
TABLE_NAME = "cipherprogrammes_contracts"
ORIGINAL_MT5_DIR = r"C:\xampp\htdocs\CIPHER\metaTrader5\MetaTrader 5"  # Source MetaTrader 5 directory
BASE_MT5_DIR = r"C:\xampp\htdocs\CIPHER\metaTrader5\users"  # Base directory for account-specific MT5 installations
MAX_RETRIES = 5
MT5_RETRY_DELAY = 3

# Market name mappings
MARKET_MAPPINGS = {
    'volatility10index': 'Volatility 10 Index',
    'volatility25index': 'Volatility 25 Index',
    'volatility50index': 'Volatility 50 Index',
    'volatility75index': 'Volatility 75 Index',
    'volatility100index': 'Volatility 100 Index',
    'driftswitchindex10': 'Drift Switch Index 10',
    'driftswitchindex20': 'Drift Switch Index 20',
    'driftswitchindex30': 'Drift Switch Index 30',
    'multistep2index': 'Multi Step 2 Index',
    'multistep4index': 'Multi Step 4 Index',
    'stepindex': 'Step Index',
    'usdjpy': 'USDJPY',
    'usdcad': 'USDCAD',
    'usdchf': 'USDCHF',
    'eurusd': 'EURUSD',
    'gbpusd': 'GBPUSD',
    'audusd': 'AUDUSD',
    'nzdusd': 'NZDUSD',
    'xauusd': 'XAUUSD',
    'ustech100': 'US Tech 100',
    'wallstreet30': 'Wall Street 30',
    'audjpy': 'AUDJPY',
    'audnzd': 'AUDNZD',
    'eurchf': 'EURCHF',
    'eurgbp': 'EURGBP',
    'eurjpy': 'EURJPY',
    'gbpjpy': 'GBPJPY'
}

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
    """Manages configuration settings for table column fetching, terminal copying, and validation."""
    def __init__(self):
        self.main_export_dir: str = EXPORT_DIR
        self.base_mt5_dir: str = BASE_MT5_DIR
        self.original_mt5_dir: str = ORIGINAL_MT5_DIR
        self.valid_programmes: List[str] = ['bouncestream', 'momentum', 'outstream', 'baseflow', 'marketwave', 'counterforce']
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
    """Manages MT5 initialization, login, and watchlist operations."""
    def initialize_mt5(self, server: str, login: str, password: str, terminal_path: str) -> bool:
        """Initialize MT5 terminal and login with provided credentials using the specified terminal path."""
        log_and_print(f"Attempting MT5 login for server: {server}, login: {login} using {terminal_path}", "INFO")
        
        # Use thread-local MT5 instance to avoid conflicts
        if not hasattr(thread_local, 'mt5'):
            thread_local.mt5 = mt5

        # Ensure no existing MT5 connections interfere
        try:
            thread_local.mt5.shutdown()
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
            thread_local.mt5.shutdown()
            return False

        # Login is handled during initialization
        log_and_print(f"Successfully logged in to MT5 for server: {server}, login: {login}", "SUCCESS")
        return True

    def get_available_symbols(self) -> List[str]:
        """Fetch and return all available symbols on the server."""
        try:
            symbols = thread_local.mt5.symbols_get()
            if symbols:
                symbol_names = [s.name for s in symbols]
                return symbol_names
            else:
                log_and_print("No symbols retrieved from server", "ERROR")
                return []
        except Exception as e:
            log_and_print(f"Error fetching available symbols: {str(e)}", "ERROR")
            return []

    def add_symbol_to_watchlist(self, market: str, programme_id: str, watchlist_results: Dict) -> tuple[bool, str]:
        """Add a market symbol to the MT5 watchlist or verify it exists, storing results for later printing."""
        try:
            # Check if symbol is available on the server
            available_symbols = self.get_available_symbols()
            if market not in available_symbols:
                log_and_print(f"Symbol {market} is not available on the server for programme ID {programme_id}", "ERROR")
                if programme_id not in watchlist_results:
                    watchlist_results[programme_id] = {'success': [], 'failed': []}
                watchlist_results[programme_id]['failed'].append(f"Symbol {market} not available")
                return False, f"Symbol {market} not available"

            # Check if symbol is already in Market Watch and visible
            symbol_info = thread_local.mt5.symbol_info(market)
            if symbol_info is not None and symbol_info.visible:
                log_and_print(f"Symbol {market} already exists and is visible in Market Watch for programme ID {programme_id}", "INFO")
                if programme_id not in watchlist_results:
                    watchlist_results[programme_id] = {'success': [], 'failed': []}
                watchlist_results[programme_id]['success'].append(f"Symbol {market} already in watchlist")
                return True, f"Symbol {market} already in watchlist"

            # Try to select the symbol to make it visible
            for _ in range(2):  # Retry once if selection fails
                if thread_local.mt5.symbol_select(market, True):
                    log_and_print(f"Successfully added {market} to Market Watch (made visible) for programme ID {programme_id}", "SUCCESS")
                    if programme_id not in watchlist_results:
                        watchlist_results[programme_id] = {'success': [], 'failed': []}
                    watchlist_results[programme_id]['success'].append(f"Symbol {market} added to watchlist")
                    return True, f"Symbol {market} added to watchlist"
                else:
                    log_and_print(f"Failed to select {market}, error: {thread_local.mt5.last_error()}. Retrying...", "WARNING")
                    time.sleep(1)  # Brief delay before retry
            else:
                log_and_print(f"Symbol {market} could not be added to Market Watch after retries for programme ID {programme_id}, error: {thread_local.mt5.last_error()}", "ERROR")
                if programme_id not in watchlist_results:
                    watchlist_results[programme_id] = {'success': [], 'failed': []}
                watchlist_results[programme_id]['failed'].append(f"Symbol {market} could not be added")
                return False, f"Symbol {market} could not be added"
        except Exception as e:
            log_and_print(f"Error processing symbol {market} for programme ID {programme_id}: {str(e)}", "ERROR")
            if programme_id not in watchlist_results:
                watchlist_results[programme_id] = {'success': [], 'failed': []}
            watchlist_results[programme_id]['failed'].append(f"Error processing {market}: {str(e)}")
            return False, f"Error processing {market}: {str(e)}"

# Programme and Contract Fetcher Class
class ProgrammeContractFetcher:
    """Manages fetching and matching programme and contract data, and MT5 operations."""
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
        programme_markets = record['programme_markets']

        log_and_print(f"Validating record: programme_id={programme_id}, user_id={user_id}, subaccount_id={subaccount_id}, "
                      f"programme={programme}, broker={broker}, programme_markets={programme_markets}", "DEBUG")

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

        return {
            'programme_id': programme_id,
            'user_id': user_id,
            'subaccount_id': subaccount_id,
            'programme': programme,
            'broker': broker,
            'broker_loginid': broker_loginid,
            'broker_password': broker_password,
            'broker_server': broker_server,
            'programme_markets': programme_markets
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
                up.programme_markets,
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
                    
                # Handle both 'data' and 'results' keys
                rows = None
                if 'data' in result and 'rows' in result['data'] and isinstance(result['data']['rows'], list):
                    rows = result['data']['rows']
                elif 'results' in result and isinstance(result['results'], list):
                    rows = result['results']
                else:
                    log_and_print(f"Invalid or missing rows in result on attempt {attempt}: {json.dumps(result, indent=2)}", "ERROR")
                    continue
                
                # Normalize rows to handle string 'None' values
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

    async def fetch_table_columns(self) -> Optional[List[str]]:
        """Fetch the column names for the cipherprogrammes_contracts table."""
        sql_query = f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{TABLE_NAME}'
        """
        log_and_print(f"Fetching columns for table '{TABLE_NAME}' with query: {sql_query}", "INFO")
        
        for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
            try:
                result = db.execute_query(sql_query)
                log_and_print(f"Raw query result for columns: {json.dumps(result, indent=2)}", "DEBUG")
                
                if not isinstance(result, dict):
                    log_and_print(f"Invalid result format on attempt {attempt}: Expected dict, got {type(result)}", "ERROR")
                    continue
                
                if result.get('status') != 'success':
                    error_message = result.get('message', 'No message provided')
                    log_and_print(f"Failed to fetch columns for table '{TABLE_NAME}' on attempt {attempt}: {error_message}", "ERROR")
                    continue
                
                # Handle both 'data' and 'results' keys
                columns_data = None
                if 'data' in result and 'rows' in result['data'] and isinstance(result['data']['rows'], list):
                    columns_data = result['data']['rows']
                elif 'results' in result and isinstance(result['results'], list):
                    columns_data = result['results']
                else:
                    log_and_print(f"Invalid or missing columns data in result on attempt {attempt}: {json.dumps(result, indent=2)}", "ERROR")
                    continue
                
                columns = [col['COLUMN_NAME'] for col in columns_data]
                log_and_print(f"Successfully fetched {len(columns)} columns for table '{TABLE_NAME}': {columns}", "SUCCESS")
                return columns
                
            except Exception as e:
                log_and_print(f"Exception on attempt {attempt}: {str(e)}", "ERROR")
                
            if attempt < RETRY_MAX_ATTEMPTS:
                delay = RETRY_DELAY * (2 ** (attempt - 1))
                log_and_print(f"Retrying after {delay} seconds...", "INFO")
                await asyncio.sleep(delay)
            else:
                log_and_print(f"Max retries reached for fetching columns of table '{TABLE_NAME}'", "ERROR")
                return None
        return None

    def parse_contract_data(self, contract: str) -> Dict[str, str]:
        """Parse contract data string into structured fields."""
        if not contract:
            return {
                'marketName': 'N/A',
                'contractType': 'N/A',
                'timeframe': 'N/A',
                'entryPrice': 'N/A',
                'exitPrice': 'N/A',
                'exitLimitPrice': 'N/A'
            }
        parts = contract.split(', ')
        parsed = {
            'marketName': 'N/A',
            'contractType': 'N/A',
            'timeframe': 'N/A',
            'entryPrice': 'N/A',
            'exitPrice': 'N/A',
            'exitLimitPrice': 'N/A'
        }
        for part in parts:
            if part.startswith('contract type: '):
                parsed['contractType'] = part.replace('contract type: ', '')
            elif part.startswith('market name: '):
                raw_market = part.replace('market name: ', '').lower()
                parsed['marketName'] = MARKET_MAPPINGS.get(raw_market, raw_market)
            elif part.startswith('timeframe: '):
                parsed['timeframe'] = part.replace('timeframe: ', '')
            elif part.startswith('entry price: '):
                parsed['entryPrice'] = part.replace('entry price: ', '')
            elif part.startswith('exit price: '):
                parsed['exitPrice'] = part.replace('exit price: ', '')
            elif part.startswith('exit-limit price: '):
                parsed['exitLimitPrice'] = part.replace('exit-limit price: ', '')
        return parsed

    async def fetch_contracts(self, market: str, contract_type: str) -> Optional[List[Dict]]:
        """Fetch contracts for a specific market and contract type."""
        column_name = f"{market}_{contract_type}contracts"
        sql_query = f"""
            SELECT id, {column_name}, timeframe, created_at
            FROM {TABLE_NAME}
            WHERE {column_name} IS NOT NULL
        """
        log_and_print(f"Fetching contracts for '{market}_{contract_type}' with query: {sql_query}", "INFO")

        for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
            try:
                result = db.execute_query(sql_query)
                log_and_print(f"Raw query result for contracts: {json.dumps(result, indent=2)}", "DEBUG")
                
                if not isinstance(result, dict):
                    log_and_print(f"Invalid result format on attempt {attempt}: Expected dict, got {type(result)}", "ERROR")
                    continue
                
                if result.get('status') != 'success':
                    error_message = result.get('message', 'No message provided')
                    log_and_print(f"Failed to fetch contracts for '{market}_{contract_type}' on attempt {attempt}: {error_message}", "ERROR")
                    continue
                
                # Handle both 'data' and 'results' keys
                contracts_data = None
                if 'data' in result and 'rows' in result['data'] and isinstance(result['data']['rows'], list):
                    contracts_data = result['data']['rows']
                elif 'results' in result and isinstance(result['results'], list):
                    contracts_data = result['results']
                else:
                    log_and_print(f"Invalid or missing contracts data in result on attempt {attempt}: {json.dumps(result, indent=2)}", "ERROR")
                    continue
                
                contracts = []
                for row in contracts_data:
                    parsed_contract = self.parse_contract_data(row.get(column_name, ''))
                    contracts.append({
                        'id': row.get('id', 'N/A'),
                        'marketName': parsed_contract['marketName'],
                        'contractType': parsed_contract['contractType'],
                        'timeframe': parsed_contract['timeframe'],
                        'entryPrice': parsed_contract['entryPrice'],
                        'exitPrice': parsed_contract['exitPrice'],
                        'exitLimitPrice': parsed_contract['exitLimitPrice'],
                        'created_at': row.get('created_at', 'N/A')
                    })
                log_and_print(f"Successfully fetched {len(contracts)} contracts for '{market}_{contract_type}'", "SUCCESS")
                return contracts
                
            except Exception as e:
                log_and_print(f"Exception on attempt {attempt}: {str(e)}", "ERROR")
                
            if attempt < RETRY_MAX_ATTEMPTS:
                delay = RETRY_DELAY * (2 ** (attempt - 1))
                log_and_print(f"Retrying after {delay} seconds...", "INFO")
                await asyncio.sleep(delay)
            else:
                log_and_print(f"Max retries reached for fetching contracts for '{market}_{contract_type}'", "ERROR")
                return None
        return None

    async def fetch_all_contracts(self) -> Dict[str, List[Dict]]:
        """Fetch all contracts across forex and deriv markets."""
        columns = await self.fetch_table_columns()
        if not columns:
            log_and_print(f"Cannot fetch contracts due to missing column information", "ERROR")
            return {}

        contract_columns = [col for col in columns if col.endswith('contracts') and ('forex' in col or 'deriv' in col)]
        results = {}

        for column in contract_columns:
            market, contract_type = column.replace('contracts', '').split('_')
            contracts = await self.fetch_contracts(market, contract_type)
            if contracts:
                results[f"{market}_{contract_type}"] = contracts
            else:
                results[f"{market}_{contract_type}"] = []

        return results

    def match_programmes_with_contracts(self, programmes: List[Dict], contracts: Dict[str, List[Dict]]) -> Dict:
        """Match user programmes with contracts based on market."""
        matches = {'main_accounts': {}, 'subaccounts': {}}
        main_accounts_with_signals = set()  # Track main accounts with at least one signal
        subaccounts_with_signals = set()    # Track subaccounts with at least one signal

        for programme in programmes:
            user_id = programme['user_id']
            programme_id = programme['programme_id']
            subaccount_id = programme['subaccount_id']
            programme_name = programme['programme'].lower()
            markets = programme['programme_markets'].split(',') if programme['programme_markets'] else []

            # Store broker details
            broker_details = {
                'broker_server': programme.get('broker_server'),
                'broker_loginid': programme.get('broker_loginid'),
                'broker_password': programme.get('broker_password')
            }

            # Determine if it's a main account or subaccount
            account_type = 'subaccounts' if subaccount_id else 'main_accounts'
            account_key = f"user_{user_id}_sub_{subaccount_id}" if subaccount_id else f"user_{user_id}"

            if account_key not in matches[account_type]:
                matches[account_type][account_key] = {'matches': [], 'broker_details': broker_details, 'user_id': user_id}

            for market in markets:
                market_key = market.lower()
                mapped_market = MARKET_MAPPINGS.get(market_key, market_key)
                # Check all contract types
                for contract_key, contract_list in contracts.items():
                    contract_market, contract_type = contract_key.split('_')
                    for contract in contract_list:
                        if (contract['marketName'].lower() == mapped_market.lower() and
                            contract['contractType'].lower() == programme_name):
                            matches[account_type][account_key]['matches'].append({
                                'programme_id': programme_id,
                                'programme': programme['programme'],
                                'market': mapped_market,
                                'contract_id': contract['id'],
                                'entry_price': contract['entryPrice'],
                                'exit_price': contract['exitPrice'],
                                'exit_limit_price': contract['exitLimitPrice'],
                                'created_at': contract['created_at']
                            })
                            # Add to the set of accounts with signals
                            if account_type == 'main_accounts':
                                main_accounts_with_signals.add(account_key)
                            else:
                                subaccounts_with_signals.add(account_key)

        return matches, len(main_accounts_with_signals), len(subaccounts_with_signals)

    def print_matches(self, matches: Dict, main_accounts_with_signals: int, subaccounts_with_signals: int):
        """Print matched programmes and contracts only for accounts with matches."""
        log_and_print("===== Programme and Contract Matches =====", "TITLE")

        if main_accounts_with_signals == 0 and subaccounts_with_signals == 0:
            log_and_print("No signals found for any accounts in contracts table", "WARNING")
            return

        # Print main account matches
        main_accounts_printed = False
        for account_key, data in matches['main_accounts'].items():
            match_list = data['matches']
            if match_list:
                if not main_accounts_printed:
                    log_and_print("Main Account Matches:", "INFO")
                    main_accounts_printed = True
                user_id = account_key.replace('user_', '')
                log_and_print(f"User ID {user_id} (Broker Server: {data['broker_details']['broker_server']}, Login: {data['broker_details']['broker_loginid']}):", "INFO")
                for match in match_list:
                    log_and_print(
                        f"Programme ID {match['programme_id']} ({match['programme']}): "
                        f"Signal available in contracts table - "
                        f"Market: {match['market']}, "
                        f"Contract ID: {match['contract_id']}, "
                        f"Entry Price: {match['entry_price']}, "
                        f"Exit Price: {match['exit_price']}, "
                        f"Exit Limit Price: {match['exit_limit_price']}, "
                        f"Created At: {match['created_at']}",
                        "DEBUG"
                    )

        # Print subaccount matches
        subaccounts_printed = False
        for account_key, data in matches['subaccounts'].items():
            match_list = data['matches']
            if match_list:
                if not subaccounts_printed:
                    log_and_print("Subaccount Matches:", "INFO")
                    subaccounts_printed = True
                user_id, sub_id = account_key.replace('user_', '').split('_sub_')
                log_and_print(f"User ID {user_id}, Subaccount ID {sub_id} (Broker Server: {data['broker_details']['broker_server']}, Login: {data['broker_details']['broker_loginid']}):", "INFO")
                for match in match_list:
                    log_and_print(
                        f"Programme ID {match['programme_id']} ({match['programme']}): "
                        f"Signal available in contracts table - "
                        f"Market: {match['market']}, "
                        f"Contract ID: {match['contract_id']}, "
                        f"Entry Price: {match['entry_price']}, "
                        f"Exit Price: {match['exit_price']}, "
                        f"Exit Limit Price: {match['exit_limit_price']}, "
                        f"Created At: {match['created_at']}",
                        "DEBUG"
                    )

    async def process_account_watchlist(self, matches: Dict, main_accounts_with_signals: int, subaccounts_with_signals: int, valid_accounts: List[Dict]):
        """Process watchlist addition for accounts with matched signals using account-specific terminal paths."""
        log_and_print("===== Processing Watchlist for Accounts with Signals =====", "TITLE")

        if main_accounts_with_signals == 0 and subaccounts_with_signals == 0:
            log_and_print("No accounts with signals to process for watchlist", "WARNING")
            return [], {}, {}

        # Validate original MetaTrader 5 directory
        if not self.config.validate_mt5_directory():
            log_and_print("Aborting watchlist processing due to invalid MetaTrader 5 directory", "ERROR")
            return [], {}, {}

        accounts_logged_in = 0
        server_symbols = {}  # Store symbols per server for debugging
        unavailable_symbols_all = {}  # Store unavailable symbols per programme
        watchlist_results = {}  # Store watchlist addition results for later printing

        # Process main accounts
        for account_key, data in matches['main_accounts'].items():
            match_list = data['matches']
            if match_list:
                broker_details = data['broker_details']
                user_id = data['user_id']
                if not all([broker_details['broker_server'], broker_details['broker_loginid'], broker_details['broker_password']]):
                    log_and_print(f"Skipping watchlist processing for {account_key}: Missing broker details", "ERROR")
                    continue

                # Check if the account is valid
                valid_account = next((acc for acc in valid_accounts if acc['user_id'] == str(user_id) and acc['subaccount_id'] is None), None)
                if not valid_account:
                    log_and_print(f"Skipping watchlist processing for {account_key}: Account not valid", "ERROR")
                    continue

                # Create account-specific terminal
                terminal_path = self.config.create_account_terminal(user_id, "ma")
                if not terminal_path:
                    log_and_print(f"Skipping watchlist processing for {account_key}: Failed to create terminal", "ERROR")
                    continue

                # Initialize MT5 with broker credentials and account-specific terminal path
                if self.mt5_manager.initialize_mt5(
                    server=broker_details['broker_server'],
                    login=broker_details['broker_loginid'],
                    password=broker_details['broker_password'],
                    terminal_path=terminal_path
                ):
                    accounts_logged_in += 1
                    # Store available symbols for the server before watchlist processing
                    server = broker_details['broker_server']
                    available_symbols = self.mt5_manager.get_available_symbols()
                    if server and available_symbols:
                        server_symbols[server] = available_symbols
                    # Add markets from matches to watchlist
                    markets = set(match['market'] for match in match_list)
                    log_and_print(f"Processing watchlist for {account_key} with markets: {', '.join(markets)}", "INFO")
                    unavailable_symbols = []
                    for market in markets:
                        success, message = self.mt5_manager.add_symbol_to_watchlist(market, valid_account['programme_id'], watchlist_results)
                        if not success:
                            unavailable_symbols.append(market)
                    if unavailable_symbols:
                        unavailable_symbols_all[valid_account['programme_id']] = unavailable_symbols
                    thread_local.mt5.shutdown()  # Close MT5 connection after processing
                    log_and_print(f"MT5 connection closed for {account_key}", "INFO")
                else:
                    log_and_print(f"Failed to initialize MT5 for {account_key}", "ERROR")

        # Process subaccounts
        for account_key, data in matches['subaccounts'].items():
            match_list = data['matches']
            if match_list:
                broker_details = data['broker_details']
                user_id = data['user_id']
                subaccount_id = account_key.split('_sub_')[1]
                if not all([broker_details['broker_server'], broker_details['broker_loginid'], broker_details['broker_password']]):
                    log_and_print(f"Skipping watchlist processing for {account_key}: Missing broker details", "ERROR")
                    continue

                # Check if the account is valid
                valid_account = next((acc for acc in valid_accounts if acc['user_id'] == str(user_id) and acc['subaccount_id'] == subaccount_id), None)
                if not valid_account:
                    log_and_print(f"Skipping watchlist processing for {account_key}: Account not valid", "ERROR")
                    continue

                # Create account-specific terminal
                terminal_path = self.config.create_account_terminal(user_id, "sa")
                if not terminal_path:
                    log_and_print(f"Skipping watchlist processing for {account_key}: Failed to create terminal", "ERROR")
                    continue

                # Initialize MT5 with broker credentials and account-specific terminal path
                if self.mt5_manager.initialize_mt5(
                    server=broker_details['broker_server'],
                    login=broker_details['broker_loginid'],
                    password=broker_details['broker_password'],
                    terminal_path=terminal_path
                ):
                    accounts_logged_in += 1
                    # Store available symbols for the server before watchlist processing
                    server = broker_details['broker_server']
                    available_symbols = self.mt5_manager.get_available_symbols()
                    if server and available_symbols:
                        server_symbols[server] = available_symbols
                    # Add markets from matches to watchlist
                    markets = set(match['market'] for match in match_list)
                    log_and_print(f"Processing watchlist for {account_key} with markets: {', '.join(markets)}", "INFO")
                    unavailable_symbols = []
                    for market in markets:
                        success, message = self.mt5_manager.add_symbol_to_watchlist(market, valid_account['programme_id'], watchlist_results)
                        if not success:
                            unavailable_symbols.append(market)
                    if unavailable_symbols:
                        unavailable_symbols_all[valid_account['programme_id']] = unavailable_symbols
                    thread_local.mt5.shutdown()  # Close MT5 connection after processing
                    log_and_print(f"MT5 connection closed for {account_key}", "INFO")
                else:
                    log_and_print(f"Failed to initialize MT5 for {account_key}", "ERROR")

        return accounts_logged_in, server_symbols, unavailable_symbols_all, watchlist_results

# Main Execution Function
async def main():
    """Main function to fetch, match programmes with contracts, and process watchlists."""
    print("\n")
    log_and_print("===== Server Contracts Processing Started =====", "TITLE")
    fetcher = ProgrammeContractFetcher()

    if not fetcher.config.validate_directory():
        log_and_print("Aborting due to invalid directory", "ERROR")
        print("\n")
        return

    # Fetch active users
    active_users = await fetcher.get_active_users()
    if not active_users:
        log_and_print("No active users found", "WARNING")
        print("\n")
        return

    # Fetch user programmes
    programmes = await fetcher.fetch_user_programmes()
    if not programmes:
        log_and_print("No user programmes fetched, aborting", "ERROR")
        print("\n")
        return

    # Validate programmes
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

    # Fetch all contracts
    contracts = await fetcher.fetch_all_contracts()
    if not contracts:
        log_and_print("No contracts fetched, aborting", "ERROR")
        print("\n")
        return

    # Match programmes with contracts
    matches, main_accounts_with_signals, subaccounts_with_signals = fetcher.match_programmes_with_contracts(valid_accounts, contracts)
    fetcher.print_matches(matches, main_accounts_with_signals, subaccounts_with_signals)

    # Process watchlist for accounts with signals
    accounts_logged_in, server_symbols, unavailable_symbols_all, watchlist_results = await fetcher.process_account_watchlist(matches, main_accounts_with_signals, subaccounts_with_signals, valid_accounts)

    # Print summary in the desired order
    print("\n")
    log_and_print("===== Server Symbols =====", "TITLE")
    for server, symbols in server_symbols.items():
        log_and_print(f"Server: {server}, Available Symbols ({len(symbols)}): {', '.join(symbols)}", "INFO")

    print("\n")
    log_and_print("===== Watchlist Processing Results =====", "TITLE")
    for programme_id, results in watchlist_results.items():
        log_and_print(f"Programme ID {programme_id}:", "INFO")
        for message in results['success']:
            log_and_print(f"{message}", "SUCCESS")
        for message in results['failed']:
            log_and_print(f"{message}", "ERROR")

    print("\n")
    log_and_print("===== Unavailable Symbols =====", "TITLE")
    for programme_id, symbols in unavailable_symbols_all.items():
        log_and_print(f"Programme ID {programme_id}: Unavailable Symbols ({len(symbols)}): {', '.join(symbols)}", "WARNING")

    print("\n")
    log_and_print("===== Processing Summary =====", "TITLE")
    log_and_print(f"Total programme records processed: {total_programmes}", "INFO")
    log_and_print(f"Total accounts passed verification: {len(valid_accounts)}", "INFO")
    log_and_print(f"Total accounts successfully logged in: {accounts_logged_in}", "INFO" if accounts_logged_in > 0 else "WARNING")
    log_and_print(f"Total main accounts with signals: {main_accounts_with_signals}", "INFO" if main_accounts_with_signals > 0 else "WARNING")
    log_and_print(f"Total subaccounts with signals: {subaccounts_with_signals}", "INFO" if subaccounts_with_signals > 0 else "WARNING")
    log_and_print(f"Skipped records: {skipped_records}", "INFO")

    print("\n")
    log_and_print("===== Server Contracts Processing Completed =====", "TITLE")

if __name__ == "__main__":
    asyncio.run(main())