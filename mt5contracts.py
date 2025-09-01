import connectwithinfinitydb as db
import os
from typing import List, Dict, Optional
from colorama import Fore, Style, init
import logging
import time
import asyncio
import json

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

# Configuration Section
EXPORT_DIR = r'C:\xampp\htdocs\CIPHER\cipherdb\cipheruserdb'
RETRY_MAX_ATTEMPTS = 3
RETRY_DELAY = 2
TABLE_NAME = "cipherprogrammes_contracts"

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
    """Manages configuration settings for table column fetching."""
    def __init__(self):
        self.main_export_dir: str = EXPORT_DIR

    def validate_directory(self) -> bool:
        """Validate the export directory exists and is writable."""
        if not os.path.exists(self.main_export_dir) or not os.access(self.main_export_dir, os.W_OK):
            log_and_print(f"Invalid or inaccessible directory: {self.main_export_dir}", "ERROR")
            return False
        return True

# Programme and Contract Fetcher Class
class ProgrammeContractFetcher:
    """Manages fetching and matching programme and contract data."""
    def __init__(self):
        self.config = ConfigManager()

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
                up.programme_markets,
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
                        'timeframe': row.get('timeframe', 'N/A'),
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
        """Match user programmes with contracts based on market and timeframe."""
        matches = {'main_accounts': {}, 'subaccounts': {}}
        main_accounts_with_signals = set()  # Track main accounts with at least one signal
        subaccounts_with_signals = set()    # Track subaccounts with at least one signal

        for programme in programmes:
            user_id = programme['user_id']
            programme_id = programme['programme_id']
            subaccount_id = programme['subaccount_id']
            programme_name = programme['programme'].lower()
            markets = programme['programme_markets'].split(',') if programme['programme_markets'] else []
            timeframe = programme['programme_timeframe'] if programme['programme_timeframe'] else 'N/A'

            # Determine if it's a main account or subaccount
            account_type = 'subaccounts' if subaccount_id else 'main_accounts'
            account_key = f"user_{user_id}_sub_{subaccount_id}" if subaccount_id else f"user_{user_id}"

            if account_key not in matches[account_type]:
                matches[account_type][account_key] = []

            for market in markets:
                market_key = market.lower()
                mapped_market = MARKET_MAPPINGS.get(market_key, market_key)
                # Check all contract types (e.g., bouncestream, momentum, etc.)
                for contract_key, contract_list in contracts.items():
                    contract_market, contract_type = contract_key.split('_')
                    for contract in contract_list:
                        if (contract['marketName'].lower() == mapped_market.lower() and
                            contract['timeframe'] == timeframe and
                            contract['contractType'].lower() == programme_name):
                            matches[account_type][account_key].append({
                                'programme_id': programme_id,
                                'programme': programme['programme'],
                                'market': mapped_market,
                                'timeframe': timeframe,
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

        # Check if there are any matches at all
        if main_accounts_with_signals == 0 and subaccounts_with_signals == 0:
            log_and_print("No signals found for any accounts in contracts table", "WARNING")
            return

        # Print main account matches only if there are matches
        main_accounts_printed = False
        for account_key, match_list in matches['main_accounts'].items():
            if match_list:  # Only print if there are matches
                if not main_accounts_printed:
                    log_and_print("Main Account Matches:", "INFO")
                    main_accounts_printed = True
                user_id = account_key.replace('user_', '')
                log_and_print(f"User ID {user_id}:", "INFO")
                for match in match_list:
                    log_and_print(
                        f"Programme ID {match['programme_id']} ({match['programme']}): "
                        f"Signal available in contracts table - "
                        f"Market: {match['market']}, Timeframe: {match['timeframe']}, "
                        f"Contract ID: {match['contract_id']}, "
                        f"Entry Price: {match['entry_price']}, "
                        f"Exit Price: {match['exit_price']}, "
                        f"Exit Limit Price: {match['exit_limit_price']}, "
                        f"Created At: {match['created_at']}",
                        "DEBUG"
                    )

        # Print subaccount matches only if there are matches
        subaccounts_printed = False
        for account_key, match_list in matches['subaccounts'].items():
            if match_list:  # Only print if there are matches
                if not subaccounts_printed:
                    log_and_print("Subaccount Matches:", "INFO")
                    subaccounts_printed = True
                user_id, sub_id = account_key.replace('user_', '').split('_sub_')
                log_and_print(f"User ID {user_id}, Subaccount ID {sub_id}:", "INFO")
                for match in match_list:
                    log_and_print(
                        f"Programme ID {match['programme_id']} ({match['programme']}): "
                        f"Signal available in contracts table - "
                        f"Market: {match['market']}, Timeframe: {match['timeframe']}, "
                        f"Contract ID: {match['contract_id']}, "
                        f"Entry Price: {match['entry_price']}, "
                        f"Exit Price: {match['exit_price']}, "
                        f"Exit Limit Price: {match['exit_limit_price']}, "
                        f"Created At: {match['created_at']}",
                        "DEBUG"
                    )

        # Print totals
        log_and_print("===== Matching Summary =====", "TITLE")
        log_and_print(f"Total main accounts with signals: {main_accounts_with_signals}", "INFO")
        log_and_print(f"Total subaccounts with signals: {subaccounts_with_signals}", "INFO")
        log_and_print("===== Matching Process Completed =====", "TITLE")

# Main Execution Function
async def main():
    """Main function to fetch and match programmes with contracts."""
    print("\n")
    log_and_print("===== Programme and Contract Matching Process =====", "TITLE")
    fetcher = ProgrammeContractFetcher()

    if not fetcher.config.validate_directory():
        log_and_print("Aborting due to invalid directory", "ERROR")
        return

    # Fetch user programmes
    programmes = await fetcher.fetch_user_programmes()
    if not programmes:
        log_and_print("No user programmes fetched, aborting", "ERROR")
        return

    # Fetch all contracts
    contracts = await fetcher.fetch_all_contracts()
    if not contracts:
        log_and_print("No contracts fetched, aborting", "ERROR")
        return

    # Match programmes with contracts
    matches, main_accounts_with_signals, subaccounts_with_signals = fetcher.match_programmes_with_contracts(programmes, contracts)
    fetcher.print_matches(matches, main_accounts_with_signals, subaccounts_with_signals)

if __name__ == "__main__":
    asyncio.run(main())