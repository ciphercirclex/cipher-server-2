import connectwithinfinitydb as db
import os
import shutil
import MetaTrader5 as mt5
from datetime import datetime
from colorama import Fore, Style, init
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time

# Initialize colorama for colored console output
init()

# Configuration Section
# Define paths and settings for MetaTrader 5 and concurrency
source_mt5_path = r'C:\xampp\htdocs\CIPHER\metaTrader5\MetaTrader 5'
destination_mt5_base = r'C:\xampp\htdocs\CIPHER\metaTrader5'
MAX_WORKERS = 4
MAX_RETRIES = 5
RETRY_DELAY = 3

# Thread-local Storage Setup
# Initialize thread-local storage for MT5 instances
thread_local = threading.local()

# Logging Function
def log_and_print(message, level="INFO"):
    """Helper function to print formatted messages with color coding and spacing."""
    indent = "    "
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    level_colors = {
        "INFO": Fore.CYAN,
        "SUCCESS": Fore.GREEN,
        "WARNING": Fore.YELLOW,
        "ERROR": Fore.RED,
        "TITLE": Fore.MAGENTA,
        "DEBUG": Fore.LIGHTBLACK_EX
    }
    color = level_colors.get(level, Fore.WHITE)
    formatted_message = f"[ {timestamp} ] │ {level:7} │ {indent}{message}"
    print(f"{color}{formatted_message}{Style.RESET_ALL}")

# MetaTrader Directory Management
def get_mt5_directory(user_id, subaccount_id=None):
    """Get or create the MetaTrader 5 directory for the user or subaccount."""
    folder_name = f"MetaTrader 5 id{user_id}" if not subaccount_id or subaccount_id == 'NULL' else f"MetaTrader 5 id{user_id} SA{subaccount_id}"
    destination_path = os.path.join(destination_mt5_base, folder_name)
    
    log_and_print(f"Checking MT5 directory for user_id={user_id}, subaccount_id={subaccount_id or 'N/A'}: {destination_path}", "DEBUG")
    if os.path.exists(destination_path):
        log_and_print(f"MetaTrader 5 directory already exists: {destination_path}", "INFO")
        terminal_path = os.path.join(destination_path, 'terminal64.exe')
        if os.path.exists(terminal_path):
            log_and_print(f"Terminal executable found: {terminal_path}", "DEBUG")
            return destination_path
        else:
            log_and_print(f"Terminal executable not found: {terminal_path}", "ERROR")
            return None
    
    try:
        if not os.path.exists(source_mt5_path):
            log_and_print(f"Source MetaTrader 5 directory does not exist: {source_mt5_path}", "ERROR")
            return None
        
        shutil.copytree(source_mt5_path, destination_path, dirs_exist_ok=True)
        log_and_print(f"Successfully copied MetaTrader 5 directory to {destination_path}", "SUCCESS")
        terminal_path = os.path.join(destination_path, 'terminal64.exe')
        if os.path.exists(terminal_path):
            log_and_print(f"Terminal executable created: {terminal_path}", "DEBUG")
            return destination_path
        else:
            log_and_print(f"Terminal executable not created: {terminal_path}", "ERROR")
            return None
    except Exception as e:
        log_and_print(f"Failed to copy MetaTrader 5 directory for user_id={user_id}, subaccount_id={subaccount_id or 'N/A'}: {str(e)}", "ERROR")
        return None

# User Rank Retrieval
def get_user_rank(user_id):
    """Retrieve the user_rank for a given user_id from the users table."""
    sql_query = f"""
        SELECT user_rank
        FROM users
        WHERE id = '{user_id}'
    """
    log_and_print(f"Retrieving user_rank for user_id={user_id}", "DEBUG")
    result = db.execute_query(sql_query)
    
    if result['status'] == 'success' and isinstance(result['results'], list) and result['results']:
        user_rank = result['results'][0]['user_rank'].lower().strip()
        log_and_print(f"Found user_rank='{user_rank}' for user_id={user_id}", "DEBUG")
        return user_rank
    log_and_print(f"No user_rank found for user_id={user_id}: {result.get('message', 'No message')}", "WARNING")
    return None

# Programme Balance Information Retrieval
def get_programme_balance_info(programme_id):
    """Get current balance information for a programme from user_programmes."""
    sql_query = f"""
        SELECT initial_balance, current_balance, profit_and_loss, initialbalance_status
        FROM user_programmes
        WHERE id = '{programme_id}'
    """
    log_and_print(f"Retrieving balance info for programme_id={programme_id}", "DEBUG")
    result = db.execute_query(sql_query)
    
    if result['status'] == 'success' and isinstance(result['results'], list) and result['results']:
        row = result['results'][0]
        initial_balance = float(row['initial_balance']) if row['initial_balance'] is not None and row['initial_balance'] != '0.00' else None
        current_balance = float(row['current_balance']) if row['current_balance'] is not None else 0.00
        profit_and_loss = float(row['profit_and_loss']) if row['profit_and_loss'] is not None else 0.00
        initialbalance_status = row['initialbalance_status'].lower().strip() if row['initialbalance_status'] else 'none'
        
        log_and_print(f"Programme balance info for programme_id={programme_id}: initial_balance={initial_balance}, "
                      f"current_balance={current_balance}, profit_and_loss={profit_and_loss}, "
                      f"initialbalance_status='{initialbalance_status}'", "DEBUG")
        
        return {
            'initial_balance': initial_balance,
            'current_balance': current_balance,
            'profit_and_loss': profit_and_loss,
            'initialbalance_status': initialbalance_status
        }
    log_and_print(f"Could not retrieve balance info for programme_id={programme_id}: {result.get('message', 'No message')}", "ERROR")
    return None

# Broker Balance Verification
def verify_broker_balance(broker_login, broker_password, broker_server, terminal_path, user_id, subaccount_id=None, programme_id=None, user_rank=None, balance_info=None):
    """Log in to MT5, check balance, and return update parameters."""
    log_and_print(f"Processing MT5 balance verification for user_id={user_id}, subaccount_id={subaccount_id or 'N/A'}, programme_id={programme_id}", "INFO")
    
    if not os.path.exists(terminal_path):
        log_and_print(f"Terminal path does not exist: {terminal_path}", "ERROR")
        return {
            "success": False,
            "message": f"Terminal path does not exist: {terminal_path}",
            "technical_error": True,
            "programme_id": programme_id,
            "update_params": None
        }

    log_and_print(f"Attempting MT5 login with broker_loginid='{broker_login}', broker_server='{broker_server}'", "DEBUG")
    try:
        # Initialize MT5 in thread-local context
        if not hasattr(thread_local, 'mt5'):
            thread_local.mt5 = mt5
        if not thread_local.mt5.initialize(path=terminal_path, login=int(broker_login), password=broker_password, server=broker_server, portable=True, timeout=30000):
            error_code, error_message = thread_local.mt5.last_error()
            log_and_print(f"MT5 initialization failed: Error code {error_code}, {error_message}", "ERROR")
            return {
                "success": False,
                "message": f"MT5 initialization failed: Error code {error_code}, {error_message}",
                "technical_error": error_code in [-10005, -10006, -10007],
                "programme_id": programme_id,
                "update_params": {
                    "new_status": "invalid_credentials",
                    "new_broker_loginstatus": "invalid"
                }
            }

        account_info = thread_local.mt5.account_info()
        if account_info is None:
            error_code, error_message = thread_local.mt5.last_error()
            log_and_print(f"Failed to get account info: {error_message}", "ERROR")
            return {
                "success": False,
                "message": f"Failed to get account info: {error_message}",
                "technical_error": error_code in [-10005, -10006, -10007],
                "programme_id": programme_id,
                "update_params": {
                    "new_status": "invalid_credentials",
                    "new_broker_loginstatus": "invalid"
                }
            }

        account_name = account_info.name
        account_balance = account_info.balance
        log_and_print(f"MT5 login successful: {broker_server}: {account_name}, balance={account_balance:.2f}", "SUCCESS")

        # Check user rank
        if user_rank is None:
            log_and_print(f"Skipping balance check: No user_rank provided for user_id={user_id}", "WARNING")
            return {
                "success": True,
                "message": f"MT5 login successful but no user_rank provided for user_id={user_id}, subaccount_id={subaccount_id or 'N/A'}, server={broker_server}",
                "technical_error": False,
                "programme_id": programme_id,
                "update_params": None
            }

        # Check balance against user rank
        log_and_print(f"Checking Balance Threshold", "INFO")
        balance_threshold = 12 if user_rank == 'unique' else 100
        
        if user_rank == 'unique' and account_balance < 12:
            log_and_print(f"Balance check failed: user_rank='unique', balance={account_balance:.2f} < 12", "WARNING")
            return {
                "success": True,
                "message": f"MT5 login successful but balance too low: user_id={user_id}, subaccount_id={subaccount_id or 'N/A'}, server={broker_server}, balance={account_balance:.2f}",
                "technical_error": False,
                "programme_id": programme_id,
                "update_params": None
            }
        elif user_rank == 'regular' and account_balance < 100:
            log_and_print(f"Balance check failed: user_rank='regular', balance={account_balance:.2f} < 100", "WARNING")
            return {
                "success": True,
                "message": f"MT5 login successful but balance too low: user_id={user_id}, subaccount_id={subaccount_id or 'N/A'}, server={broker_server}, balance={account_balance:.2f}",
                "technical_error": False,
                "programme_id": programme_id,
                "update_params": None
            }

        log_and_print(f"Balance check passed: user_rank='{user_rank}', balance={account_balance:.2f} >= {balance_threshold}", "DEBUG")

        # Use pre-fetched balance info
        if balance_info is None:
            log_and_print(f"Could not retrieve balance info for programme_id={programme_id}", "ERROR")
            return {
                "success": False,
                "message": f"Could not retrieve balance info for programme_id={programme_id}",
                "technical_error": True,
                "programme_id": programme_id,
                "update_params": None
            }

        initialbalance_status = balance_info['initialbalance_status']
        initial_balance = balance_info['initial_balance']
        current_balance = balance_info['current_balance']
        profit_and_loss = balance_info['profit_and_loss']

        log_and_print(f"Checking Initial Balance Status", "INFO")
        log_and_print(f"Current initialbalance_status: '{initialbalance_status}'", "DEBUG")

        update_params = {
            "new_status": "verified",
            "new_programme_status": "invited",
            "new_broker_loginstatus": "logged_in",
            "new_programme_contractstatus": "execute_contracts"
        }

        if initialbalance_status in ['none', 'usage-timeout', '0.00']:
            log_and_print(f"Updating initial_balance to {account_balance:.2f} and setting initialbalance_status to 'in-use'", "INFO")
            update_params.update({
                "new_initial_balance": account_balance,
                "new_initialbalance_status": "in-use"
            })
        elif initialbalance_status == 'in-use':
            log_and_print(f"Initial balance already in use. Updating current_balance to {account_balance:.2f}", "INFO")
            if initial_balance is not None:
                new_profit_and_loss = account_balance - initial_balance
                log_and_print(f"Calculating profit_and_loss: {account_balance:.2f} - {initial_balance:.2f} = {new_profit_and_loss:.2f}", "DEBUG")
                if new_profit_and_loss > 0:
                    log_and_print(f"Profit detected: +{new_profit_and_loss:.2f}", "SUCCESS")
                elif new_profit_and_loss < 0:
                    log_and_print(f"Loss detected: {new_profit_and_loss:.2f}", "WARNING")
                else:
                    log_and_print(f"No change in balance: {new_profit_and_loss:.2f}", "INFO")
            else:
                new_profit_and_loss = 0.00
                log_and_print(f"Initial balance not set, setting profit_and_loss to 0.00", "WARNING")
            update_params.update({
                "new_current_balance": account_balance,
                "new_profit_and_loss": new_profit_and_loss
            })
        else:
            log_and_print(f"Unknown initialbalance_status: '{initialbalance_status}'. Skipping balance update.", "WARNING")

        return {
            "success": True,
            "message": f"MT5 login successful for user_id={user_id}, subaccount_id={subaccount_id or 'N/A'}, server={broker_server}, balance={account_balance:.2f}",
            "technical_error": False,
            "programme_id": programme_id,
            "update_params": update_params
        }
    except Exception as e:
        log_and_print(f"Error during MT5 login for user_id={user_id}, subaccount_id={subaccount_id or 'N/A'}: {str(e)}", "ERROR")
        return {
            "success": False,
            "message": f"Error during MT5 login: {str(e)}",
            "technical_error": True,
            "programme_id": programme_id,
            "update_params": {
                "new_status": "invalid_credentials",
                "new_broker_loginstatus": "invalid"
            }
        }
    finally:
        if hasattr(thread_local, 'mt5'):
            thread_local.mt5.shutdown()

# Batch Update Function
def batch_update_user_programmes(updates):
    """Execute batched updates to user_programmes table with retries, skipping redundant updates."""
    log_and_print(f"Received updates: {updates}", "DEBUG")
    if not updates:
        log_and_print("No updates to process", "INFO")
        return 0

    update_queries = []
    for update in updates:
        programme_id = update['programme_id']
        params = update['update_params']
        if not params:
            log_and_print(f"Skipping update for programme_id={programme_id}: No update parameters provided", "DEBUG")
            continue
        
        # Verify programme_id exists
        sql_query = f"SELECT id FROM user_programmes WHERE id = '{programme_id}'"
        log_and_print(f"Verifying programme_id={programme_id} with query: {sql_query}", "DEBUG")
        result = db.execute_query(sql_query)
        if result['status'] != 'success' or not isinstance(result['results'], list) or not result['results']:
            log_and_print(f"Skipping update for programme_id={programme_id}: ID does not exist", "ERROR")
            continue

        # Check current values to avoid redundant updates
        check_query = f"""
            SELECT broker_status, status, broker_loginstatus, programme_contractstatus, 
                   initial_balance, current_balance, profit_and_loss, initialbalance_status
            FROM user_programmes 
            WHERE id = '{programme_id}'
        """
        check_result = db.execute_query(check_query)
        if check_result['status'] == 'success' and check_result['results']:
            current = check_result['results'][0]
            is_redundant = (
                current['broker_status'] == params.get('new_status') and
                current['status'] == params.get('new_programme_status', current['status']) and
                current['broker_loginstatus'] == params.get('new_broker_loginstatus', current['broker_loginstatus']) and
                current['programme_contractstatus'] == params.get('new_programme_contractstatus', current['programme_contractstatus']) and
                (params.get('new_initial_balance') is None or float(current['initial_balance'] or 0) == params.get('new_initial_balance', 0)) and
                (params.get('new_current_balance') is None or float(current['current_balance'] or 0) == params.get('new_current_balance', 0)) and
                (params.get('new_profit_and_loss') is None or float(current['profit_and_loss'] or 0) == params.get('new_profit_and_loss', 0)) and
                current['initialbalance_status'] == params.get('new_initialbalance_status', current['initialbalance_status'])
            )
            if is_redundant:
                log_and_print(f"Skipping update for programme_id={programme_id}: values already match", "INFO")
                continue

        # Build update query
        set_clauses = [f"broker_status = '{params['new_status']}'"]
        if params.get('new_programme_status'):
            set_clauses.append(f"status = '{params['new_programme_status']}'")
        if params.get('new_broker_loginstatus'):
            set_clauses.append(f"broker_loginstatus = '{params['new_broker_loginstatus']}'")
        if params.get('new_initial_balance') is not None:
            set_clauses.append(f"initial_balance = {params['new_initial_balance']:.2f}")
        if params.get('new_current_balance') is not None:
            set_clauses.append(f"current_balance = {params['new_current_balance']:.2f}")
        if params.get('new_profit_and_loss') is not None:
            set_clauses.append(f"profit_and_loss = {params['new_profit_and_loss']:.2f}")
        if params.get('new_programme_contractstatus'):
            set_clauses.append(f"programme_contractstatus = '{params['new_programme_contractstatus']}'")
        if params.get('new_initialbalance_status'):
            set_clauses.append(f"initialbalance_status = '{params['new_initialbalance_status']}'")
        
        sql_query = f"""
            UPDATE user_programmes
            SET {', '.join(set_clauses)}
            WHERE id = '{programme_id}'
        """
        update_queries.append((sql_query, programme_id, params))

    if not update_queries:
        log_and_print("No valid updates to process after verification", "WARNING")
        return 0

    log_and_print(f"Executing {len(update_queries)} batched update queries", "INFO")
    success_count = 0
    for sql_query, programme_id, params in update_queries:
        for attempt in range(1, MAX_RETRIES + 1):
            log_and_print(f"Executing update query (attempt {attempt}/{MAX_RETRIES}) for programme_id={programme_id}: {sql_query}", "DEBUG")
            result = db.execute_query(sql_query)
            if result['status'] == 'success' and isinstance(result['results'], dict) and result['results'].get('affected_rows', 0) > 0:
                log_and_print(f"Successfully updated programme_id={programme_id}: {params}", "SUCCESS")
                success_count += 1
                break
            else:
                error_message = result.get('message', 'No message provided')
                affected_rows = result['results'].get('affected_rows', 0) if isinstance(result['results'], dict) else 'N/A'
                log_and_print(f"Failed to update programme_id={programme_id} on attempt {attempt}: {error_message}, affected_rows={affected_rows}", "ERROR")
                if attempt < MAX_RETRIES:
                    delay = RETRY_DELAY * (2 ** (attempt - 1))
                    log_and_print(f"Retrying after {delay} seconds...", "INFO")
                    time.sleep(delay)
                else:
                    log_and_print(f"Max retries reached for programme_id={programme_id}. Update failed.", "ERROR")

    log_and_print(f"Successfully updated {success_count}/{len(update_queries)} programmes", "INFO")
    return success_count

# Main Processing Function
def main():
    try:
        print("\n")
        log_and_print("===== Verification Process =====", "TITLE")
        
        # SQL query to fetch all columns from user_programmes, joined with users
        sql_query = """
            SELECT 
                u.id AS user_id, 
                u.account_status, 
                u.user_rank, 
                u.userid_status,
                up.id AS programme_id, 
                up.user_id AS up_user_id, 
                up.subaccount_id, 
                up.programme, 
                up.status, 
                up.broker, 
                up.broker_server, 
                up.broker_loginid, 
                up.broker_password, 
                up.broker_status, 
                up.category, 
                up.leverage, 
                up.initial_balance, 
                up.initialbalance_status, 
                up.current_balance, 
                up.profit_and_loss, 
                up.profit_split, 
                up.last_status, 
                up.last_violation, 
                up.last_programme_violated, 
                up.account_username, 
                up.account_type, 
                up.broker_loginstatus, 
                up.returns_method, 
                up.returns_options, 
                up.programme_timeframe, 
                up.programme_markets, 
                up.programme_contractstatus, 
                up.created_at
            FROM 
                users u
            LEFT JOIN 
                user_programmes up ON u.id = up.user_id
        """
        log_and_print(f"Sending query: {sql_query}", "INFO")

        # Execute query using connectwithinfinitydb
        result = db.execute_query(sql_query)

        # Process query results
        print("\n")
        log_and_print("--- Query Execution Results ---", "TITLE")
        log_and_print(f"Status: {result['status']}", "INFO")
        log_and_print(f"Message: {result['message']}", "INFO")

        if result['status'] != 'success':
            log_and_print("Query failed, no results to process", "ERROR")
            return

        if not isinstance(result['results'], list) or not result['results']:
            log_and_print("No results returned", "INFO")
            return

        # Initialize counters
        total_verified = 0
        total_main_accounts = 0
        total_subaccounts = 0
        skipped_records = 0
        accounts_updated = 0
        processed_programme_ids = set()

        log_and_print(f"Fetched {len(result['results'])} rows", "SUCCESS")

        # Define variables for all user_programmes columns
        programme_id = None
        up_user_id = None
        subaccount_id = None
        programme = None
        status = None
        broker = None
        broker_server = None
        broker_loginid = None
        broker_password = None
        broker_status = None
        category = None
        leverage = None
        initial_balance = None
        initialbalance_status = None
        current_balance = None
        profit_and_loss = None
        profit_split = None
        last_status = None
        last_violation = None
        last_programme_violated = None
        account_username = None
        account_type = None
        broker_loginstatus = None
        returns_method = None
        returns_options = None
        programme_timeframe = None
        programme_markets = None
        programme_contractstatus = None
        created_at = None

        # Filter eligible programmes and group by user_id
        eligible_programmes = []
        updates = []
        users = {}
        for row in result['results']:
            user_id = row['user_id']
            programme_id = row['programme_id']
            if user_id not in users:
                user_rank = get_user_rank(user_id)  # Fetch user_rank in main thread
                users[user_id] = {
                    'account_status': row['account_status'],
                    'user_rank': user_rank,
                    'userid_status': row['userid_status'],
                    'programmes': {}
                }

            # Assign row values to variables
            programme_id = row['programme_id']
            up_user_id = row['up_user_id']
            subaccount_id = row['subaccount_id']
            programme = row['programme']
            status = row['status']
            broker = row['broker']
            broker_server = row['broker_server']
            broker_loginid = row['broker_loginid']
            broker_password = row['broker_password']
            broker_status = row['broker_status']
            category = row['category']
            leverage = row['leverage']
            initial_balance = row['initial_balance']
            initialbalance_status = row['initialbalance_status']
            current_balance = row['current_balance']
            profit_and_loss = row['profit_and_loss']
            profit_split = row['profit_split']
            last_status = row['last_status']
            last_violation = row['last_violation']
            last_programme_violated = row['last_programme_violated']
            account_username = row['account_username']
            account_type = row['account_type']
            broker_loginstatus = row['broker_loginstatus']
            returns_method = row['returns_method']
            returns_options = row['returns_options']
            programme_timeframe = row['programme_timeframe']
            programme_markets = row['programme_markets']
            programme_contractstatus = row['programme_contractstatus']
            created_at = row['created_at']

            balance_info = get_programme_balance_info(programme_id) if programme_id else None
            users[user_id]['programmes'][programme_id] = {
                'programme_id': programme_id,
                'user_id': up_user_id,
                'subaccount_id': subaccount_id,
                'status': status,
                'broker_status': broker_status,
                'broker': broker,
                'broker_server': broker_server,
                'broker_loginid': broker_loginid,
                'broker_password': broker_password,
                'broker_loginstatus': broker_loginstatus,
                'programme': programme,
                'balance_info': balance_info
            }

        # Filter eligible programmes and create directories
        log_and_print("Filtering eligible programmes and creating directories", "INFO")
        for user_id, user_data in users.items():
            if user_data['account_status'] != 'active':
                log_and_print(f"Skipping user_id={user_id}: account_status={user_data['account_status']} is not active", "INFO")
                skipped_records += len(user_data['programmes'])
                continue

            for programme_id, prog in user_data['programmes'].items():
                if programme_id in processed_programme_ids:
                    log_and_print(f"Skipping programme_id={programme_id}: already processed", "DEBUG")
                    skipped_records += 1
                    continue

                if not (
                    (prog['status'] == 'interested' and prog['broker_status'] == 'login_successful') or
                    (prog['status'] == 'invited' and prog['broker_status'] == 'verified') or
                    ((prog['status'] == 'interested' or prog['status'] == 'invited') and prog['broker_status'] == 'login_successful')
                ):
                    log_and_print(f"Skipping programme_id={programme_id}: does not meet conditions (status={prog['status']}, broker_status={prog['broker_status']})", "DEBUG")
                    skipped_records += 1
                    continue

                account_type = 'subaccount' if prog['subaccount_id'] and prog['subaccount_id'] != 'NULL' else 'main_account'
                if account_type == 'main_account':
                    total_main_accounts += 1
                else:
                    total_subaccounts += 1

                # Check for valid credentials
                if (prog['broker_server'] == 'none' or 
                    prog['broker_loginid'] == 'none' or 
                    prog['broker_password'] == 'none'):
                    log_and_print(f"Skipping MT5 login for programme_id={programme_id}: invalid credentials (server={prog['broker_server']}, loginid={prog['broker_loginid']})", "DEBUG")
                    skipped_records += 1
                    updates.append({
                        'programme_id': programme_id,
                        'update_params': {
                            'new_status': 'invalid_credentials',
                            'new_broker_loginstatus': 'invalid'
                        }
                    })
                    processed_programme_ids.add(programme_id)
                    continue

                mt5_path = get_mt5_directory(user_id, prog['subaccount_id'])
                if not mt5_path:
                    log_and_print(f"Skipping MT5 login due to failed directory access for programme_id={programme_id}", "ERROR")
                    processed_programme_ids.add(programme_id)
                    updates.append({
                        'programme_id': programme_id,
                        'update_params': {
                            'new_status': 'invalid_credentials',
                            'new_broker_loginstatus': 'invalid'
                        }
                    })
                    continue

                eligible_programmes.append({
                    'broker_login': prog['broker_loginid'],
                    'broker_password': prog['broker_password'],
                    'broker_server': prog['broker_server'],
                    'terminal_path': os.path.join(mt5_path, 'terminal64.exe'),
                    'user_id': user_id,
                    'subaccount_id': prog['subaccount_id'],
                    'programme_id': programme_id,
                    'programme_name': prog['programme'],
                    'account_type': account_type,
                    'user_rank': user_data['user_rank'],
                    'balance_info': prog['balance_info']
                })
                processed_programme_ids.add(programme_id)

        # Process eligible programmes in parallel
        log_and_print(f"Processing {len(eligible_programmes)} eligible programmes in parallel", "INFO")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_programme = {
                executor.submit(verify_broker_balance, 
                               prog['broker_login'], 
                               prog['broker_password'], 
                               prog['broker_server'], 
                               prog['terminal_path'], 
                               prog['user_id'], 
                               prog['subaccount_id'], 
                               prog['programme_id'],
                               prog['user_rank'],
                               prog['balance_info']): prog
                for prog in eligible_programmes
            }

            for future in as_completed(future_to_programme):
                prog = future_to_programme[future]
                try:
                    result = future.result()
                    if result['success'] and not result['technical_error'] and 'balance too low' not in result['message']:
                        total_verified += 1
                        if result['update_params']:
                            updates.append({
                                'programme_id': result['programme_id'],
                                'update_params': result['update_params']
                            })
                    else:
                        if result['update_params']:
                            updates.append({
                                'programme_id': result['programme_id'],
                                'update_params': result['update_params']
                            })
                except Exception as e:
                    log_and_print(f"Error processing programme_id={prog['programme_id']}: {str(e)}", "ERROR")
                    updates.append({
                        'programme_id': prog['programme_id'],
                        'update_params': {
                            'new_status': 'invalid_credentials',
                            'new_broker_loginstatus': 'invalid'
                        }
                    })

        # Batch update database
        accounts_updated = batch_update_user_programmes(updates)

        # Verify updates
        if accounts_updated < len(updates):
            log_and_print("Verifying database updates", "INFO")
            programme_ids = [f"'{u['programme_id']}'" for u in updates if u['programme_id'] is not None]
            if programme_ids:
                verify_query = f"""
                    SELECT id, broker_status, status, broker_loginstatus, programme_contractstatus, 
                           initial_balance, current_balance, profit_and_loss, initialbalance_status
                    FROM user_programmes 
                    WHERE id IN ({','.join(programme_ids)})
                """
                result = db.execute_query(verify_query)
                if result['status'] == 'success' and isinstance(result['results'], list):
                    for row in result['results']:
                        log_and_print(f"Verification: programme_id={row['id']}, broker_status={row['broker_status']}, "
                                      f"status={row['status']}, broker_loginstatus={row['broker_loginstatus']}, "
                                      f"programme_contractstatus={row['programme_contractstatus']}, "
                                      f"initial_balance={row['initial_balance']}, current_balance={row['current_balance']}, "
                                      f"profit_and_loss={row['profit_and_loss']}, initialbalance_status={row['initialbalance_status']}", "DEBUG")

        # Display summary
        print("\n")
        log_and_print("===== Processing Summary =====", "TITLE")
        log_and_print(f"Total main accounts processed: {total_main_accounts}", "INFO")
        log_and_print(f"Total subaccounts processed: {total_subaccounts}", "INFO")
        log_and_print(f"Successful verifications (MT5 login succeeded): {total_verified}", "INFO" if total_verified > 0 else "WARNING")
        log_and_print(f"Accounts updated to verified/invited: {accounts_updated}", "INFO" if accounts_updated > 0 else "WARNING")
        log_and_print(f"Skipped records (non-qualifying status/broker_status or invalid user): {skipped_records}", "INFO")
        print("\n")
        log_and_print("===== Verification Process Completed =====", "TITLE")

    finally:
        db.shutdown()

if __name__ == "__main__":
    main()