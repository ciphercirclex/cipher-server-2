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
    
    log_and_print(f"Checking MT5 directory: {destination_path}", "DEBUG")
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
        log_and_print(f"Source MT5 path: {source_mt5_path}", "DEBUG")
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

# Programme ID Verification
def verify_programme_id(programme_id):
    """Verify if programme_id exists in user_programmes table."""
    sql_query = f"SELECT id FROM user_programmes WHERE id = '{programme_id}'"
    log_and_print(f"Verifying programme_id={programme_id} with query: {sql_query}", "DEBUG")
    result = db.execute_query(sql_query)
    log_and_print(f"Verification result: {result}", "DEBUG")
    if result['status'] == 'success' and isinstance(result['results'], list) and result['results']:
        log_and_print(f"programme_id={programme_id} exists in user_programmes", "DEBUG")
        return True
    log_and_print(f"programme_id={programme_id} not found in user_programmes: {result['message']}", "ERROR")
    return False

# MetaTrader 5 Login Function
def login_to_mt5(broker_server, broker_login, broker_password, terminal_path, user_id, subaccount_id=None, programme_id=None):
    """Attempt to log in to MT5 with provided credentials."""
    log_and_print(f"Checking terminal path: {terminal_path}", "DEBUG")
    if not os.path.exists(terminal_path):
        log_and_print(f"Terminal path does not exist for user_id={user_id}, subaccount_id={subaccount_id or 'N/A'}, programme_id={programme_id}: {terminal_path}", "ERROR")
        return {
            "success": False,
            "message": f"Terminal path does not exist: {terminal_path}",
            "programme_id": programme_id,
            "update_params": {
                "broker_status": "invalid_credentials",
                "broker_loginstatus": "logged_out"
            }
        }

    log_and_print(f"Attempting MT5 login with broker_loginid='{broker_login}', broker_server='{broker_server}'", "DEBUG")
    try:
        if not hasattr(thread_local, 'mt5'):
            thread_local.mt5 = mt5
        if not thread_local.mt5.initialize(path=terminal_path, login=int(broker_login), password=broker_password, server=broker_server, portable=True, timeout=30000):
            error_code, error_message = thread_local.mt5.last_error()
            log_and_print(f"MT5 initialization failed for user_id={user_id}, subaccount_id={subaccount_id or 'N/A'}, programme_id={programme_id}, server={broker_server}, login={broker_login}: Error code {error_code}, {error_message}", "ERROR")
            return {
                "success": False,
                "message": f"MT5 initialization failed: Error code {error_code}, {error_message}",
                "programme_id": programme_id,
                "update_params": {
                    "broker_status": "invalid_credentials",
                    "broker_loginstatus": "logged_out"
                }
            }

        account_info = thread_local.mt5.account_info()
        if account_info is None:
            error_code, error_message = thread_local.mt5.last_error()
            log_and_print(f"Failed to get account info for user_id={user_id}, subaccount_id={subaccount_id or 'N/A'}, programme_id={programme_id}, server={broker_server}, login={broker_login}: {error_message}", "ERROR")
            return {
                "success": False,
                "message": f"Failed to get account info: {error_message}",
                "programme_id": programme_id,
                "update_params": {
                    "broker_status": "invalid_credentials",
                    "broker_loginstatus": "logged_out"
                }
            }

        log_and_print(f"MT5 login successful: {broker_server}: {account_info.name}, balance={account_info.balance:.2f}", "SUCCESS")
        update_params = {
            "broker_status": "login_successful",
            "broker_loginstatus": "logged_in"
        }
        return {
            "success": True,
            "message": f"MT5 login successful for user_id={user_id}, subaccount_id={subaccount_id or 'N/A'}, programme_id={programme_id}",
            "programme_id": programme_id,
            "update_params": update_params
        }
    except Exception as e:
        log_and_print(f"Error during MT5 login for user_id={user_id}, subaccount_id={subaccount_id or 'N/A'}, programme_id={programme_id}: {str(e)}", "ERROR")
        return {
            "success": False,
            "message": f"Error during MT5 login: {str(e)}",
            "programme_id": programme_id,
            "update_params": {
                "broker_status": "invalid_credentials",
                "broker_loginstatus": "logged_out"
            }
        }
    finally:
        if hasattr(thread_local, 'mt5'):
            thread_local.mt5.shutdown()

# Batch Update Function
def batch_update_broker_status(updates):
    """Execute batched updates to user_programmes table with retries, skipping redundant updates."""
    log_and_print(f"Received updates: {updates}", "DEBUG")
    if not updates:
        log_and_print("No updates to process", "INFO")
        return 0

    update_queries = []
    for update in updates:
        programme_id = update['programme_id']
        params = update['update_params']
        log_and_print(f"Processing update for programme_id={programme_id}: {params}", "DEBUG")
        
        # Verify programme_id exists
        if not verify_programme_id(programme_id):
            log_and_print(f"Skipping update for programme_id={programme_id}: ID does not exist", "ERROR")
            continue
        
        # Check current values to avoid redundant updates
        check_query = f"SELECT broker_status, broker_loginstatus FROM user_programmes WHERE id = '{programme_id}'"
        check_result = db.execute_query(check_query)
        if check_result['status'] == 'success' and check_result['results']:
            current_status = check_result['results'][0]['broker_status']
            current_loginstatus = check_result['results'][0]['broker_loginstatus']
            if (current_status == params['broker_status'] and 
                current_loginstatus == params['broker_loginstatus']):
                log_and_print(f"Skipping update for programme_id={programme_id}: values already match (broker_status={current_status}, broker_loginstatus={current_loginstatus})", "INFO")
                continue
        
        # Proceed with update if values differ
        sql_query = f"""
            UPDATE user_programmes
            SET broker_status = '{params['broker_status']}', broker_loginstatus = '{params['broker_loginstatus']}'
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
                log_and_print(f"Successfully updated programme_id={programme_id}: broker_status='{params['broker_status']}', broker_loginstatus='{params['broker_loginstatus']}'", "SUCCESS")
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
        log_and_print("===== Login Brokers Process =====", "TITLE")

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
        total_logins = 0
        valid_credentials_main = 0
        valid_credentials_sub = 0
        invalid_credentials_main = 0
        invalid_credentials_sub = 0
        total_main_accounts = 0
        total_subaccounts = 0
        skipped_records = 0
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
        updates = []  # Store updates for invalid credentials
        users = {}
        for row in result['results']:
            user_id = row['user_id']
            if user_id not in users:
                users[user_id] = {
                    'account_status': row['account_status'],
                    'user_rank': row['user_rank'],
                    'userid_status': row['userid_status'],
                    'programmes': []
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

            users[user_id]['programmes'].append({
                'programme_id': programme_id,
                'user_id': up_user_id,
                'subaccount_id': subaccount_id,
                'status': status,
                'broker_status': broker_status,
                'broker': broker,
                'broker_server': broker_server,
                'broker_loginid': broker_loginid,
                'broker_password': broker_password,
                'broker_loginstatus': broker_loginstatus
            })

        # Filter eligible programmes and create directories
        log_and_print("Filtering eligible programmes and creating directories", "INFO")
        for user_id, user_data in users.items():
            if user_data['account_status'] != 'active':
                log_and_print(f"Skipping user_id={user_id}: account_status={user_data['account_status']} is not active", "INFO")
                skipped_records += len(user_data['programmes'])
                continue

            for prog in user_data['programmes']:
                programme_id = prog['programme_id']
                if programme_id in processed_programme_ids:
                    log_and_print(f"Skipping programme_id={programme_id}: already processed", "DEBUG")
                    skipped_records += 1
                    continue

                # Skip if already logged in and login successful
                if prog['broker_status'] == 'login_successful' and prog['broker_loginstatus'] == 'logged_in':
                    log_and_print(f"Skipping programme_id={programme_id}: already logged in with successful status", "INFO")
                    skipped_records += 1
                    continue

                # Include 'unverified' accounts with valid credentials
                if not (
                    (prog['status'] == 'interested' and prog['broker_status'] in ['credential_submitted', 'unverified']) or
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
                            'broker_status': 'invalid_credentials',
                            'broker_loginstatus': 'logged_out'
                        },
                        'account_type': account_type
                    })
                    if account_type == 'main_account':
                        invalid_credentials_main += 1
                    else:
                        invalid_credentials_sub += 1
                    processed_programme_ids.add(programme_id)
                    continue

                mt5_path = get_mt5_directory(user_id, prog['subaccount_id'])
                if not mt5_path:
                    log_and_print(f"Skipping MT5 login due to failed directory access for programme_id={programme_id}", "ERROR")
                    processed_programme_ids.add(programme_id)
                    updates.append({
                        'programme_id': programme_id,
                        'update_params': {
                            'broker_status': 'invalid_credentials',
                            'broker_loginstatus': 'logged_out'
                        },
                        'account_type': account_type
                    })
                    if account_type == 'main_account':
                        invalid_credentials_main += 1
                    else:
                        invalid_credentials_sub += 1
                    continue

                eligible_programmes.append({
                    'broker_server': prog['broker_server'],
                    'broker_login': prog['broker_loginid'],
                    'broker_password': prog['broker_password'],
                    'terminal_path': os.path.join(mt5_path, 'terminal64.exe'),
                    'user_id': user_id,
                    'subaccount_id': prog['subaccount_id'],
                    'programme_id': programme_id,
                    'account_type': account_type,
                    'broker_status': prog['broker_status']
                })
                processed_programme_ids.add(programme_id)

        log_and_print(f"Eligible programmes: {eligible_programmes}", "DEBUG")
        # Process eligible programmes in parallel
        log_and_print(f"Processing {len(eligible_programmes)} eligible programmes in parallel", "INFO")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_programme = {
                executor.submit(login_to_mt5,
                               prog['broker_server'],
                               prog['broker_login'],
                               prog['broker_password'],
                               prog['terminal_path'],
                               prog['user_id'],
                               prog['subaccount_id'],
                               prog['programme_id']): prog
                for prog in eligible_programmes
            }

            for future in as_completed(future_to_programme):
                prog = future_to_programme[future]
                try:
                    result = future.result()
                    if result['success']:
                        total_logins += 1
                        if prog['account_type'] == 'main_account':
                            valid_credentials_main += 1
                        else:
                            valid_credentials_sub += 1
                        if prog['broker_status'] == 'login_successful':
                            result['update_params'] = {
                                'broker_status': prog['broker_status'],
                                'broker_loginstatus': 'logged_in'
                            }
                    else:
                        if prog['account_type'] == 'main_account':
                            invalid_credentials_main += 1
                        else:
                            invalid_credentials_sub += 1
                    updates.append({
                        'programme_id': result['programme_id'],
                        'update_params': result['update_params']
                    })
                except Exception as e:
                    log_and_print(f"Error processing programme_id={prog['programme_id']}: {str(e)}", "ERROR")
                    if prog['account_type'] == 'main_account':
                        invalid_credentials_main += 1
                    else:
                        invalid_credentials_sub += 1
                    updates.append({
                        'programme_id': prog['programme_id'],
                        'update_params': {
                            'broker_status': 'invalid_credentials',
                            'broker_loginstatus': 'logged_out'
                        }
                    })

        log_and_print(f"Updates to be applied: {updates}", "DEBUG")
        # Batch update database
        success_count = batch_update_broker_status(updates)

        # Verify updates
        if success_count < len(updates):
            log_and_print("Verifying database updates", "INFO")
            programme_ids = [f"'{u['programme_id']}'" for u in updates if u['programme_id'] is not None]
            if programme_ids:
                verify_query = f"SELECT id, broker_status, broker_loginstatus FROM user_programmes WHERE id IN ({','.join(programme_ids)})"
                result = db.execute_query(verify_query)
                if result['status'] == 'success' and isinstance(result['results'], list):
                    for row in result['results']:
                        log_and_print(f"Verification: programme_id={row['id']}, broker_status={row['broker_status']}, broker_loginstatus={row['broker_loginstatus']}", "DEBUG")

        # Display summary
        print("\n")
        log_and_print("===== Processing Summary =====", "TITLE")
        log_and_print(f"Total main accounts processed: {total_main_accounts}", "INFO")
        log_and_print(f"Total subaccounts processed: {total_subaccounts}", "INFO")
        log_and_print(f"Successful logins: {total_logins}", "INFO")
        log_and_print(f"Accounts with valid credentials: Main={valid_credentials_main}, Sub={valid_credentials_sub}", "INFO")
        log_and_print(f"Accounts with invalid credentials: Main={invalid_credentials_main}, Sub={invalid_credentials_sub}", "INFO")
        log_and_print(f"Skipped records: {skipped_records}", "INFO")
        print("\n")
        log_and_print("===== Login Brokers Process Completed =====", "TITLE")

    finally:
        db.shutdown()

if __name__ == "__main__":
    main()