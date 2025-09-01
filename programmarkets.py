import connectwithinfinitydb as db
from datetime import datetime
from colorama import Fore, Style, init
import threading

# Initialize colorama for colored console output
init()

# Thread-local Storage Setup
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

# Main Processing Function
def main():
    try:
        print("\n")
        log_and_print("===== Programme Markets and Timeframe Process =====", "TITLE")

        # SQL query to fetch user and programme details, including programme_timeframe
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

        # Initialize counters and storage
        total_main_accounts_with_markets = 0
        total_subaccounts_with_markets = 0
        users = {}
        processed_programme_ids = set()

        log_and_print(f"Fetched {len(result['results'])} rows", "SUCCESS")

        # Organize data by user
        for row in result['results']:
            user_id = row['user_id']
            if user_id not in users:
                users[user_id] = {
                    'account_status': row['account_status'],
                    'main_accounts': [],
                    'subaccounts': []
                }

            programme_id = row['programme_id']
            if programme_id in processed_programme_ids or programme_id is None:
                continue

            programme_markets = row['programme_markets'] if row['programme_markets'] and row['programme_markets'].lower() != 'none' else ''
            programme_timeframe = row['programme_timeframe'] if row['programme_timeframe'] and row['programme_timeframe'].lower() != 'none' else 'No timeframe specified'
            account_type = 'subaccount' if row['subaccount_id'] and row['subaccount_id'] != 'NULL' else 'main_account'

            programme_data = {
                'programme_id': programme_id,
                'programme': row['programme'],
                'programme_markets': programme_markets,
                'programme_timeframe': programme_timeframe
            }

            if account_type == 'main_account':
                users[user_id]['main_accounts'].append(programme_data)
                if programme_markets:
                    total_main_accounts_with_markets += 1
            else:
                users[user_id]['subaccounts'].append(programme_data)
                if programme_markets:
                    total_subaccounts_with_markets += 1

            processed_programme_ids.add(programme_id)

        # Display programme markets and timeframe for each user
        print("\n")
        log_and_print("--- Programme Markets and Timeframe by User ---", "TITLE")
        for user_id, user_data in users.items():
            if user_data['account_status'] != 'active':
                log_and_print(f"Skipping user_id={user_id}: account_status={user_data['account_status']} is not active", "INFO")
                continue

            # Display main accounts
            if user_data['main_accounts']:
                log_and_print(f"User ID {user_id} Main Account Programme Markets and Timeframe:", "INFO")
                for prog in user_data['main_accounts']:
                    markets = prog['programme_markets'] if prog['programme_markets'] else "No markets specified"
                    timeframe = prog['programme_timeframe']
                    log_and_print(f"Programme ID {prog['programme_id']} ({prog['programme']}): {markets} {timeframe}", "DEBUG")

            # Display subaccounts
            if user_data['subaccounts']:
                log_and_print(f"User ID {user_id} Subaccount Programme Markets and Timeframe:", "INFO")
                for prog in user_data['subaccounts']:
                    markets = prog['programme_markets'] if prog['programme_markets'] else "No markets specified"
                    timeframe = prog['programme_timeframe']
                    log_and_print(f"Programme ID {prog['programme_id']} ({prog['programme']}): {markets} {timeframe}", "DEBUG")

        # Display summary
        print("\n")
        log_and_print("===== Processing Summary =====", "TITLE")
        log_and_print(f"Total main accounts with programme_markets not empty: {total_main_accounts_with_markets}", "INFO")
        log_and_print(f"Total subaccounts with programme_markets not empty: {total_subaccounts_with_markets}", "INFO")
        print("\n")
        log_and_print("===== Programme Markets and Timeframe Process Completed =====", "TITLE")

    except Exception as e:
        log_and_print(f"Unexpected error: {str(e)}", "ERROR")
    finally:
        db.shutdown()

if __name__ == "__main__":
    main()