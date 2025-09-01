import os
import re
from datetime import datetime

# Configuration
remote_export_dirs = [
    r'C:\xampp\htdocs\CIPHER\cipherdb\cipheruserdb_backup',
    r'C:\xampp\htdocs\CIPHER\cipherdb\cipheruserdb'
]

def log_and_print(message, level="INFO"):
    """Helper function to print formatted log messages with clear formatting."""
    level_colors = {
        "INFO": "\033[94mINFO   \033[0m",
        "ERROR": "\033[91mERROR  \033[0m",
        "WARNING": "\033[93mWARNING\033[0m",
        "DEBUG": "\033[90mDEBUG  \033[0m",
        "SUCCESS": "\033[92mSUCCESS\033[0m"
    }
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted_message = f"[ {timestamp} ] │ {level_colors.get(level, 'INFO   ')} │ {message}"
    print(formatted_message)

def parse_sql_inserts(file_path):
    """Parse an SQL file to extract INSERT statement values."""
    if not os.path.exists(file_path):
        log_and_print(f"SQL file not found: {file_path}", "ERROR")
        return []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        insert_statements = re.split(r';\s*\n', content.strip())
        records = []
        
        for stmt in insert_statements:
            if not stmt.strip().startswith('INSERT INTO'):
                continue
            values_match = re.search(r'VALUES\s*\((.*?)\)', stmt, re.DOTALL)
            if values_match:
                values_str = values_match.group(1)
                values = []
                current = ''
                in_quotes = False
                i = 0
                while i < len(values_str):
                    char = values_str[i]
                    if char == "'" and (i == 0 or values_str[i-1] != '\\'):
                        in_quotes = not in_quotes
                    elif char == ',' and not in_quotes:
                        values.append(current.strip())
                        current = ''
                        i += 1
                        continue
                    current += char
                    i += 1
                if current.strip():
                    values.append(current.strip())
                cleaned_values = []
                for val in values:
                    val = val.strip()
                    if val.startswith("'") and val.endswith("'"):
                        val = val[1:-1]
                    cleaned_values.append(val)
                records.append(cleaned_values)
        log_and_print(f"Successfully parsed {len(records)} records from {file_path}", "DEBUG")
        return records
    except Exception as e:
        log_and_print(f"Error parsing SQL file {file_path}: {str(e)}", "ERROR")
        return []

def get_user_rank(user_id, users_file):
    """Retrieve the user_rank for a given user_id from users.sql."""
    log_and_print(f"Retrieving user_rank for user_id={user_id} from {users_file}", "DEBUG")
    records = parse_sql_inserts(users_file)
    for values in records:
        if len(values) >= 11 and values[0] == str(user_id):
            user_rank = values[10].lower().strip()
            log_and_print(f"Found user_rank='{user_rank}' for user_id={user_id}", "DEBUG")
            return user_rank
    log_and_print(f"No user_rank found for user_id={user_id} in {users_file}", "WARNING")
    return None

def clean_notifications_file(notifications_file, valid_notifications):
    """Clean notifications.sql by keeping only valid, unique notifications."""
    try:
        with open(notifications_file, 'w', encoding='utf-8') as f:
            for notification in valid_notifications:
                user_id = notification['user_id']
                subaccount_id = notification['subaccount_id'] if notification['subaccount_id'] else 'NULL'
                message = notification['message'].replace("'", "''")
                timestamp = notification['timestamp']
                f.write(f"INSERT INTO notifications (id, user_id, subaccount_id, notification_messages, time_stamp) "
                        f"VALUES ({notification['id']}, {user_id}, {subaccount_id}, '{message}', '{timestamp}');\n")
        log_and_print(f"Cleaned {notifications_file}: {len(valid_notifications)} valid notifications retained", "INFO")
    except Exception as e:
        log_and_print(f"Failed to clean {notifications_file}: {str(e)}", "ERROR")

def verify_and_update_notifications():
    """Verify user and programme conditions in .sql files and update notifications.sql for each account."""
    total_inserted_count = 0

    for remote_export_dir in remote_export_dirs:
        print("\n" + "="*100)
        log_and_print(f" Processing Directory: {remote_export_dir} ", "INFO")
        print("-"*100 + "\n")

        if not os.path.exists(remote_export_dir):
            log_and_print(f"Directory does not exist: {remote_export_dir}", "ERROR")
            continue
        if not os.access(remote_export_dir, os.W_OK):
            log_and_print(f"Directory is not writable: {remote_export_dir}. Run: icacls \"{remote_export_dir}\" /grant Everyone:F", "ERROR")
            continue

        # Step 1: Parse users.sql for active users
        log_and_print(" Parsing users.sql to identify active users ", "INFO")
        users_file = os.path.join(remote_export_dir, 'users.sql')
        active_users = []
        users_records = parse_sql_inserts(users_file)
        for record in users_records:
            if len(record) >= 10 and record[9].lower().strip() == 'active':
                active_users.append({'id': record[0]})
        
        log_and_print(f"Users file: {users_file}", "INFO")
        log_and_print(f"Active users found: {len(active_users)}", "INFO")
        if not active_users:
            log_and_print("No active users found, skipping directory", "WARNING")
            continue
        print()

        # Step 2: Parse user_programmes.sql for qualifying accounts
        log_and_print(" Parsing user_programmes.sql for eligible accounts ", "INFO")
        user_programmes_file = os.path.join(remote_export_dir, 'user_programmes.sql')
        matching_accounts = []
        programmes_records = parse_sql_inserts(user_programmes_file)
        log_and_print(f"Programmes file: {user_programmes_file}", "INFO")
        log_and_print(f"Parsed programme records: {len(programmes_records)}", "INFO")

        for i, record in enumerate(programmes_records, 1):
            if len(record) >= 9:
                status, broker_status = record[4].lower().strip(), record[8].lower().strip()
                log_and_print(f"Record {i}: programme_id={record[0]}, user_id={record[1]}, subaccount_id={record[2]}, "
                              f"programme={record[3]}, status='{status}', broker_status='{broker_status}'", "DEBUG")
                if (status == 'interested' and broker_status in ['credential_submitted', 'login_successful']) or \
                   (status == 'invited' and broker_status == 'verified'):
                    user_id = record[1]
                    if any(u['id'] == user_id for u in active_users):
                        subaccount_id = record[2] if record[2] != 'NULL' else None
                        account_type = 'subaccount' if subaccount_id else 'main account'
                        programme_name = record[3]
                        programme_id = record[0]
                        initial_balance = float(record[12]) if len(record) >= 13 and record[12] != 'NULL' and record[12].strip() else 0.0
                        matching_accounts.append({
                            'user_id': user_id,
                            'subaccount_id': subaccount_id,
                            'programme_id': programme_id,
                            'programme': programme_name,
                            'account_type': account_type,
                            'broker_status': broker_status,
                            'status': status,
                            'initial_balance': initial_balance
                        })
                        log_and_print(f"Added {account_type}: programme_id={programme_id}, user_id={user_id}, "
                                      f"subaccount_id={subaccount_id or 'N/A'}, programme={programme_name}, "
                                      f"status={status}, broker_status={broker_status}, initial_balance={initial_balance:.2f}", "DEBUG")
            else:
                log_and_print(f"Record {i}: programme_id={record[0] if record else 'N/A'}, insufficient columns (found {len(record)}, expected >= 9)", "WARNING")
        
        log_and_print(f"Qualifying accounts found: {len(matching_accounts)}", "INFO")
        if not matching_accounts:
            log_and_print("No qualifying accounts found, skipping directory", "WARNING")
            continue
        print()

        # Step 3: Parse and clean notifications.sql
        log_and_print(" Processing notifications.sql ", "INFO")
        notifications_file = os.path.join(remote_export_dir, 'notifications.sql')
        existing_notifications = parse_sql_inserts(notifications_file)
        valid_notifications = []
        existing_notification_keys = set()
        max_id = 0

        for record in existing_notifications:
            if len(record) >= 5:
                try:
                    notification_id = int(record[0])
                    user_id = record[1]
                    subaccount_id = record[2] if record[2] != 'NULL' else None
                    message = record[3]
                    timestamp = record[4]
                    programme_id = None
                    for account in matching_accounts:
                        if account['user_id'] == user_id and account['subaccount_id'] == subaccount_id:
                            programme_id = account['programme_id']
                            break
                    if not programme_id:
                        log_and_print(f"Skipping invalid notification: user_id={user_id}, subaccount_id={subaccount_id or 'N/A'}, "
                                      f"message='{message}' (no matching programme)", "WARNING")
                        continue
                    
                    # Check for duplicates
                    key = (user_id, subaccount_id, programme_id, message)
                    if key not in existing_notification_keys:
                        valid_notifications.append({
                            'id': notification_id,
                            'user_id': user_id,
                            'subaccount_id': subaccount_id,
                            'message': message,
                            'timestamp': timestamp
                        })
                        existing_notification_keys.add(key)
                        max_id = max(max_id, notification_id)
                        log_and_print(f"Retained notification: id={notification_id}, user_id={user_id}, "
                                      f"subaccount_id={subaccount_id or 'N/A'}, message='{message}'", "DEBUG")
                    else:
                        log_and_print(f"Removing duplicate notification: user_id={user_id}, subaccount_id={subaccount_id or 'N/A'}, "
                                      f"message='{message}'", "INFO")
                except ValueError:
                    log_and_print(f"Skipping invalid notification ID: {record[0]}", "WARNING")
                    continue

        # Write cleaned notifications back to file
        clean_notifications_file(notifications_file, valid_notifications)
        print()

        # Step 4: Generate New Notifications
        log_and_print(" Generating New Notifications ", "INFO")
        inserted_count = 0
        for account in matching_accounts:
            user_id = account['user_id']
            subaccount_id = account['subaccount_id']
            programme_id = account['programme_id']
            programme_name = account['programme']
            account_type = account['account_type']
            broker_status = account['broker_status']
            status = account['status']
            initial_balance = account['initial_balance']

            log_and_print(f"Evaluating {account_type}: programme_id={programme_id}, user_id={user_id}, "
                          f"subaccount_id={subaccount_id or 'N/A'}, programme={programme_name}, "
                          f"status={status}, broker_status={broker_status}, initial_balance={initial_balance:.2f}", "INFO")

            # Define notification message based on status and broker_status
            notification_message = None
            if status == 'interested':
                if broker_status == 'credential_submitted':
                    notification_message = f"Your broker credentials for {programme_name} are under verification. " \
                                          f"Once validated, your programme will begin streaming."
                elif broker_status == 'login_successful':
                    notification_message = f"You're all set! Your {programme_name} credentials are verified. " \
                                          f"Take the next step and make a deposit to unlock streaming now!"
            elif status == 'invited' and broker_status == 'verified':
                user_rank = get_user_rank(user_id, users_file)
                if user_rank is None:
                    log_and_print(f"Skipping notification: No user_rank for user_id={user_id}, programme_id={programme_id}", "WARNING")
                    continue
                balance_threshold = 1 if user_rank == 'unique' else 100
                if initial_balance >= balance_threshold:
                    notification_message = f"You're all set, your {programme_name} has started streaming"
                elif initial_balance < balance_threshold:
                    notification_message = f"Your {programme_name} streaming is paused, fund your broker account now to activate it."
                else:
                    notification_message = f"Congrats, you have been invited for {programme_name} program"

            if not notification_message:
                log_and_print(f"Skipping notification: user_id={user_id}, {account_type}_id={subaccount_id or 'N/A'}, "
                              f"programme_id={programme_id}, programme={programme_name}, status={status}, "
                              f"broker_status={broker_status} (no matching condition)", "DEBUG")
                continue

            # Check for duplicate notifications
            key = (user_id, subaccount_id, programme_id, notification_message)
            if key in existing_notification_keys:
                log_and_print(f"Skipping duplicate: user_id={user_id}, {account_type}_id={subaccount_id or 'N/A'}, "
                              f"programme_id={programme_id}, programme={programme_name}, message='{notification_message}'", "INFO")
                continue

            # Create and append new notification
            log_and_print(f"Creating notification: user_id={user_id}, {account_type}_id={subaccount_id or 'N/A'}, "
                          f"programme_id={programme_id}, programme={programme_name}, message='{notification_message}'", "INFO")

            try:
                new_id = max_id + 1
                max_id = new_id
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                subaccount_id_sql = 'NULL' if subaccount_id is None else subaccount_id
                escaped_message = notification_message.replace("'", "''")
                notification_line = f"INSERT INTO notifications (id, user_id, subaccount_id, notification_messages, time_stamp) " \
                                   f"VALUES ({new_id}, {user_id}, {subaccount_id_sql}, '{escaped_message}', '{timestamp}');\n"

                try:
                    with open(notifications_file, 'a', encoding='utf-8') as f:
                        f.write(notification_line)
                    inserted_count += 1
                    total_inserted_count += 1
                    existing_notification_keys.add(key)
                    log_and_print(f"Inserted notification: user_id={user_id}, {account_type}_id={subaccount_id or 'N/A'}, "
                                  f"programme_id={programme_id}, programme={programme_name}, message='{notification_message}'", "SUCCESS")
                except Exception as e:
                    log_and_print(f"Failed to write notification: user_id={user_id}, {account_type}_id={subaccount_id or 'N/A'}, "
                                  f"programme_id={programme_id}, error={str(e)}", "ERROR")
            except Exception as e:
                log_and_print(f"Failed to process notification: user_id={user_id}, {account_type}_id={subaccount_id or 'N/A'}, "
                              f"programme_id={programme_id}, error={str(e)}", "ERROR")
        
        print()
        # Step 5: Verify contents of notifications.sql
        if inserted_count > 0:
            log_and_print(" Verifying notifications.sql ", "INFO")
            try:
                updated_notifications = parse_sql_inserts(notifications_file)
                log_and_print(f"Found {len(updated_notifications)} notifications in {notifications_file}", "DEBUG")
                for record in updated_notifications:
                    if len(record) >= 5:
                        user_id = record[1]
                        subaccount_id = record[2] if record[2] != 'NULL' else None
                        message = record[3]
                        timestamp = record[4]
                        log_and_print(f"Notification: id={record[0]}, user_id={user_id}, subaccount_id={subaccount_id or 'N/A'}, "
                                      f"message='{message}', timestamp={timestamp}", "DEBUG")
            except Exception as e:
                log_and_print(f"Failed to verify notifications.sql: {str(e)}", "ERROR")
        
        print()
        log_and_print(f" Directory Summary: {inserted_count} new notifications added to {notifications_file} ", "INFO" if inserted_count > 0 else "WARNING")
        print("-"*100 + "\n")

    print("="*100)
    log_and_print(f" Processing Complete: {total_inserted_count} new notifications inserted across all directories ", "SUCCESS" if total_inserted_count > 0 else "INFO")
    print("="*100 + "\n")

if __name__ == "__main__":
    verify_and_update_notifications()