import connectwithinfinitydb as db
import os
from typing import List, Dict, Optional
from colorama import Fore, Style, init
import logging
import time
import asyncio

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
    """Manages configuration settings for table column fetching and modification."""
    def __init__(self):
        self.main_export_dir: str = EXPORT_DIR

    def validate_directory(self) -> bool:
        """Validate the export directory exists and is writable."""
        if not os.path.exists(self.main_export_dir) or not os.access(self.main_export_dir, os.W_OK):
            log_and_print(f"Invalid or inaccessible directory: {self.main_export_dir}", "ERROR")
            return False
        return True

# Table Column Manager Class
class TableColumnManager:
    """Manages fetching and modifying column names and data for specified tables."""
    def __init__(self):
        self.config = ConfigManager()

    async def fetch_table_columns(self, table_name: str) -> Optional[List[str]]:
        """Fetch the column names for a specified table, ensuring uniqueness."""
        sql_query = f"""
            SELECT DISTINCT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table_name}'
        """
        log_and_print(f"Fetching columns for table '{table_name}' with query: {sql_query}", "INFO")
        
        for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
            result = db.execute_query(sql_query)
            if result['status'] == 'success' and isinstance(result['results'], list):
                columns = [col['COLUMN_NAME'] for col in result['results']]
                # Remove duplicates while preserving order
                seen = set()
                unique_columns = [c for c in columns if not (c in seen or seen.add(c))]
                log_and_print(f"Successfully fetched {len(unique_columns)} unique columns for table '{table_name}'", "SUCCESS")
                return unique_columns
            else:
                error_message = result.get('message', 'No message provided')
                log_and_print(f"Failed to fetch columns for table '{table_name}' on attempt {attempt}: {error_message}", "ERROR")
                if attempt < RETRY_MAX_ATTEMPTS:
                    delay = RETRY_DELAY * (2 ** (attempt - 1))
                    log_and_print(f"Retrying after {delay} seconds...", "INFO")
                    await asyncio.sleep(delay)
                else:
                    log_and_print(f"Max retries reached for fetching columns of table '{table_name}'", "ERROR")
                    return None
        return None

    async def validate_columns(self, table_name: str, columns: List[str]) -> List[str]:
        """Validate that the provided columns exist in the table by attempting a SELECT query."""
        valid_columns = []
        for column in columns:
            sql_query = f"SELECT {column} FROM {table_name} WHERE 1=0"
            result = db.execute_query(sql_query)
            if result['status'] == 'success':
                valid_columns.append(column)
        return valid_columns

    async def fetch_table_data(self, table_name: str) -> Optional[List[Dict]]:
        """Fetch all data from the specified table, using only valid columns."""
        columns = await self.fetch_table_columns(table_name)
        if not columns:
            log_and_print(f"Cannot fetch data for table '{table_name}' due to missing column information", "ERROR")
            return None

        # Validate columns to ensure they exist
        valid_columns = await self.validate_columns(table_name, columns)
        if not valid_columns:
            log_and_print(f"No valid columns found for table '{table_name}' after validation", "ERROR")
            return None

        if len(valid_columns) < len(columns):
            log_and_print(f"Reduced columns from {len(columns)} to {len(valid_columns)} after validation", "INFO")

        sql_query = f"SELECT {', '.join(valid_columns)} FROM {table_name}"
        log_and_print(f"Fetching data for table '{table_name}' with query: {sql_query}", "INFO")
        
        for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
            result = db.execute_query(sql_query)
            if result['status'] == 'success' and isinstance(result['results'], list):
                log_and_print(f"Successfully fetched {len(result['results'])} rows for table '{table_name}'", "SUCCESS")
                return result['results']
            else:
                error_message = result.get('message', 'No message provided')
                log_and_print(f"Failed to fetch data for table '{table_name}' on attempt {attempt}: {error_message}", "ERROR")
                if attempt < RETRY_MAX_ATTEMPTS:
                    delay = RETRY_DELAY * (2 ** (attempt - 1))
                    log_and_print(f"Retrying after {delay} seconds...", "INFO")
                    await asyncio.sleep(delay)
                else:
                    log_and_print(f"Max retries reached for fetching data from table '{table_name}'", "ERROR")
                    return None
        return None

    async def column_for_tables(self, table: str, alter: Optional[str] = None, remove: Optional[str] = None, data: Optional[str] = None) -> Dict[str, any]:
        """Alter, remove a column, or fetch data from the specified table."""
        result = {"status": False, "data": None}

        if alter and remove:
            log_and_print(f"Cannot specify both alter and remove for table '{table}'", "ERROR")
            return result

        current_columns = await self.fetch_table_columns(table)
        if not current_columns:
            log_and_print(f"Failed to fetch current columns for table '{table}', aborting operation", "ERROR")
            return result

        if alter:
            # Validate column name (basic check for valid SQL identifier)
            if not alter.isidentifier() or any(c in alter for c in ' ;,'):
                log_and_print(f"Invalid column name for alter: '{alter}' in table '{table}'", "ERROR")
                return result

            # Check if column already exists
            if alter in current_columns:
                log_and_print(f"Column '{alter}' already exists in table '{table}', no action taken", "WARNING")
                result["status"] = True
            else:
                # Add new column (using DATETIME for programmetrade_startdate, VARCHAR(255) for others)
                column_type = 'DATETIME' if alter == 'programmetrade_startdate' else 'VARCHAR(255)'
                sql_query = f"""
                    ALTER TABLE {table}
                    ADD {alter} {column_type} NULL
                """
                log_and_print(f"Adding column '{alter}' to table '{table}' with query: {sql_query}", "INFO")

                for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
                    result_query = db.execute_query(sql_query)
                    if result_query['status'] == 'success':
                        log_and_print(f"Successfully added column '{alter}' to table '{table}'", "SUCCESS")
                        result["status"] = True
                        break
                    else:
                        error_message = result_query.get('message', 'No message provided')
                        log_and_print(f"Failed to add column '{alter}' to table '{table}' on attempt {attempt}: {error_message}", "ERROR")
                        if attempt < RETRY_MAX_ATTEMPTS:
                            delay = RETRY_DELAY * (2 ** (attempt - 1))
                            log_and_print(f"Retrying after {delay} seconds...", "INFO")
                            await asyncio.sleep(delay)
                        else:
                            log_and_print(f"Max retries reached for adding column '{alter}' to table '{table}'", "ERROR")
                            return result

        elif remove:
            # Validate column name
            if not remove.isidentifier() or any(c in remove for c in ' ;,'):
                log_and_print(f"Invalid column name for remove: '{remove}' in table '{table}'", "ERROR")
                return result

            # Check if column exists
            if remove not in current_columns:
                log_and_print(f"Column '{remove}' does not exist in table '{table}', no action taken", "WARNING")
                result["status"] = True
            else:
                # Remove column
                sql_query = f"""
                    ALTER TABLE {table}
                    DROP COLUMN {remove}
                """
                log_and_print(f"Removing column '{remove}' from table '{table}' with query: {sql_query}", "INFO")

                for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
                    result_query = db.execute_query(sql_query)
                    if result_query['status'] == 'success':
                        log_and_print(f"Successfully removed column '{remove}' from table '{table}'", "SUCCESS")
                        result["status"] = True
                        break
                    else:
                        error_message = result_query.get('message', 'No message provided')
                        log_and_print(f"Failed to remove column '{remove}' from table '{table}' on attempt {attempt}: {error_message}", "ERROR")
                        if attempt < RETRY_MAX_ATTEMPTS:
                            delay = RETRY_DELAY * (2 ** (attempt - 1))
                            log_and_print(f"Retrying after {delay} seconds...", "INFO")
                            await asyncio.sleep(delay)
                        else:
                            log_and_print(f"Max retries reached for removing column '{remove}' to table '{table}'", "ERROR")
                            return result

        if data == "fetch":
            table_data = await self.fetch_table_data(table)
            if table_data:
                result["data"] = table_data
                result["status"] = True
                log_and_print(f"Data fetched for table '{table}': {len(table_data)} rows", "SUCCESS")
            else:
                log_and_print(f"Failed to fetch data for table '{table}'", "ERROR")
                result["status"] = False

        elif data is not None:
            log_and_print(f"Invalid data parameter: '{data}' for table '{table}'. Expected 'fetch' or None", "ERROR")
            result["status"] = False

        return result

    async def alter_tables(self, tables: List[str]) -> Dict[str, Optional[List[str]]]:
        """Fetch column names for the specified tables."""
        print("\n")
        log_and_print("===== Table Column Fetching Process =====", "TITLE")
        log_and_print(f"Directory: {self.config.main_export_dir}", "INFO")

        if not self.config.validate_directory():
            log_and_print("Aborting column fetching due to invalid directory", "ERROR")
            print("\n")
            return {table: None for table in tables}

        columns = {table: None for table in tables}

        for table in tables:
            table_columns = await self.fetch_table_columns(table)
            if table_columns:
                columns[table] = table_columns
                log_and_print(f"{table.capitalize()} Columns: {table_columns}", "DEBUG")
            else:
                log_and_print(f"Failed to fetch columns for table '{table}'", "ERROR")

        print("\n")
        log_and_print("===== Column Fetching Summary =====", "TITLE")
        for table in tables:
            log_and_print(f"{table.capitalize()} Columns Fetched: {'Yes' if columns[table] else 'No'}", "INFO")
        print("\n")
        log_and_print("===== Table Column Fetching Process Completed =====", "TITLE")

        return columns

# Main Execution Function
async def main():
    """Main function to control table column fetching and modification."""
    manager = TableColumnManager()
    
    # Specify tables to fetch columns for
    tables = ["user_programmes", "users"]
    
    # Fetch columns for specified tables
    columns = await manager.alter_tables(tables)
    
    # Example column modifications and data fetching
    # Add 'programmetrade_startdate' to user_programmes
    result = await manager.column_for_tables("user_programmes", alter="programmetrade_startdate", remove=None, data="fetch")
    if result["status"] and result["data"]:
        log_and_print(f"Fetched data for user_programmes: {len(result['data'])} rows", "DEBUG")
    
    # Remove 'programmetrade_startdate' from user_programmes and fetch data
    result = await manager.column_for_tables("user_programmes", alter=None, remove="programmetrade_startdate", data="fetch")
    if result["status"] and result["data"]:
        log_and_print(f"Fetched data for user_programmes after column removal: {len(result['data'])} rows", "DEBUG")
    
    # Fetch columns again to show updated structure
    updated_columns = await manager.alter_tables(tables)
    
    return updated_columns

if __name__ == "__main__":
    asyncio.run(main())