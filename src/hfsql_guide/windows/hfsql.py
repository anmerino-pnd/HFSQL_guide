from hfsql_guide.settings.paths import DATA_DIR, PARQUET_DIR, BLACKLISTED_TABLES, FAILED_TABLES
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from hfsql_guide.settings.credentials import dsn, user, passwd
import pandas as pd
import pypyodbc
import warnings
import sys

TIMEOUT_SECONDS = 3 * 60  

tables = pd.read_excel(DATA_DIR / 'Tables_name.xlsx').iloc(axis=1)[0].tolist()

connection_string = (
    f"DSN={dsn};"
    f"UID={user};"
    f"PWD={passwd};"
)

def log_failed(table: str, reason: str):
    """Append failed table info to the log."""
    with FAILED_TABLES.open("a", encoding="utf8") as f:
        f.write(f"{table}  ---  {reason}\n")

def run_query_with_timeout(query: str, connection_string: str, timeout: int):
    """
    Runs a query in a separate thread, creating its OWN connection
    to prevent "zombie" hangs.
    """
    
    def _run_query_in_thread():
        """This function runs in the isolated thread."""
        conn_thread = None
        try:
            conn_thread = pypyodbc.connect(connection_string, autocommit=True)
            

            df = pd.read_sql(query, conn_thread, dtype="object")  # pyright: ignore[reportArgumentType]
            return df
        finally:
            if conn_thread:
                conn_thread.close()

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(_run_query_in_thread)
        return future.result(timeout=timeout)

with open(BLACKLISTED_TABLES, 'r', encoding='utf8') as f:
    blacklist = [line.strip() for line in f if line.strip()]

with open(FAILED_TABLES, 'r', encoding='utf8') as f:
    already_failed = [line.split('  ---  ')[0] for line in f if line.strip()]

try:
    existing_files = set(f.stem for f in PARQUET_DIR.glob("*.parquet"))
    print(f"{len(existing_files)} .parquet files found.")
except FileNotFoundError:
    print("Nonexistent PARQUET_DIR, all files will be created.")
    existing_files = set()

warnings.filterwarnings('ignore', category=UserWarning)
warnings.filterwarnings('ignore', category=FutureWarning)

try:
    print("Database connection established.")

    for i, table in enumerate(tables, start=1):
        print(f"Table {i} of {len(tables)}")
        if table in existing_files:
            print(f"    Skipping table {i}, .parquet file already exists.")
            continue
        if table in already_failed:
            print(f"    Skipping previously failed table: {i}")
            continue
        if table in blacklist:
            print(f"    Skipping blacklisted table: {i}")
            continue
        try:  
            query = f"SELECT * FROM {table}"

            try:
                df = run_query_with_timeout(query, connection_string, TIMEOUT_SECONDS)
            except TimeoutError:
                print(f"    Timeout for {table}", file=sys.stderr)
                log_failed(table, "timeout")
                continue

            df = df.astype('string')  # convert all columns to string dtype to avoid type issues
            df = df.replace(['', ' ', 'NULL'], pd.NA) # replace empty strings and 'NULL' with NaN
            df.to_parquet(PARQUET_DIR / f"{table}.parquet", index=False)          

        except Exception as e:
            print(f"    An error occurred: {e}", file=sys.stderr)
            log_failed(table, str(e))
            continue

    print("Data export completed successfully.")

except pypyodbc.Error as e:
    print(f"Database connection error: {e}", file=sys.stderr)

except Exception as e:
    print(f"    An unexpected error occurred: {e}", file=sys.stderr)        