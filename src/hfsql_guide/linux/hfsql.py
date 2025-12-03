from hfsql_guide.settings.credentials import dsn, user, passwd
from hfsql_guide.settings.paths import PARQUET_DIR
from datetime import datetime
import pandas as pd
import subprocess
import sys
import os


def query_hfsql(sql_query):
    dsn_str = f"DSN={dsn};UID={user};PWD={passwd}" # Remember to change the values in .env

    command = f'echo "{sql_query}" | iodbctest "{dsn_str}"'

    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            encoding='latin-1' # Critical for special chars
        )

        if result.returncode != 0:
            raise Exception(f"Process error: {result.stderr}")

        return parse_iodbctest_output(result.stdout)

    except Exception as e:
        raise e

def parse_iodbctest_output(raw_text):
    lines = raw_text.splitlines()
    data = []
    headers = []
    separator_index = -1

    for i, line in enumerate(lines):
        if line.strip().startswith('---') and '+' in line:
            separator_index = i
            break

    if separator_index == -1:
        return []

    headers = [h.strip() for h in lines[separator_index - 1].split('|')]

    for line in lines[separator_index + 1:]:
        if not line.strip() or "result set" in line:
            continue

        values = [v.strip() for v in line.split('|')]

        if len(values) == len(headers):
            row_dict = dict(zip(headers, values))
            data.append(row_dict)

    return data

def save_to_parquet(data, prefix="query_result"):

    if not data:
        print("Not data found.")
        return

    try:
        df = pd.DataFrame(data)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{prefix}_{timestamp}.parquet"
        
        full_path = os.path.join(str(PARQUET_DIR), filename)
        

        df.to_parquet(full_path, index=False)
        print(f"    Table was succesfully saved in:\n   -> {full_path}")
        print(f"    Saved data: {len(df)}")
        
    except Exception as e:
        print(f"  An error ocurred while saving the data: {e}")

if __name__ == "__main__":
    print("\n --- HFSQL LINUX EXTRACTOR --- ")
    print(f" Path: {PARQUET_DIR}")
    print(" 'exit' to end.\n")

    while True:
        try:
            user_input = input("\n  Please insert your SQL query: ").strip()
            print("\n --- lower case with _ instead of white spaces please --- ")
            prefix = input("  Please insert the Name of the file: ").strip()
            
            if user_input.lower() in ['exit', 'quit']:
                print(" Bye!")
                break
            
            if not user_input:
                continue

            print("⏳ wait please...")
            resultados = query_hfsql(user_input)

            if resultados:
                save_to_parquet(resultados, prefix.lower())
            else:
                print("⚠️ Empty table.")

        except KeyboardInterrupt:
            print("\n Operation was cancelled.")
            break
        except Exception as e:
            print(f"❌ Error: {e}")