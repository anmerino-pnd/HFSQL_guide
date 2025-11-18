# config.py
from pathlib import Path

def find_project_root(start_path: Path, marker_file: str = "pyproject.toml") -> Path:
    current = start_path.resolve()
    while not (current / marker_file).exists() and current != current.parent:
        current = current.parent
    return current

BASE_DIR = find_project_root(Path(__file__))

DATA_DIR = BASE_DIR / "data"
PARQUET_DIR = DATA_DIR / "parquet" 
FAILED_TABLES = DATA_DIR / "failed_tables.txt"
BLACKLISTED_TABLES = DATA_DIR / "blacklist.txt"


for path in [
    DATA_DIR,
    PARQUET_DIR
             ]:
    path.mkdir(parents=True, exist_ok=True)

for file in [
    FAILED_TABLES, 
    BLACKLISTED_TABLES]:
    file.touch(exist_ok=True)
