# config.py
from pathlib import Path

def find_project_root(start_path: Path, marker_file: str = "pyproject.toml") -> Path:
    current = start_path.resolve()
    while not (current / marker_file).exists() and current != current.parent:
        current = current.parent
    return current

BASE_DIR = find_project_root(Path(__file__))

DATA_DIR = BASE_DIR / "data"
CSV_DIR = BASE_DIR / "csv" 


for path in [
    DATA_DIR,
    CSV_DIR
             ]:
    path.mkdir(parents=True, exist_ok=True)
