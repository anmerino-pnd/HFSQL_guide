import os
from pydantic import BaseModel
from dotenv import load_dotenv

dsn : str = os.getenv('DSN_NAME') or ""
user: str = os.getenv('USERNAME') or ""
passwd: str = os.getenv('PASSWORD') or ""