from dotenv import load_dotenv                              # Importa función para leer el archivo .env
import os                                                   # Importa módulo para trabajar con variables del sistema
from pathlib import Path

load_dotenv()       # Carga las variables del archivo .env

# raíz del proyecto
BASE_DIR = Path(__file__).resolve().parent.parent

# variables del .env
FILE_PATH = BASE_DIR / os.getenv("FILE_PATH")
OUTPUT_PATH = BASE_DIR / os.getenv("OUTPUT_PATH")

SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE")
SQL_DRIVER = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")
SQL_TRUST_CERT = os.getenv("SQL_TRUST_CERT", "yes")
SQL_USE_WINDOWS_AUTH = os.getenv("SQL_USE_WINDOWS_AUTH", "yes")

SQL_USERNAME = os.getenv("SQL_USERNAME")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")


if not FILE_PATH.exists():
    raise FileNotFoundError(f"No se encontró el archivo de entrada: {FILE_PATH}")