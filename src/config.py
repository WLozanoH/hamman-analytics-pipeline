from dotenv import load_dotenv                              # Importa función para leer el archivo .env
import os                                                   # Importa módulo para trabajar con variables del sistema
from pathlib import Path

load_dotenv()       # Carga las variables del archivo .env

# raíz del proyecto
BASE_DIR = Path(__file__).resolve().parent.parent

# variables del .env
FILE_PATH = BASE_DIR / os.getenv("FILE_PATH")
OUTPUT_PATH = BASE_DIR / os.getenv("OUTPUT_PATH")

if not FILE_PATH.exists():
    raise FileNotFoundError(f"No se encontró el archivo de entrada: {FILE_PATH}")