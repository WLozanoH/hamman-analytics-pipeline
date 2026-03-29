from dotenv import load_dotenv
import os

load_dotenv()       # Carga las variables del archivo .env

FILE_PATH = os.getenv("FILE_PATH")
OUTPUT_PATH = os.getenv("OUTPUT_PATH")