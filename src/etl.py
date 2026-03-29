import pandas as pd
import logging
from config import FILE_PATH                                

# =========================================================
# CONFIGURACIÓN
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

pd.options.mode.chained_assignment = None

# =========================================================
# EXTRACT
# =========================================================

def extract_data():
    """Lee las hojas del excel necesarias para el ETL."""
    logging.info("Leyendo archivo de Excel")
    
    clientes = pd.read_excel(FILE_PATH, sheet_name="BASE DE DATOS", engine="openpyxl")
    diario = pd.read_excel(FILE_PATH, sheet_name="HAMMAM DIARIO", engine="openpyxl")
    
    logging.info("Archivo leído correctamente")
    logging.info(f"Registros clientes: {len(clientes)}")
    logging.info(f"Registros diarios: {len(diario)}")

    return clientes, diario

def run_etl():                                     
    """Ejecuta la fase Extract del pipeline ETL."""
    logging.info("Iniciando ETL")
    
    clientes, diario = extract_data()
    
    logging.info("Fase Extract completada correctamente")
    
if __name__ == "__main__":
    run_etl()
