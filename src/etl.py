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


# =========================================================
# HELPERS GENERALES
# =========================================================

def normalize_text(series):
    
    return(
        series.astype("string")
        .str.strip()
        .str.upper()
    )
    
def replace_invalid_names(series):
    invalidos = {"-", "--", "---", "", "NAN", "NONE", "NULL", ".", "  "}
    
    series = (
        series.astype("string")
        .str.strip()
        .str.upper()
        .replace(r"\s+", " ", regex=True)
    )
    
    series = series.replace(list(invalidos), pd.NA)
    return series

def standarize_column_names(df):
    """Estandariza nombre de las columnas a minúscula y sin espacios."""
    
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    return df


def clean_especific_column(df, col):
    """Limpia y Estandariza los valores de una columnas específica de tipo texto"""
    
    df[col] = replace_invalid_names(df[col])
    
    return df


def extract_first_name_first_surname(nombre):
    """"
    Regla:
    - 4 o más tokens: primer nombre + primer apellido paterno
        GERSON EDUARDO LAVADO GOMEZ -> GERSON LAVADO
    - 3 tokens: primer nombre + segundo token
        JUAN QUISPE CASTILLO -> JUAN QUISPE
    - 2 tokens: se conserva
        XIMENA TORRES -> XIMENA TORRES
    - 1 token: se conserva
    """
    if pd.isna(nombre):
        return pd.NA
    
    tokens = str(nombre).strip().upper().split()
    tokens = [t for t in tokens if t]
    
    if len(tokens) >=4:
        return f"{tokens[0]} {tokens[2]}"
    
    elif len(tokens) == 3:
        return f"{tokens[0]} {tokens[1]}"
    
    elif len(tokens) == 2:
        return f"{tokens[0]} {tokens[1]}"
    
    elif len(tokens) == 1:
        return tokens[0]
    
    return pd.NA

# =========================================================
# Limpieza Clientes
# =========================================================
    

def clean_dni_column(df, column):
    """Limpia y Extrae solo números de la columna DNI."""  
        
    df[column] = (
        df[column]
        .astype(str)
        .str.strip()
        .replace(["-", "", "NAN", "nan", "None"], pd.NA)
        .str.extract(r"(\d+)", expand = False)
    )
    
    return df


def remove_duplicate_clients(clientes):
    """
    Prioriza registros con DNI.
    Luego elimina duplicados por DNI
    """
    clientes_con_dni = clientes[clientes["dni"].notna()].copy()
    clientes_sin_dni = clientes[clientes["dni"].isna()].copy()
    
    clientes_con_dni = clientes_con_dni.drop_duplicates(subset="dni", keep="first")
    
    clientes = pd.concat([clientes_con_dni, clientes_sin_dni], ignore_index = True)
    
    return clientes
        

def create_extract_match_key_clientes(clientes):
    """
    Extrae solo el primer nombre y primer apellido
    """
    clientes["nombre_simple"] = clientes["nombre_y_apellido"].apply(
        extract_first_name_first_surname
    )
    
    return clientes        
        

def keep_unique_match_keys(clientes):
    """
    Solo conserva claves nombre_simple que sean únicas.
    Así evitamos asignar DNI a nombres ambiguos
    """
    freq = clientes["nombre_simple"].value_counts(dropna=True)
    claves_unicas = freq[freq == 1].index
    
    clientes_unicos = clientes[clientes["nombre_simple"].isin(claves_unicas)].copy()
    
    return clientes_unicos


def run_etl():                                     
    """Ejecuta la fase Extract del pipeline ETL."""
    logging.info("Iniciando ETL")
    clientes, diario = extract_data()
    logging.info("Fase Extract completada correctamente")
    
    # LIMPIEZA CLIENTES
    logging.info("Iniciando limpieza de clientes")
    
    clientes = standarize_column_names(clientes) 
    clientes = clean_especific_column(clientes, "nombre_y_apellido")
    clientes = clean_dni_column(clientes, "dni")
    clientes = remove_duplicate_clients(clientes)
    clientes = create_extract_match_key_clientes(clientes)
    logging.info("Iniciando Extracción de claves únicas en nombre_simple")
    clientes_clean = keep_unique_match_keys(clientes)
    
    logging.info("Limpieza de clientes completada correctamente")
    
if __name__ == "__main__":
    run_etl()
