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

def standardize_column_names(df):
    """Estandariza nombre de las columnas a minúscula y sin espacios."""
    
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    return df


def clean_specific_column(df, col):
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


# =========================================================
# LIMPIEZA DIARIO
# =========================================================
def remove_empty_rows(df):
    """Remover filas completamente vacías"""
    
    df = df.dropna(how="all")
    
    return df


def select_columns(df):
    """Selecciona las primeras 10 columnas relevantes del archivo diario."""
    
    df = df.iloc[:,:10].copy()
    
    return df


def normalize_generic_names(df):
    """
    Estandariza nombres genéricos NIÑOS Y NIÑAS.
    Primero limpia, luego homologa.
    """
    
    mask_niños = df["nombre"].str.startswith("NIÑO", na=False)
    mask_niñas = df["nombre"].str.startswith("NIÑA", na=False)
    
    df.loc[mask_niños, "nombre"] = "NIÑOS"
    df.loc[mask_niñas, "nombre"] = "NIÑAS"
    
    df["nombre"] = df["nombre"].replace({
    "BRANDEA TU MARCA": pd.NA,
    "GREATS GROUP": pd.NA
    })

    return df


def clean_dates(df):
    """
    Limpia y estandariza la columna fecha.
    
        - Convierte valores a datetime usando formato day-first (dd/mm/yyyy).
        - Los valores no válidos se convierten a NaT.
        - Rellena automáticamente las fechas que faltan usando el valor superior.
    """
    
    df["fecha"] = pd.to_datetime(df["fecha"], errors= "coerce", dayfirst=True)
    df["fecha"] = df["fecha"].ffill()
    
    return df


def create_extract_match_key_diario(diario):
    """
    Extrae solo el primer nombre y primer apellido
    """
    diario["nombre_simple"] = diario["nombre"].apply(extract_first_name_first_surname)
    
    return diario   



# =========================================================
# MERGE POR CLAVE SEGURA
# =========================================================

def merge_clients_exact(df, clientes_unicos):
    """
    Merge exacto y conservador por nombre_simple.
    Solo son claves únicas en la base de clientes.
    """
    
    df = df.merge(
        clientes_unicos,
        on="nombre_simple",
        how="left",
        suffixes=("", "_cliente")
    )
    return df
    

# =========================================================
# COMPLETAR DNI Y NOMBRE
# =========================================================

def build_final_dni(df):
    """
    Prioridad:
    1. dni_cliente del merge
    2. dni original
    """
    
    df["dni_cliente"] = df["dni_cliente"].fillna(df["dni"])

    df["dni_cliente"] = (
        df["dni_cliente"]
        .astype("string").str.strip()
        .str.replace(r"\.0$", "", regex=True)
        .str.extract(r"(\d+)", expand=False)
    )

    return df
    
    
def standardize_invalid_dni(df):
    """Estandarizar DNIs inválidos.
    - Condiciones:
        - Que no haya valores nulos.
        - tamaño menor a 8 caracteres.
        - solo aplicable a dni_genéricos.
    """
    
    dni_genericos = {"123", "234", "1212"}
    
    mask = (
        df["dni_cliente"].notna() &
        (df["dni_cliente"].str.len() < 8) &
        (~df["dni_cliente"].isin(dni_genericos))
    )

    df.loc[mask, "dni_cliente"] = pd.NA

    return df


def fill_name_from_exact_match(df):
    """Completa la columna nombre usando nombre_y_apellido del merge"""
    
    df["nombre"] = df["nombre"].fillna(df["nombre_y_apellido"])
    
    return df


def fill_name_from_dni(df, clientes):
    """Recupera el nombre vacío desde la tabla de clientes usando DNI"""
    
    clientes_clean = (
        clientes[["dni", "nombre_y_apellido"]]
        .dropna(subset=["dni"])
        .drop_duplicates(subset="dni")
    )
    mapa = dict(zip(clientes_clean["dni"], clientes_clean["nombre_y_apellido"]))
    mask = df["nombre"].isna() & df["dni_cliente"].notna()
    df.loc[mask, "nombre"] = df.loc[mask, "dni_cliente"].map(mapa)
    
    return df


def fill_phone_from_dni(df, clientes):

    clientes_clean = (
        clientes[["dni", "celular"]]
        .dropna(subset=["dni"])
        .drop_duplicates(subset="dni")
    )

    mapa = dict(zip(clientes_clean["dni"], clientes_clean["celular"]))

    df["celular"] = df["celular"].fillna(df["dni_cliente"].map(mapa))

    return df


def fill_names_by_dni_group(df):
    """Rellena nombres faltantes usando agrupación por DNI."""
    df["nombre"] = (
        df.groupby("dni_cliente")["nombre"]
        .transform(lambda x: x.ffill().bfill())
    )

    return df


def assign_generic_names_by_dni(df):
    """Asigna nombres genéricos a DNIs especiales."""

    df.loc[df["dni_cliente"] == "123", "nombre"] = "NIÑOS"
    df.loc[df["dni_cliente"] == "234", "nombre"] = "NIÑAS"
    df.loc[df["dni_cliente"] == "1212", "nombre"] = "ACOMPAÑANTE"

    return df


def fill_unknown_names(df):
    """Asigna nombre desconocido a registros sin nombre."""

    df["nombre"] = df["nombre"].fillna("DESCONOCIDO")

    return df


def correction_final_dni(df):
    """
    - Asegurarse que no tenga espacios adicionales.
    - Extraer solo valores numéricos
    - Completa valores nulos por 999999999
    - Asegurarse que sea entero
    """
    df["dni_cliente"] = (
        df["dni_cliente"]
        .astype("string")
        .str.strip()
        .str.extract(r"(\d+)", expand=False)
    )
    
    df["dni_cliente"] = df["dni_cliente"].fillna("999999999")
    
    # convertir a entero para Power BI
    df["dni_cliente"] = df["dni_cliente"].astype("int64")
    
    return df


def run_etl():                                     
    """Ejecuta la fase Extract del pipeline ETL."""
    logging.info("Iniciando ETL")
    clientes, diario = extract_data()
    logging.info("Fase Extract completada correctamente")
    
    # LIMPIEZA CLIENTES
    logging.info("Iniciando limpieza de clientes")
    
    clientes = standardize_column_names(clientes) 
    clientes = clean_specific_column(clientes, "nombre_y_apellido")
    clientes = clean_dni_column(clientes, "dni")
    clientes = remove_duplicate_clients(clientes)
    clientes = create_extract_match_key_clientes(clientes)
    logging.info("Iniciando Extracción de claves únicas en nombre_simple")
    clientes_unicos = keep_unique_match_keys(clientes)
    
    logging.info("Limpieza de clientes completada correctamente")
    
    # LIMPIEZA DIARIO
    logging.info("Iniciando fase clean transacciones")
    
    diario = standardize_column_names(diario)
    diario = remove_empty_rows(diario)
    diario = select_columns(diario)
    diario = clean_specific_column(diario, "nombre")
    diario = normalize_generic_names(diario)
    diario = clean_dates(diario)
    diario = create_extract_match_key_diario(diario)

    logging.info("Fase clean transacciones completada correctamente.")
    
    # MERGE POR CLAVE SEGURA
    logging.info("Iniciando fase merge y matching")
    
    diario = merge_clients_exact(diario, clientes_unicos)
    diario = build_final_dni(diario)
    diario = standardize_invalid_dni(diario)
    diario = fill_name_from_exact_match(diario)
    diario = fill_name_from_dni(diario, clientes)
    diario = fill_phone_from_dni(diario, clientes)
    diario = fill_names_by_dni_group(diario)
    diario = assign_generic_names_by_dni(diario)
    diario = fill_unknown_names(diario)
    diario = correction_final_dni(diario)
    
    logging.info("Fase Merge y matching completada correctamente")
    
    
    

if __name__ == "__main__":
    run_etl()
