from dotenv import load_dotenv                      # Importa función para leer el archivo .env
import os                                           # Importa módulo para trabajar con variables del sistema
from config import FILE_PATH, OUTPUT_PATH   

def main():                                         # Función principal del script
    load_dotenv()                                   # Carga las variables del archivo .env
    print("Pipeline iniciado correctamente 🚀")     # Muestra mensaje en consola
    print(f"Archivo de entrada {FILE_PATH}")
    print(f"Archivo de salida {OUTPUT_PATH}")

if __name__ == "__main__":                          # Si este archivo se ejecuta directamente...
    main()                                          # ...ejecuta la función principal
    