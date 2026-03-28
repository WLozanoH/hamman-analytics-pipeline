from dotenv import load_dotenv   # Importa función para leer el archivo .env
import os                        # Importa módulo para trabajar con variables del sistema

def main():                      # Función principal del script
    load_dotenv()                # Carga las variables del archivo .env
    print("Pipeline iniciado correctamente 🚀")  # Muestra mensaje en consola

if __name__ == "__main__":       # Si este archivo se ejecuta directamente...
    main()                       # ...ejecuta la función principal