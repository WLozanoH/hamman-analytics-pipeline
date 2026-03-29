# Orquestador de todo el pipeline controlado desde aquí
from etl import run_etl

def main():                                         # Función principal del script
    print("Pipeline iniciado correctamente 🚀")     # Muestra mensaje en consola
    run_etl()
    print("Pipeline Finalizado correctamente ✅") 

if __name__ == "__main__":                          # Si este archivo se ejecuta directamente...
    main()                                          # ...ejecuta la función principal
    