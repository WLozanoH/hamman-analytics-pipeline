@echo off
cd /d "%~dp0"

call venv\scripts\activate.bat

python src\run_pipeline.py

echo.
echo Proceso Finalizado
pause