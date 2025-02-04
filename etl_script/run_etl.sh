#!/bin/bash
# Activar el entorno virtual
source /home/yvan/Escritorio/etl_script/venv/bin/activate

# Cambiar al directorio raíz del proyecto
cd /home/yvan/Escritorio

# Ejecutar el script ETL
python -m etl_script.main

# Desactivar el entorno virtual
deactivate
