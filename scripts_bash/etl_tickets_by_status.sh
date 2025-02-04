#!/bin/bash

# Activar entorno virtual
source /home/yvan/Escritorio/app/etl_script/venv/bin/activate

# Ejecutar el script de tickets_by_status
python -m etl_script.tickets_by_status

# Desactivar entorno virtual
deactivate
