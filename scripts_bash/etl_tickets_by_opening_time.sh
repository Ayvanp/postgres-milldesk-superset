#!/bin/bash

# Activar entorno virtual
source /home/yvan/Escritorio/app/etl_script/venv/bin/activate

# Ejecutar el script de tickets_by_opening_time
python -m etl_script.tickets_by_opening_time

# Desactivar entorno virtual
deactivate
