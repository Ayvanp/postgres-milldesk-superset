#!/bin/bash

# Activar entorno virtual
source /home/yvan/Escritorio/app/etl_script/venv/bin/activate

# Ejecutar el script de ticket_status
python -m etl_script.ticket_status

# Desactivar entorno virtual
deactivate
