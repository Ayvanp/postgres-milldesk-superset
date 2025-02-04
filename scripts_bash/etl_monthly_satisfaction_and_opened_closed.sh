#!/bin/bash

# Activar entorno virtual
source /home/yvan/Escritorio/app/etl_script/venv/bin/activate

# Ejecutar el script de monthly_satisfaction_and_opened_closed
python -m etl_script.monthly_satisfaction_and_opened_closed

# Desactivar entorno virtual
deactivate
