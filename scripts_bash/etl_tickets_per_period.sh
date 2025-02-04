#!/bin/bash

# Activar entorno virtual
source /home/yvan/Escritorio/app/etl_script/venv/bin/activate

# Ejecutar el script de tickets_per_period
python -m etl_script.tickets_per_period

# Desactivar entorno virtual
deactivate
