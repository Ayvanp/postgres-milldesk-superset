#!/bin/bash

# Activar entorno virtual
source /home/yvan/Escritorio/app/etl_script/venv/bin/activate

# Ejecutar el script de activities_hours_and_listTicketsActivities
python -m etl_script.activities_hours_and_listTicketsActivities

# Desactivar entorno virtual
deactivate
