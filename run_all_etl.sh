#!/bin/bash

# 🏁 Script para ejecutar todas las tareas ETL juntas y verificar su correcto funcionamiento.

LOG_FILE="etl_test_run.log"
VENV_PATH="/home/yvan/Escritorio/app/etl_script/venv/bin/activate"

# Activar el entorno virtual
echo "🚀 Activando entorno virtual..."
source $VENV_PATH

# Función para ejecutar cada script ETL y registrar logs
run_etl_task() {
    local script_name=$1
    echo "--------------------------------------" | tee -a $LOG_FILE
    echo "▶️ Ejecutando: $script_name" | tee -a $LOG_FILE
    echo "--------------------------------------" | tee -a $LOG_FILE
    python -m etl_script.$script_name >> $LOG_FILE 2>&1
    if [ $? -eq 0 ]; then
        echo "✅ $script_name ejecutado exitosamente" | tee -a $LOG_FILE
    else
        echo "❌ ERROR en $script_name (revisar logs)" | tee -a $LOG_FILE
    fi
    echo "" | tee -a $LOG_FILE
}

# Ejecutar cada tarea ETL individualmente
run_etl_task "monthly_satisfaction_and_opened_closed"
run_etl_task "ticket_status"
run_etl_task "tickets_by_opening_time"
run_etl_task "activities_hours_and_listTicketsActivities"
run_etl_task "tickets_per_period"
run_etl_task "tickets_by_status"

# Desactivar el entorno virtual
echo "🔻 Desactivando entorno virtual..."
deactivate

echo "✅ Todos los procesos ETL han sido ejecutados. Revisa $LOG_FILE para verificar los resultados."
