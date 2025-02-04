# etl_script/monthly_satisfaction_and_opened_closed.py

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import text

# Módulos propios
from etl_script.logger import setup_logging
from etl_script.db import get_engine
from etl_script.loader import load_data, load_monthly_satisfaction_average
from etl_script.transformations import (
    transform_monthly_satisfaction_average,
    transform_opened_closed_monthly
)
from etl_script.api_client import (
    get_monthly_satisfaction_average,
    get_opened_closed_monthly
)

def main():
    logger = setup_logging()
    logger.info("🚀 Inicio del proceso ETL para monthly_satisfaction & opened_closed_monthly")

    engine = get_engine()

    # 1) Definir las tareas
    tasks = [
        {
            "label": "monthly_satisfaction_average",
            "extract_fn": get_monthly_satisfaction_average,
            "extract_kwargs": {},
            "transform_fn": transform_monthly_satisfaction_average,
            "target_table": "monthly_satisfaction_average"
        },
        {
            "label": "opened_closed_monthly",
            "extract_fn": get_opened_closed_monthly,
            "extract_kwargs": {},
            "transform_fn": transform_opened_closed_monthly,
            "target_table": "opened_closed_monthly"
        },
    ]

    # 2) Truncar solo las tablas relevantes
    with engine.begin() as conn:
        logger.info("🧹 Truncando tablas: monthly_satisfaction_average, opened_closed_monthly...")
        conn.execute(text("TRUNCATE TABLE monthly_satisfaction_average;"))
        conn.execute(text("TRUNCATE TABLE opened_closed_monthly;"))

    # 3) Función para extraer y transformar
    def fetch_transform(task):
        label = task["label"]
        logger.info(f"[{label}] Iniciando extracción...")
        data = task["extract_fn"](**task["extract_kwargs"])
        logger.info(f"[{label}] Extracción completa. Transformando datos...")

        df = task["transform_fn"](data)
        logger.info(f"[{label}] Transformación completa. Filas obtenidas: {len(df)}")
        return (task["target_table"], df, label)

    # 4) Ejecutar en paralelo
    results = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        future_to_label = {executor.submit(fetch_transform, t): t["label"] for t in tasks}
        for future in as_completed(future_to_label):
            label = future_to_label[future]
            try:
                table_name, df, label = future.result()
                results.append((table_name, df, label))
            except Exception as e:
                logger.error(f"[{label}] Error en fetch_transform: {e}")

    # 5) Cargar
    for table_name, df, label in results:
        if df.empty:
            logger.warning(f"[{label}] DataFrame vacío. Se omite carga en '{table_name}'.")
            continue

        # Caso especial: monthly_satisfaction_average
        if table_name == "monthly_satisfaction_average":
            load_monthly_satisfaction_average(df, engine)
        else:
            load_data(df, table_name, engine)

        logger.info(f"[{label}] Carga completada en '{table_name}'.")

    # 6) (En este script, no generamos SLA porque no estamos cargando la tabla tickets)
    logger.info("🏁 Proceso ETL finalizado para monthly_satisfaction_and_opened_closed.")

if __name__ == "__main__":
    main()
