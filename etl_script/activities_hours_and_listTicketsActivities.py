# etl_script/activities_hours_and_listTicketsActivities.py

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import text

# Módulos propios
from etl_script.logger import setup_logging
from etl_script.db import get_engine
from etl_script.loader import load_data
from etl_script.transformations import (
    transform_activities_hours_to_charge,
    transform_ticket_activities
)
from etl_script.api_client import (
    get_activities_hours_to_charge,
    get_ticket_activities
)

def main():
    logger = setup_logging()
    logger.info("🚀 Inicio del proceso ETL para activitiesHoursToCharge + listTicketsActivities")

    engine = get_engine()

    # 1) Tareas (2 endpoints)
    tasks = [
        {
            "label": "activities_hours_to_charge",
            "extract_fn": get_activities_hours_to_charge,
            "extract_kwargs": {},
            "transform_fn": transform_activities_hours_to_charge,
            "target_table": "activities_hours_to_charge"
        },
        {
            "label": "ticket_activities",
            "extract_fn": get_ticket_activities,
            "extract_kwargs": {},
            "transform_fn": transform_ticket_activities,
            "target_table": "ticket_activities"
        }
    ]

    # 2) Truncar tablas
    with engine.begin() as conn:
        logger.info("🧹 Truncando tablas: activities_hours_to_charge, ticket_activities...")
        conn.execute(text("TRUNCATE TABLE activities_hours_to_charge;"))
        conn.execute(text("TRUNCATE TABLE ticket_activities;"))

    # 3) fetch_transform
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
    with ThreadPoolExecutor(max_workers=2) as executor:
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
            logger.warning(f"[{label}] DataFrame vacío. Se omite carga.")
            continue
        load_data(df, table_name, engine)
        logger.info(f"[{label}] Carga completada en '{table_name}'.")

    # 6) No generamos SLA (no involucra la tabla tickets).
    logger.info("🏁 Proceso ETL finalizado para activitiesHoursToCharge + listTicketsActivities.")

if __name__ == "__main__":
    main()
