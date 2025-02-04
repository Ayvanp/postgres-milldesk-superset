# etl_script/ticket_status.py

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import text

# Módulos propios
from etl_script.logger import setup_logging
from etl_script.db import get_engine
from etl_script.loader import load_data
from etl_script.transformations import transform_ticket_status
from etl_script.api_client import get_ticket_status

def main():
    logger = setup_logging()
    logger.info("🚀 Inicio del proceso ETL para ticket_status")

    engine = get_engine()

    # 1) Definir las tareas
    tasks = [
        {
            "label": "ticket_status",
            "extract_fn": get_ticket_status,
            "extract_kwargs": {},
            "transform_fn": transform_ticket_status,
            "target_table": "ticket_status"
        }
    ]

    # 2) Truncar tabla relevante
    with engine.begin() as conn:
        logger.info("🧹 Truncando tabla: ticket_status...")
        conn.execute(text("TRUNCATE TABLE ticket_status CASCADE;"))

    # 3) Función para extraer y transformar
    def fetch_transform(task):
        label = task["label"]
        logger.info(f"[{label}] Iniciando extracción...")
        data = task["extract_fn"](**task["extract_kwargs"])
        logger.info(f"[{label}] Extracción completa. Transformando datos...")

        df = task["transform_fn"](data)
        logger.info(f"[{label}] Transformación completa. Filas obtenidas: {len(df)}")
        return (task["target_table"], df, label)

    # 4) Ejecutar en paralelo (aunque aquí solo hay 1 tarea, no pasa nada)
    results = []
    with ThreadPoolExecutor(max_workers=1) as executor:
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
        load_data(df, table_name, engine)

    # 6) No SLA (porque no estamos tocando la tabla tickets)
    logger.info("🏁 Proceso ETL finalizado para ticket_status.")

if __name__ == "__main__":
    main()
