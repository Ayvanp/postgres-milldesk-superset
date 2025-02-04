# etl_script/tickets_by_opening_time.py

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import text

# Módulos propios
from etl_script.logger import setup_logging
from etl_script.db import get_engine
from etl_script.loader import load_data
from etl_script.transformations import transform_tickets_by_hour
from etl_script.api_client import get_tickets_by_hour

def main():
    logger = setup_logging()
    logger.info("🚀 Inicio del proceso ETL para tickets_by_hour")

    engine = get_engine()

    tasks = [
        {
            "label": "tickets_by_hour",
            "extract_fn": get_tickets_by_hour,
            "extract_kwargs": {},
            "transform_fn": transform_tickets_by_hour,
            "target_table": "tickets_by_hour"
        }
    ]

    # 2) Truncar tabla
    with engine.begin() as conn:
        logger.info("🧹 Truncando tabla: tickets_by_hour...")
        conn.execute(text("TRUNCATE TABLE tickets_by_hour;"))

    # 3) fetch_transform
    def fetch_transform(task):
        label = task["label"]
        logger.info(f"[{label}] Iniciando extracción...")
        data = task["extract_fn"](**task["extract_kwargs"])
        logger.info(f"[{label}] Extracción completa. Transformando datos...")
        df = task["transform_fn"](data)
        logger.info(f"[{label}] Transformación completa. Filas obtenidas: {len(df)}")
        return (task["target_table"], df, label)

    # 4) Ejecución en paralelo
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
            logger.warning(f"[{label}] DataFrame vacío. Se omite carga.")
            continue
        load_data(df, table_name, engine)
        logger.info(f"[{label}] Carga completada en '{table_name}'.")

    logger.info("🏁 Proceso ETL finalizado para tickets_by_opening_time.")

if __name__ == "__main__":
    main()
