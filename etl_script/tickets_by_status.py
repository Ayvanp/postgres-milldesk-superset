# etl_script/tickets_by_status.py

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import text

# Módulos propios
from etl_script.logger import setup_logging
from etl_script.db import get_engine
from etl_script.loader import load_data
from etl_script.transformations import transform_tickets
from etl_script.api_client import get_tickets_by_status

def main():
    logger = setup_logging()
    logger.info("🚀 Inicio del proceso ETL para tickets_by_status (y SLA)")

    engine = get_engine()

    # 1) Definir estatus
    statuses = ['Pendiente', 'Abierto', 'Asignado', 'Listo para cierre']

    # 2) Definir las tareas (una por cada status)
    tasks = [
        {
            "label": f"tickets_by_status_{st}",
            "extract_fn": get_tickets_by_status,
            "extract_kwargs": {"status": st},
            "transform_fn": transform_tickets,
            "target_table": "tickets"
        }
        for st in statuses
    ]

    # 3) Truncar la tabla 'tickets' (y las que dependan de ella, si procede)
    with engine.begin() as conn:
        logger.info("🧹 Truncando tabla: tickets...")
        conn.execute(text("TRUNCATE TABLE tickets;"))

    # 4) fetch_transform
    def fetch_transform(task):
        label = task["label"]
        logger.info(f"[{label}] Iniciando extracción...")
        data = task["extract_fn"](**task["extract_kwargs"])
        logger.info(f"[{label}] Extracción completa. Transformando datos...")

        df = task["transform_fn"](data)
        logger.info(f"[{label}] Transformación completa. Filas obtenidas: {len(df)}")
        return (task["target_table"], df, label)

    # 5) Ejecutar en paralelo
    results = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        future_to_label = {executor.submit(fetch_transform, t): t["label"] for t in tasks}
        for future in as_completed(future_to_label):
            label = future_to_label[future]
            try:
                table_name, df, lbl = future.result()
                results.append((table_name, df, lbl))
            except Exception as e:
                logger.error(f"[label={label}] Error en fetch_transform: {e}")

    # 6) Cargar
    for table_name, df, label in results:
        if df.empty:
            logger.warning(f"[{label}] DataFrame vacío. Se omite carga.")
            continue
        load_data(df, table_name, engine)
        logger.info(f"[{label}] Carga completada en '{table_name}'.")

    # 7) Generar tabla SLA (igual que en tu main)
    try:
        logger.info("⏳ Generando la tabla 'tickets_sla_detalle' con los cálculos SLA...")
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE tickets_sla_detalle;"))

            insert_query = """
                INSERT INTO tickets_sla_detalle (
                    id,
                    ticket,
                    requester,
                    status,
                    slasexpirationdate,
                    estado_sla,
                    tiempo_restante_intervalo,
                    dias_restantes,
                    horas_restantes,
                    minutos_restantes,
                    segundos_restantes
                )
                SELECT
                    t.id,
                    t.ticket,
                    t.requester,
                    t.status,
                    t.slasexpirationdate,

                    CASE
                       WHEN t.slasexpirationdate = 'SLA expirado' THEN 'SLA expirado (manual)'
                       WHEN t.slasexpirationdate ~ '^\d{2}/\d{2}/\d{4}\s\d{2}:\d{2}$' THEN
                         CASE 
                            WHEN to_timestamp(t.slasexpirationdate, 'DD/MM/YYYY HH24:MI') < NOW() 
                                 THEN 'VENCIDO'
                            ELSE 'DENTRO DE SLA'
                         END
                       ELSE 'SIN FECHA VÁLIDA'
                    END AS estado_sla,

                    CASE
                       WHEN t.slasexpirationdate ~ '^\d{2}/\d{2}/\d{4}\s\d{2}:\d{2}$'
                       THEN date_trunc('second', to_timestamp(t.slasexpirationdate, 'DD/MM/YYYY HH24:MI') - NOW())
                       ELSE NULL
                    END AS tiempo_restante_intervalo,

                    EXTRACT(DAY FROM (
                      CASE 
                         WHEN t.slasexpirationdate ~ '^\d{2}/\d{2}/\d{4}\s\d{2}:\d{2}$'
                         THEN to_timestamp(t.slasexpirationdate, 'DD/MM/YYYY HH24:MI') - NOW()
                         ELSE INTERVAL '0'
                      END
                    )) AS dias_restantes,

                    EXTRACT(HOUR FROM (
                      CASE 
                         WHEN t.slasexpirationdate ~ '^\d{2}/\d{2}/\d{4}\s\d{2}:\d{2}$'
                         THEN to_timestamp(t.slasexpirationdate, 'DD/MM/YYYY HH24:MI') - NOW()
                         ELSE INTERVAL '0'
                      END
                    )) AS horas_restantes,

                    EXTRACT(MINUTE FROM (
                      CASE 
                         WHEN t.slasexpirationdate ~ '^\d{2}/\d{2}/\d{4}\s\d{2}:\d{2}$'
                         THEN to_timestamp(t.slasexpirationdate, 'DD/MM/YYYY HH24:MI') - NOW()
                         ELSE INTERVAL '0'
                      END
                    )) AS minutos_restantes,

                    CASE 
                       WHEN t.slasexpirationdate ~ '^\d{2}/\d{2}/\d{4}\s\d{2}:\d{2}$'
                       THEN (EXTRACT(SECOND FROM (to_timestamp(t.slasexpirationdate, 'DD/MM/YYYY HH24:MI') - NOW())))::int
                       ELSE 0
                    END AS segundos_restantes

                FROM public.tickets t
                WHERE t.slasexpirationdate IS NOT NULL
                ORDER BY t.id DESC;
            """
            conn.execute(text(insert_query))

        logger.info("✅ Tabla 'tickets_sla_detalle' generada con éxito.")
    except Exception as e:
        logger.error(f"❌ Error al generar la tabla 'tickets_sla_detalle': {e}")

    logger.info("🏁 Proceso ETL finalizado para tickets_by_status (y SLA).")

if __name__ == "__main__":
    main()
