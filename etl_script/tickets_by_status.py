# etl_script/tickets_by_status.py
import logging
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

    # 1) Extracción: Llamar al endpoint sin filtrar por estado (se obtiene toda la info)
    logger.info("Iniciando extracción de tickets (todos los estados)...")
    # Al pasar una cadena vacía, se genera la URL:
    data = get_tickets_by_status("")
    logger.info("Extracción completada. Transformando datos...")

    # 2) Transformación
    df = transform_tickets(data)
    logger.info(f"Transformación completada. Total de filas obtenidas: {len(df)}")

    # 3) Truncar la tabla 'tickets'
    with engine.begin() as conn:
        logger.info("🧹 Truncando tabla: tickets...")
        conn.execute(text("TRUNCATE TABLE tickets;"))

    # 4) Cargar datos
    if df.empty:
        logger.warning("DataFrame vacío. Se omite carga en 'tickets'.")
    else:
        load_data(df, "tickets", engine)
        logger.info("Carga completada en la tabla 'tickets'.")

    # 5) Generar la tabla SLA (proceso sin cambios respecto a la versión anterior)
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

