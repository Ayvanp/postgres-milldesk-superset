# etl_script/loader.py
import logging
import pandas as pd
from sqlalchemy import text

logger = logging.getLogger(__name__)

def load_data(df, table_name, engine, if_exists='append', index=False):
    """
    Carga un DataFrame a la tabla indicada. Por defecto, hace append.
    """
    if df.empty:
        logger.warning(f"⚠️ No hay datos para cargar en la tabla '{table_name}'.")
        return
    try:
        df.to_sql(table_name, engine, if_exists=if_exists, index=index, method='multi')
        logger.info(f"✅ Datos cargados exitosamente en la tabla '{table_name}'.")
    except Exception as e:
        error_message = str(e).split('\n')[0]
        logger.error(f"❌ Error al cargar datos en '{table_name}': {error_message}")

def load_activities_hours_by_department(df, engine):
    load_data(df, 'activities_hours_by_department', engine, if_exists='append')

def load_monthly_satisfaction_average(df, engine):
    """
    Carga los datos de satisfacción mensual promedio en la base de datos.
    """
    load_data(df, 'monthly_satisfaction_average', engine, if_exists='append')

