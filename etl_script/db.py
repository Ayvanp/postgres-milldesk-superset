# etl_script/db.py
from sqlalchemy import create_engine
from etl_script.config import DATABASE_URI
import logging

def get_engine():
    logger = logging.getLogger(__name__)
    try:
        engine = create_engine(DATABASE_URI)
        # Prueba de conexión
        with engine.connect() as connection:
            logger.info("🔗 Conexión a la base de datos establecida correctamente.")
        return engine
    except Exception as e:
        logger.critical(f"❌ Error al conectar con la base de datos: {e}")
        raise
