# etl_script/logger.py
import logging
from logging.handlers import RotatingFileHandler

def setup_logging(logfile='etl_process.log'):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')

    # Handler para archivo
    app_log_handler = RotatingFileHandler(logfile, maxBytes=5*1024*1024, backupCount=5)
    app_log_handler.setLevel(logging.INFO)
    app_log_handler.setFormatter(formatter)
    
    # Handler para consola
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    # Añade los handlers al logger raíz
    logger.addHandler(app_log_handler)
    logger.addHandler(console_handler)
    
    # Reducir verbosidad de sqlalchemy
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
    logging.getLogger('sqlalchemy.pool').setLevel(logging.WARNING)
    
    return logger
