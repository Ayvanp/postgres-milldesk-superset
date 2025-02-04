# etl_script/config.py
import os
from dotenv import load_dotenv
from pathlib import Path

# Determinar la ruta del archivo .env
env_path = Path(__file__).parent.parent / '.env'

# Cargar las variables de entorno desde el archivo .env
load_dotenv(dotenv_path=env_path)

# API Configuration
API_KEY = os.getenv('API_KEY')
BASE_URL = os.getenv('BASE_URL')

# Validar variables API
if not API_KEY:
    raise ValueError("La variable API_KEY debe estar definida en el archivo .env")
if not BASE_URL:
    raise ValueError("La variable BASE_URL debe estar definida en el archivo .env")

HEADERS = {
    'Authorization': f'Bearer {API_KEY}',
    'Content-Type': 'application/json'
}

# Database Configuration
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

# Validar variables de la base de datos
required_db_vars = [DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME]
if not all(required_db_vars):
    missing = [var for var, value in zip(
        ['DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_PORT', 'DB_NAME'],
        required_db_vars
    ) if not value]
    raise ValueError(f"Las siguientes variables deben estar definidas en el archivo .env: {', '.join(missing)}")

DATABASE_URI = f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
