# etl_script/transformations.py
import pandas as pd
import logging

logger = logging.getLogger(__name__)

# ---- Validaciones genéricas ----
def validate_data(data, expected_keys):
    if not isinstance(data, list):
        logger.error("❌ Los datos recibidos no son una lista.")
        return False
    for item in data:
        if not all(key in item for key in expected_keys):
            logger.error(f"❌ Faltan claves esperadas en el dato: {item}")
            return False
    return True

def validate_activities_hours_data(data):
    expected_keys = ['department', 'worked_hour', 'charge_hour']
    return validate_data(data, expected_keys)

# ---- Transformaciones comunes ----
def convert_date_columns(df, date_columns, date_format='%d/%m/%Y'):
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format=date_format, errors='coerce')
        else:
            df[col] = pd.NaT
    return df

def convert_timestamp_columns(df, timestamp_columns, timestamp_format='%d/%m/%Y %H:%M'):
    for col in timestamp_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format=timestamp_format, errors='coerce')
        else:
            df[col] = pd.NaT
    return df

def convert_numeric_columns(df, numeric_columns):
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        else:
            df[col] = pd.NA
    return df

# ---- Transformaciones específicas ----
def transform_generic(data, expected_keys, rename_mapping=None, date_columns=None, timestamp_columns=None, numeric_columns=None):
    if not validate_data(data, expected_keys):
        logger.error(f"❌ Datos inválidos para transformación.")
        return pd.DataFrame()
    
    df = pd.DataFrame(data)
    
    if rename_mapping:
        df = df.rename(columns=rename_mapping)
    
    if date_columns:
        df = convert_date_columns(df, date_columns)
    
    if timestamp_columns:
        df = convert_timestamp_columns(df, timestamp_columns)
    
    if numeric_columns:
        df = convert_numeric_columns(df, numeric_columns)
    
    return df

def transform_ticket_status(data):
    expected_keys = ['status', 'description', 'action']
    return transform_generic(
        data,
        expected_keys=expected_keys
    )

def transform_tickets(data):
    if not data:
        logger.warning("⚠️ No hay datos de tickets para transformar.")
        return pd.DataFrame()
    
    # Renombrar 'end' a 'end_date'
    for item in data:
        if 'end' in item:
            item['end_date'] = item.pop('end')
    
    expected_keys = ['id', 'start', 'end_date', 'charge_hour', 'worked_hour', 'analysis', 'reopening', 'starttime', 'endtime', 'analysistime']
    return transform_generic(
        data,
        expected_keys=expected_keys,
        date_columns=['start', 'end_date', 'analysis', 'reopening'],
        timestamp_columns=['starttime', 'endtime', 'analysistime'],
        numeric_columns=['charge_hour', 'worked_hour']
    )

def transform_tickets_per_period(data):
    if not data:
        return pd.DataFrame()
    
    # Renombrar 'end' a 'end_date'
    for item in data:
        if 'end' in item:
            item['end_date'] = item.pop('end')
    
    essential_keys = ['id', 'start', 'end_date', 'charge_hour', 'worked_hour']
    if not validate_data(data, essential_keys):
        logger.error("❌ Datos de tickets por período inválidos (faltan claves esenciales).")
        return pd.DataFrame()
    
    return transform_generic(
        data,
        expected_keys=essential_keys + ['analysis', 'reopening', 'starttime', 'endtime', 'analysistime'],
        date_columns=['start', 'end_date', 'analysis', 'reopening'],
        timestamp_columns=['starttime', 'endtime', 'analysistime'],
        numeric_columns=['charge_hour', 'worked_hour']
    )

def transform_opened_closed_monthly(data):
    expected_keys = ['month', 'year', 'opened', 'closed']
    if not validate_data(data, expected_keys):
        logger.error("❌ Datos abiertos vs cerrados mensuales inválidos.")
        return pd.DataFrame()
    
    df = pd.DataFrame(data)
    df = convert_numeric_columns(df, ['month', 'year', 'opened', 'closed'])
    return df

def transform_tickets_by_hour(data):
    expected_keys = ['hour', 'amount', 'percentage']
    if not validate_data(data, expected_keys):
        logger.error("❌ Datos de tickets por hora inválidos.")
        return pd.DataFrame()
    
    df = pd.DataFrame(data)
    df = convert_numeric_columns(df, ['hour', 'amount', 'percentage'])
    return df

def transform_activities_hours_by_department(data):
    if not validate_activities_hours_data(data):
        logger.error("❌ Datos de actividades por departamento inválidos.")
        return pd.DataFrame()
    
    df = pd.DataFrame(data)

    # Convertir HH:MM a minutos
    def time_to_minutes(time_str):
        try:
            hours, minutes = map(int, time_str.split(':'))
            return hours * 60 + minutes
        except:
            return None
    
    df['worked_minutes'] = df['worked_hour'].apply(time_to_minutes)
    df['charge_minutes'] = df['charge_hour'].apply(time_to_minutes)

    df = df.rename(columns={
        'department': 'department',
        'worked_minutes': 'worked_minutes',
        'charge_minutes': 'charge_minutes'
    })

    # Seleccionar solo las columnas necesarias
    df = df[['department', 'worked_minutes', 'charge_minutes']]

    return df

def transform_monthly_satisfaction_average(data):
    """
    Transforma los datos de satisfacción mensual en un DataFrame de Pandas.
    """
    expected_keys = ['month', 'year', 'month_year', 'evaluation']
    
    if not validate_data(data, expected_keys):
        logger.error("❌ Datos de satisfacción mensual inválidos.")
        return pd.DataFrame()
    
    df = pd.DataFrame(data)
    df = convert_numeric_columns(df, ['month', 'year', 'evaluation'])
    return df

# ---- NUEVA TRANSFORMACIÓN PARA TICKET ACTIVITIES ----
def transform_ticket_activities(data):
    """
    Transforma los datos obtenidos desde el endpoint listTicketsActivities a un DataFrame.
    
    Estructura de ejemplo esperada por item:
    {
      "activity": "Notebook",
      "description": "Se configura notebook según indicaciones",
      "id": "2547",
      "ticket": "Configuración notebook",
      "agent": "Marco Fernandez",
      "typeofactivity": "Presencial",
      "start": "20/04/2018",
      "end": "20/04/2018",
      "charge_hour": "03:00",
      "worked_hour": "03:00",
      "parts": null,
      "id_ticket": "2052"
    }
    """
    # Si data es un dict único, lo convertimos a lista para que DataFrame lo procese
    if isinstance(data, dict):
        data = [data]

    expected_keys = [
        "activity", "description", "id", "ticket", "agent",
        "typeofactivity", "start", "end", "charge_hour", "worked_hour",
        "parts", "id_ticket"
    ]
    if not validate_data(data, expected_keys):
        logger.error("❌ Datos de ticket activities inválidos.")
        return pd.DataFrame()

    df = pd.DataFrame(data)

    # Renombrar 'id' a 'activity_id', 'start' a 'start_date' y 'end' a 'end_date'
    df.rename(columns={'id': 'activity_id', 'start': 'start_date', 'end': 'end_date'}, inplace=True)

    # Convertir columnas de fecha (dd/mm/yyyy)
    df = convert_date_columns(df, ['start_date', 'end_date'], date_format='%d/%m/%Y')

    # Convertir columnas de hora (HH:MM) a tipo time, y calcular minutos
    def parse_time(time_str):
        try:
            return pd.to_datetime(time_str, format='%H:%M').time()
        except:
            return None
    
    df['charge_hour'] = df['charge_hour'].apply(parse_time)
    df['worked_hour'] = df['worked_hour'].apply(parse_time)

    df['charge_minutes'] = df['charge_hour'].apply(lambda t: t.hour * 60 + t.minute if t else None)
    df['worked_minutes'] = df['worked_hour'].apply(lambda t: t.hour * 60 + t.minute if t else None)

    return df

# etl_script/transformations.py

def transform_activities_hours_to_charge(data):
    """
    Transforma los datos obtenidos desde el endpoint activitiesHoursToCharge a un DataFrame.
    """
    # 1) Validar que sea una lista (o convertir dict a lista)
    if isinstance(data, dict):
        data = [data]

    # 2) Definir las claves esperadas
    expected_keys = [
        "id_ticket", "location_id", "ticket", "activity", "description",
        "start", "end", "parts", "start_time", "end_time", "contract", "agent",
        "location", "typeofactivity", "requester", "cost", "charge_hour"
    ]

    # 3) Validar que existan esas claves en cada item
    if not validate_data(data, expected_keys):
        logger.error("❌ Datos de 'activitiesHoursToCharge' inválidos.")
        return pd.DataFrame()

    # 4) Pasar a DataFrame
    df = pd.DataFrame(data)

    # 5) Renombrar columnas 'start' y 'end' -> 'start_date', 'end_date' 
    df.rename(columns={
        'start': 'start_date',
        'end': 'end_date'
    }, inplace=True)

    # 6) Convertir columnas de fecha (formato dd/mm/yyyy)
    df = convert_date_columns(df, ['start_date', 'end_date'], date_format='%d/%m/%Y')

    # 7) Parsear 'start_time' y 'end_time' como hora (HH:MM)
    def parse_time(time_str):
        try:
            return pd.to_datetime(time_str, format='%H:%M').time()
        except:
            return None

    df['start_time'] = df['start_time'].apply(parse_time)
    df['end_time'] = df['end_time'].apply(parse_time)

    # 8) Convertir 'cost' a numérico
    df['cost'] = pd.to_numeric(df['cost'], errors='coerce')

    # 9) Convertir 'charge_hour' (HH:MM) y calcular 'charge_minutes'
    def time_to_minutes(time_str):
        if not time_str:
            return None
        try:
            hh, mm = map(int, time_str.split(':'))
            return hh * 60 + mm
        except:
            return None

    df['charge_minutes'] = df['charge_hour'].apply(time_to_minutes)

    # 10) Retornar DataFrame con las columnas relevantes
    return df
