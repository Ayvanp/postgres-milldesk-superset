# etl_script/api_client.py
import requests
import logging
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type
)
from requests.exceptions import RequestException, HTTPError, Timeout, ConnectionError

from etl_script.config import API_KEY, BASE_URL, HEADERS

logger = logging.getLogger(__name__)

@retry(
    retry=retry_if_exception_type((RequestException, HTTPError, Timeout, ConnectionError)),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    reraise=True
)
def make_request(url, params=None):
    try:
        response = requests.get(url, headers=HEADERS, params=params, timeout=60)
        response.raise_for_status()
        return response.json()
    except (HTTPError, Timeout, ConnectionError) as e:
        logger.warning(f"⚠️ Intento fallido para URL {url}: {e}. Reintentando...")
        raise
    except RequestException as e:
        logger.error(f"❗ Error de solicitud para URL {url}: {e}")
        raise

def fetch_data(endpoint, params=None):
    """
    Función genérica para obtener datos desde un endpoint específico.
    """
    url = f"{BASE_URL}{API_KEY}/{endpoint}"
    try:
        data = make_request(url, params=params)
        logger.info(f"✅ Datos obtenidos exitosamente desde el endpoint '{endpoint}'.")
        return data
    except Exception as e:
        logger.error(f"❌ No se pudieron obtener datos desde '{endpoint}': {e}")
        return []

# Funciones específicas ahora utilizan `fetch_data`
def get_ticket_status():
    return fetch_data("listTicketStatus")

def get_tickets_by_status(status):
    return fetch_data("showTicketsByStatus", params={'status': status})

def get_tickets_per_period(start_date, end_date):
    return fetch_data("showTicketsPerPeriod", params={'start': start_date, 'end': end_date})

def get_opened_closed_monthly():
    return fetch_data("openedVersusClosedMonthly")

def get_tickets_by_hour():
    return fetch_data("ticketsByOpeningTime")

def get_activities_hours_by_department():
    return fetch_data("activitiesHoursByDepartment")

def get_monthly_satisfaction_average():
    """
    Obtiene el promedio de satisfacción mensual desde la API de Milldesk.
    """
    return fetch_data("monthlySatisfactionAverage")

def get_ticket_activities():
    """
    Obtiene las actividades de tickets desde el endpoint listTicketsActivities.
    """
    return fetch_data("listTicketsActivities")

def get_activities_hours_to_charge():
    """
    Obtiene los datos de actividades con horas a cargar desde el endpoint activitiesHoursToCharge.
    """
    return fetch_data("activitiesHoursToCharge")
