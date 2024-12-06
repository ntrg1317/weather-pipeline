import logging
import requests
import pandas as pd
from datetime import datetime

from .helpers import CassandraDatabase, ApiKeyManager

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("spark_structured_streaming")

DATA_FILE_PATH = '/opt/airflow/data/cities.csv'
API_KEYS = ['059768aecac5e0d1c2f39bf1adf42469',
           'd4679f6c6e151ddadc48561eb6cd0d92',
           'c27c2bc3eabb559ebdfec77fd9fc6944',
           '249cd2594bcf61dbf230f9dfad24c921',
           '24468a5617626f0e78a20b6138654f81',
           '85266108076d1d2145f20b3675cfc039',
           '5cfde48228127e5e10b26e7541572834',
           '6d7f069fe57ff2093a58a5e0eede275c',
           '997580b85e04d97d3b366c59134489ea']
RATE_LIMIT = 3000
KEYSPACE = "weather"
CONTACT_POINTS = ["127.0.0.1"]
USERNAME = "cassandra"
PASSWORD = "cassandra"
API_KEY_MANAGER = ApiKeyManager(API_KEYS, RATE_LIMIT)

# Get city latitude, longitude
def get_city_location(city):
    """
    Fetches the latitude and longitude of a city.
    :param city_id:
    :type city_id:
    :param cities:
    :type cities:
    :return:
    :rtype:
    """
    cities = pd.read_csv(DATA_FILE_PATH)
    if cities.empty:
        logging.info("No data available in the cities file.")
    city = cities[cities['city'] == city]
    if city.empty:
        logging.info(f"No city found with ID: {city}")
        return None
    return city.iloc[0]["latitude"], city.iloc[0]["longitude"]

def fetch_city_weather_data(city):
    api_key = API_KEY_MANAGER.get_api_key()
    lat, lon = get_city_location(city)
    url = f"http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}"
    try:
        response = requests.get(url)
        if response.status_code == 429:
            logging.warning(f"Rate limit hit for API key: {api_key}. Switching keys.")
            API_KEY_MANAGER.increment_usage(api_key)
            return fetch_city_weather_data(city)

        response.raise_for_status()
        data = response.json()

        weather = data.get('weather', [{}])
        main = weather[0].get('main') if weather else None
        description = weather[0].get('description') if weather else None
        return {
            'city': city,
            'time': int(data['dt']),
            'main': main,
            'description': description,
            'temp': data.get('main', {}).get('temp'),
            'feels_like': data.get('main', {}).get('feels_like'),
            'temp_min': data.get('main', {}).get('temp_min'),
            'temp_max': data.get('main', {}).get('temp_max'),
            'pressure': data.get('main', {}).get('pressure'),
            'humidity': data.get('main', {}).get('humidity'),
            'sea_level': data.get('main', {}).get('sea_level', None),
            'grnd_level': data.get('main', {}).get('grnd_level', None),
            'visibility': data.get('visibility', None),
            'wind': {
                'speed': data.get('wind', {}).get('speed', None),
                'deg': data.get('wind', {}).get('deg', None),
                'gust': data.get('wind', {}).get('gust', None),
            },
            'precipitation': {
                'rain': data.get('rain', 0),
                'snow': data.get('snow', 0),
            },
            'clouds': data.get('clouds', {}).get('all', None),
            'sunrise': data.get('sys', {}).get('sunrise', None),
            'sunset': data.get('sys', {}).get('sunset', None),
        }
    except Exception as e:
        logging.error(f"Error fetching weather data for {city_id}: {e}")
        return None

data = fetch_city_weather_data(1)
print(data)