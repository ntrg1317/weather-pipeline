from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import timedelta
from tasks.helpers import ApiKeyManager, CassandraDatabase
import logging
import pandas as pd
import requests
import json

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("spark_structured_streaming")

DATA_FILE_PATH = '/opt/airflow/data/cities.csv'
SECURE_CONNECT_BUNDLE = '/opt/airflow/data/secure-connect-weather-cluster.zip'
CLUSTER_TOKEN = '/opt/airflow/data/weather_cluster-token.json'


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
    try:
        cities = pd.read_csv(DATA_FILE_PATH)
        if cities.empty:
            logging.info("No data available in the cities file.")
    except Exception as e:
        logging.error(f"Failed to load cities data: {e}")
    city = cities[cities['city'] == city]
    if city.empty:
        logging.info(f"No city found with ID: {city}")
        return None
    return city.iloc[0]["lat"], city.iloc[0]["lng"]

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
            "city": city,
            "datetime": data.get('dt') * 1000,
            "main": main,
            "description": description,
            "temp": data.get('main', {}).get('temp'),
            "feels_like": data.get('main', {}).get('feels_like'),
            "temp_min": data.get('main', {}).get('temp_min'),
            "temp_max": data.get('main', {}).get('temp_max'),
            "pressure": data.get('main', {}).get('pressure'),
            "humidity": data.get('main', {}).get('humidity'),
            "sea_level": data.get('main', {}).get('sea_level', None),
            "grnd_level": data.get('main', {}).get('grnd_level', None),
            "visibility": data.get('visibility', None),
            "wind": {
                "speed": data.get('wind', {}).get('speed', None),
                "deg": data.get('wind', {}).get('deg', None),
                "gust": data.get('wind', {}).get('gust', None),
            },
            "precipitation": {
                "rain": data.get('rain', {}).get('1h', 0),
                "snow": data.get('snow', {}).get('1h', 0),
            },
            "clouds": data.get('clouds', {}).get('all', None),
            "sunrise": data.get('sys', {}).get('sunrise', None) * 1000,
            "sunset": data.get('sys', {}).get('sunset', None) * 1000,
        }
    except Exception as e:
        logging.error(f"Error fetching weather data for {city}: {e}")
        return None

def fetch_weather_data():
    cities = pd.read_csv(DATA_FILE_PATH)
    with open(CLUSTER_TOKEN) as f:
        secrets = json.load(f)
    cassandra_db = CassandraDatabase(
        secure_connect_bundle=SECURE_CONNECT_BUNDLE,
        username=secrets["clientId"],
        password=secrets["secret"],
        keyspace="weather"
    )
    for city in cities['city'].unique():
        data = fetch_city_weather_data(city)
        import numpy as np
        def convert_int64_to_int(data):
            """
            Recursively converts all int64 values in the data to standard Python int.
            :param data: The data to be converted
            :return: The data with int64 values replaced by Python ints
            """
            if isinstance(data, dict):
                return {key: convert_int64_to_int(value) for key, value in data.items()}
            elif isinstance(data, list):
                return [convert_int64_to_int(item) for item in data]
            elif isinstance(data, np.int64):  # Check for int64 type
                return int(data)  # Convert to Python int
            else:
                return data
        data = convert_int64_to_int(data)
        cassandra_db.load_to_cassandra("realtime_weather", data)

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(seconds=60),
}

with DAG(
    f'weather_data_pipeline',
    default_args=default_args,
    description='Collect weather data for cities in Asia every 10 minutes',
    schedule_interval='*/10 * * * *',  # Chạy mỗi 10 phút
    start_date=datetime(2024, 10, 23),
    catchup=False,
) as dag:
    start = BashOperator(
        task_id='start',
        bash_command='echo "Starting ELT Pipeline"',
    )

    fetch_weather_data = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data,
        dag=dag,
    )


    start >> fetch_weather_data