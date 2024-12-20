from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import timedelta
from datetime import datetime
import logging, logging.config, configparser
import pandas as pd
import requests
import json
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from tasks.helpers import CassandraDatabase

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("WEATHER_DAG")

# Load project configuration
config = configparser.ConfigParser()
config.read("./config/project.cfg")

API_KEYS = json.loads(config["OPENWEATHERMAP"]["API_KEYS"])


def read_cities_csv(**context):
    """Read cities data from CSV and push to XCom"""
    try:
        cities = pd.read_csv(config["DATA"]["DATA_FILE_PATH"])
        if cities.empty:
            raise ValueError("No data available in the cities file.")

        # Convert cities DataFrame to list of dictionaries for XCom
        cities_data = cities[['city', 'lat', 'lng']].to_dict('records')
        context['task_instance'].xcom_push(key='cities_data', value=cities_data)
        logging.info(f"Successfully read {len(cities_data)} cities")
        return cities_data
    except Exception as e:
        logging.error(f"Failed to load cities data: {e}")
        raise


def kelvin_to_celsius(kelvin):
    return round(kelvin - 273.15, 2)


def fetch_city_weather_data(city: Dict) -> Dict:
    """
    Fetch weather data for a city using API and return it as a dictionary.
    """
    lat, lon = city['lat'], city['lng']
    city_name = city['city']

    def make_request(api_key):
        url = f"http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}"
        try:
            response = requests.get(url)
            if response.status_code == 429:
                logging.warning(f"Rate limit hit for API key: {api_key}")
                return None
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logging.error(f"Error fetching weather data for city {city_name} with API key {api_key}: {e}")
            return None

    for api_key in API_KEYS:
        data = make_request(api_key)
        if data:
            logging.info(f"Successfully fetched weather data for city: {city_name}")
            weather = data.get('weather', [{}])
            main = weather[0].get('main') if weather else None
            description = weather[0].get('description') if weather else None
            dt = datetime.utcfromtimestamp(data['dt'])
            sunrise = datetime.utcfromtimestamp(data['sys']['sunrise'])
            sunset = datetime.utcfromtimestamp(data['sys']['sunset'])
            return {
                "city": city_name,
                "datetime": str(dt),
                "main": main,
                "description": description,
                "temp": kelvin_to_celsius(data.get('main', {}).get('temp')),
                "feels_like": kelvin_to_celsius(data.get('main', {}).get('feels_like')),
                "temp_min": kelvin_to_celsius(data.get('main', {}).get('temp_min')),
                "temp_max": kelvin_to_celsius(data.get('main', {}).get('temp_max')),
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
                "sunrise": str(sunrise),
                "sunset": str(sunset),
            }
    logging.error(f"All API keys exhausted for city {city_name}. Skipping.")


def fetch_weather_data(**context):
    """
    Fetch weather data for all cities concurrently using ThreadPoolExecutor
    """
    cities = context['task_instance'].xcom_pull(task_ids='read_cities_csv', key='cities_data')
    weather_data = []

    def fetch_with_retry(city: Dict) -> Dict:
        """Helper function to fetch data for a single city with retry logic"""
        for attempt in range(3):  # Retry up to 3 times
            try:
                data = fetch_city_weather_data(city)
                if data:
                    logging.info(f"Successfully fetched data for {city['city']} on attempt {attempt + 1}")
                    return data
                logging.warning(f"No data returned for {city['city']} on attempt {attempt + 1}")
            except Exception as e:
                logging.error(f"Error fetching data for {city['city']} on attempt {attempt + 1}: {e}")
                if attempt == 2:  # Last attempt
                    return None
        return None

    # Use ThreadPoolExecutor for concurrent API calls
    with ThreadPoolExecutor(max_workers=len(API_KEYS)) as executor:
        # Submit all cities to the thread pool
        future_to_city = {
            executor.submit(fetch_with_retry, city): city
            for city in cities
        }

        # Process completed futures as they come in
        for future in as_completed(future_to_city):
            city = future_to_city[future]
            try:
                data = future.result()
                if data:
                    weather_data.append(data)
                    logging.info(f"Added data for {city['city']}, current total: {len(weather_data)}")
            except Exception as e:
                logging.error(f"Failed to process {city['city']}: {e}")

    logging.info(f"Completed fetching weather data for {len(weather_data)} out of {len(cities)} cities")

    # Push results to XCom
    context['task_instance'].xcom_push(key='weather_data', value=weather_data)
    return weather_data


def load_to_cassandra(**context):
    """Load weather data into Cassandra database"""
    weather_data = context['task_instance'].xcom_pull(task_ids='fetch_weather_data', key='weather_data')

    cassandra_db = CassandraDatabase(
        secure_connect_bundle=config["ASTRA"]["SECURE_CONNECT_BUNDLE"],
        username=config["ASTRA"]["ASTRA_CLIENT_ID"],
        password=config["ASTRA"]["ASTRA_CLIENT_SECRET"],
        keyspace="weather"
    )

    for data in weather_data:
        cassandra_db.load_to_cassandra("realtime_weather", data)

    logging.info(f"Successfully loaded {len(weather_data)} records to Cassandra")


default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
        'weather_pipeline',
        default_args=default_args,
        description='Collect weather data for cities in Asia every 10 minutes',
        schedule_interval='*/10 * * * *',
        start_date=datetime(2024, 10, 23),
        catchup=False
) as dag:
    start = BashOperator(
        task_id='start',
        bash_command='echo "Starting ELT Pipeline"',
        dag=dag
    )

    read_cities = PythonOperator(
        task_id='read_cities_csv',
        python_callable=read_cities_csv,
        dag=dag
    )

    fetch_weather = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data,
        dag=dag
    )

    load_cassandra = PythonOperator(
        task_id='load_to_cassandra',
        python_callable=load_to_cassandra,
        dag=dag
    )

    end = BashOperator(
        task_id='end',
        bash_command='echo "ELT Pipeline completed"',
        dag=dag
    )

    # Define task dependencies
    start >> read_cities >> fetch_weather >> load_cassandra >> end