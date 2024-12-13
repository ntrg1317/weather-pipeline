from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import timedelta
from tasks.helpers import CassandraDatabase
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging, logging.config, configparser
import pandas as pd
import requests
import json

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("WEATHER_DAG")

# Load project configuration
config = configparser.ConfigParser()
config.read("./config/project.cfg")

API_KEYS = json.loads(config["OPENWEATHERMAP"]["API_KEYS"])

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
        cities = pd.read_csv(config["DATA"]["DATA_FILE_PATH"])
        if cities.empty:
            logging.info("No data available in the cities file.")
    except Exception as e:
        logging.error(f"Failed to load cities data: {e}")
    city = cities[cities['city'] == city]
    if city.empty:
        logging.info(f"No city found with ID: {city}")
        return None
    return city.iloc[0]["lat"], city.iloc[0]["lng"]

def kelvin_to_celsius(kelvin):
    return round(kelvin - 273.15, 2)

def fetch_city_weather_data(city):
    """
    Fetch weather data for a city using API and return it as a dictionary.
    """
    lat, lon = get_city_location(city)
    if not lat or not lon:
        logging.warning(f"Skipping city {city} due to missing location data.")
        return None

    def make_request(api_key):
        url = f"http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}"
        try:
            response = requests.get(url)
            if response.status_code == 429:
                # Rate limit hit for this API key, return None to trigger key rotation
                logging.warning(f"Rate limit hit for API key: {api_key}")
                return None
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logging.error(f"Error fetching weather data for city {city} with API key {api_key}: {e}")
            return None

    for api_key in API_KEYS:
        data = make_request(api_key)
        if data:
            logging.info(f"Successfully fetched weather data for city: {city} using API key: {api_key}")
            weather = data.get('weather', [{}])
            main = weather[0].get('main') if weather else None
            description = weather[0].get('description') if weather else None
            dt = datetime.utcfromtimestamp(data['dt'])
            sunrise = datetime.utcfromtimestamp(data['sys']['sunrise'])
            sunset = datetime.utcfromtimestamp(data['sys']['sunset'])
            return {
                "city": city,
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
    logging.error(f"All API keys exhausted for city {city}. Skipping.")
    return None

def fetch_weather_data():
    cities = pd.read_csv(config["DATA"]["DATA_FILE_PATH"])

    cassandra_db = CassandraDatabase(
        secure_connect_bundle=config["ASTRA"]["SECURE_CONNECT_BUNDLE"],
        username=config["ASTRA"]["ASTRA_CLIENT_ID"],
        password=config["ASTRA"]["ASTRA_CLIENT_SECRET"],
        keyspace="weather"
    )

    def fetch_and_store(city):
        data = fetch_city_weather_data(city)
        if data:
            cassandra_db.load_to_cassandra("realtime_weather", data)
            logging.info(f"Data stored for city: {city}")

    # Fetch data concurrently for all cities
    with ThreadPoolExecutor(max_workers=len(API_KEYS)) as executor:
        future_to_city = {executor.submit(fetch_and_store, city): city for city in cities['city'].unique()}
        for future in as_completed(future_to_city):
            city = future_to_city[future]
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error processing city {city}: {e}")

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    f'weather_data_pipeline',
    default_args=default_args,
    description='Collect weather data for cities in Asia every 10 minutes',
    schedule_interval='*/10 * * * *',  # Chạy mỗi 10 phút
    start_date=datetime(2024, 10, 23),
    catchup=False
) as dag:
    start = BashOperator(
        task_id='start',
        bash_command='echo "Starting ELT Pipeline"',
    )

    fetch_weather_data = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data,
        dag=dag
    )

    # process_data = PythonOperator(
    #     task_id='process_weather_data',
    #     python_callable=process_weather_data,
    #     dag=dag
    # )

    # test_spark = SparkSubmitOperator(
    #     task_id="spark_job",
    #     application="/opt/spark/apps/hello.py",
    #     name=spark_app_name,
    #     conn_id="spark_default",
    #     verbose=1,
    #     conf={
    #         "spark.master":spark_master,
    #         "spark.jars.packages": "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1",
    #     },
    #     dag=dag
    # )

    start >> fetch_weather_data