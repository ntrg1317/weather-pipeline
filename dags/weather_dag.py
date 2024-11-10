import multiprocessing

from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from tasks import fetch_weather_data
from tasks import load_to_cassandra
from airflow.utils.dates import timedelta

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    f'weather_data_pipeline',
    default_args=default_args,
    description='Collect weather data for cities in Asia every 10 minutes',
    schedule_interval='*/10 * * * *',  # Chạy mỗi 10 phút
    start_date=datetime(2024, 10, 23),
    catchup=True,
) as dag:
    fetch_cities_task = MySqlOperator(
        task_id='fetch_cities',
        sql= r"""SELECT name 
        FROM cities 
        JOIN countries ON cities.country_id = countries.id 
        JOIN regions ON countries.region_id = regions.id 
        WHERE region = 'Asia'""",
        mysql_conn_id='cities-db-connection',
        dag=dag,
    )


    def process_weather_data(api_keys, cities):
        with multiprocessing.Pool(len(api_keys)) as pool:
            results = []
            for city in cities:
                for api_key in api_keys:
                    result = pool.apply_async(fetch_weather_data, (city, api_key))
                    results.append(result)
            pool.close()
            pool.join()

        return [result.get() for result in results if result.get() is not None]


    fetch_weather_task = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=lambda: process_weather_data(
            ['059768aecac5e0d1c2f39bf1adf42469',
             'd4679f6c6e151ddadc48561eb6cd0d92',
             'c27c2bc3eabb559ebdfec77fd9fc6944',
             '249cd2594bcf61dbf230f9dfad24c921',
             '24468a5617626f0e78a20b6138654f81',
             '85266108076d1d2145f20b3675cfc039',
             '5cfde48228127e5e10b26e7541572834',
             '6d7f069fe57ff2093a58a5e0eede275c'], fetch_cities_task.output),
        dag=dag,
    )

    # Task Load: Lưu dữ liệu vào Cassandra
    store_data_task = PythonOperator(
        task_id='store_data_to_cassandra',
        python_callable=lambda: load_to_cassandra(fetch_weather_task.output),
        dag=dag,
    )

    fetch_cities_task >> fetch_weather_task >> store_data_task

