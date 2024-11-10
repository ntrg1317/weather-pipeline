import logging
from cassandra.cluster import Cluster

def load_to_cassandra(data):
    cluster = Cluster(['your_cassandra_host'])
    session = cluster.connect('your_keyspace')
    for record in data:
        try:
            session.execute(
                """
                INSERT INTO weather_data (city, temperature, humidity, wind_speed, timestamp)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (record['city'], record['temperature'], record['humidity'], record['wind_speed'], record['timestamp'])
            )
        except Exception as e:
            logging.error(f"Error writing to Cassandra for city {record['city']}: {e}")
    cluster.shutdown()