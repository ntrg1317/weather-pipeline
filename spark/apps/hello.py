from sparkCassandraConnector import SparkCassandraConnect
from pyspark.sql.functions import col
import configparser, logging.config

#Load project configuration
config = configparser.ConfigParser()
config.read("./config/project.cfg")

# Initialize Spark Session

# spark = SparkSession.builder \
#     .appName("AstraDB_Spark_Integration") \
#     .config("spark.files", SECURE_CONNECT_BUNDLE) \
#     .config("spark.cassandra.connection.config.cloud.path", "secure-connect-weather-cluster.zip") \
#     .config("spark.cassandra.auth.username", ASTRA_CLIENT_ID) \
#     .config("spark.cassandra.auth.password", ASTRA_CLIENT_SECRET) \
#     .getOrCreate()

spart_conn = SparkCassandraConnect(
    app_name="AstraDB_Spark_Integration",
    master=config["SPARK"]["SPARK_MASTER"],
    secure_connect_bundle_file_path=config["ASTRA"]["SECURE_CONNECT_BUNDLE"],
    secure_connect_bundle_file="secure-connect-weather-cluster.zip",
    username=config["ASTRA"]["ASTRA_CLIENT_ID"],
    password=config["ASTRA"]["ASTRA_CLIENT_SECRET"]
)

def process_city_weather_data(city:str):
    realtime_weather_df = (spart_conn.read_from_cassandra(config["ASTRA"]["ASTRA_KEYSPACE"], "realtime_weather")
                           .filter(col("city") == city))

    realtime_weather_df.select(realtime_weather_df.datetime).