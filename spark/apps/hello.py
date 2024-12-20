from docutils.nodes import table

from sparkCassandraConnector import SparkCassandraConnect
from pyspark.sql.functions import (
    col,
    date_trunc,
    avg,
    min,
    max,
    sum as spark_sum
)
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
    #         "spark.master":spark_master,
    #         "spark.jars.packages": "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1",
    #     },
def calculate_aggregates(df):
    """
    Calculate daily, monthly, and yearly aggregates
    """
    # Daily Aggregates
    daily_agg = (df
        .groupBy(
            col("city"),
            date_trunc("day", col("datetime")).alias("datetime")
        )
        .agg(
            avg("temp").alias("avg_temp"),
            min("temp_min").alias("temp_min"),
            max("temp_max").alias("temp_max"),
            avg("pressure").alias("avg_pressure")
        )
    )

    # Monthly Aggregates
    monthly_agg = (df
        .groupBy(
            col("city"),
            date_trunc("month", col("datetime")).alias("datetime")
        )
        .agg(
            avg("temp").alias("avg_temp"),
            min("temp_min").alias("temp_min"),
            max("temp_max").alias("temp_max"),
            avg("pressure").alias("avg_pressure")
        )
    )

    # Yearly Aggregates
    yearly_agg = (df
        .groupBy(
            col("city"),
            date_trunc("year", col("datetime")).alias("datetime")
        )
        .agg(
            avg("temp").alias("avg_temp"),
            min("temp_min").alias("temp_min"),
            max("temp_max").alias("temp_max"),
            avg("pressure").alias("avg_pressure")
        )
    )

    return daily_agg, monthly_agg, yearly_agg

if __name__ == "__main__":
    spark_conn = SparkCassandraConnect(
        app_name="AstraDB_Spark_Integration",
        master=config["SPARK"]["SPARK_MASTER"],
        secure_connect_bundle_file_path=config["ASTRA"]["SECURE_CONNECT_BUNDLE"],
        secure_connect_bundle_file="secure-connect-weather-cluster.zip",
        username=config["ASTRA"]["ASTRA_CLIENT_ID"],
        password=config["ASTRA"]["ASTRA_CLIENT_SECRET"]
    )

    try:
        realtime_weather_df = spark_conn.read_from_cassandra(keyspace="weather", table="realtime_weather")

        daily_aggregate, monthly_aggregate, yearly_aggregate = calculate_aggregates(realtime_weather_df)

        spark_conn.write_to_cassandra(dataframe=daily_aggregate, keyspace="weather", table="daily_aggregate")
        spark_conn.write_to_cassandra(dataframe=monthly_aggregate, keyspace="weather", table="monthly_aggregate")
        spark_conn.write_to_cassandra(dataframe=yearly_aggregate, keyspace="weather", table="yearly_aggregate")
    except Exception as e:
        print(e)