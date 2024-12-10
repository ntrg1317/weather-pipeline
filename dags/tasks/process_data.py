from typing import List, Dict, Any
from datetime import datetime, timedelta
from .helpers import SparkCassandraConnector
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, sum, avg, min, max,
    window, count, current_timestamp
)

from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType,
    DoubleType, IntegerType
)

class WeatherDataProcessor(SparkCassandraConnector):
    def __init__(
            self,
            app_name: str = "WeatherDataAggregation",
            master: str = "local[*]",
            secure_connect_bundle = None,
            username = None,
            password = None
    ):
        """
        Initialize WeatherDataProcessor with Cassandra connection

        :param app_name: Name of the Spark application
        :param master: Spark master URL
        :param cassandra_port: Cassandra port
        :param username: Cassandra username
        :param password: Cassandra password
        :param secure_connect_bundle: Path to secure connect bundle
        """
        super().__init__(
            app_name=app_name,
            master=master,
            secure_connect_bundle=secure_connect_bundle,
            username=username,
            password=password
        )

    def read_realtime_weather(self, city: str) -> DataFrame:
        """
        Read realtime weather data for a specific city

        :param city: City name to filter data
        :return: Spark DataFrame with realtime weather data
        """
        return (
            self.read_from_cassandra(
                keyspace="weather",
                table="realtime_weather"
            )
            .filter(col("city") == city)
        )

    def aggregate_daily_precipitation(
            self,
            city: str,
            start_date: datetime,
            end_date: datetime
    ) -> DataFrame:
        """
        Aggregate daily precipitation for a city

        :param city: City name
        :param start_date: Start of aggregation period
        :param end_date: End of aggregation period
        :return: Spark DataFrame with daily precipitation
        """
        realtime_df = self.read_realtime_weather(city)

        daily_precipitation = (
            realtime_df
            .filter(
                (col("datetime") >= start_date) &
                (col("datetime") < end_date)
            )
            .groupBy(
                col("city"),
                window(col("datetime"), "1 day")
            )
            .agg(
                sum(col("precipitation")).alias("sum_precipitation")
            )
            .select(
                col("city"),
                col("window.start").alias("datetime"),
                col("sum_precipitation")
            )
        )

        return daily_precipitation

    def aggregate_daily_weather(
            self,
            city: str,
            start_date: datetime,
            end_date: datetime
    ) -> DataFrame:
        """
        Aggregate daily weather statistics

        :param city: City name
        :param start_date: Start of aggregation period
        :param end_date: End of aggregation period
        :return: Spark DataFrame with daily weather aggregates
        """
        realtime_df = self.read_realtime_weather(city)

        daily_aggregate = (
            realtime_df
            .filter(
                (col("datetime") >= start_date) &
                (col("datetime") < end_date)
            )
            .groupBy(
                col("city"),
                window(col("datetime"), "1 day")
            )
            .agg(
                avg(col("pressure")).alias("avg_pressure"),
                avg(col("temp")).alias("avg_temp"),
                min(col("temp")).alias("temp_min"),
                max(col("temp")).alias("temp_max")
            )
            .select(
                col("city"),
                col("window.start").alias("datetime"),
                col("avg_pressure"),
                col("avg_temp"),
                col("temp_min"),
                col("temp_max")
            )
        )

        return daily_aggregate

    def process_daily_aggregates(
            self,
            city: str,
            process_date: datetime = None
    ):
        """
        Process and write daily aggregates for a city

        :param city: City to process
        :param process_date: Date to process (defaults to current date)
        """
        if process_date is None:
            process_date = datetime.now()

        start_date = process_date.replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        end_date = start_date + timedelta(days=1)

        # Process precipitation aggregate
        precipitation_df = self.aggregate_daily_precipitation(
            city, start_date, end_date
        )
        self.write_to_cassandra(
            precipitation_df,
            "weather",
            "daily_precipitation_aggregate"
        )

        # Process daily weather aggregate
        daily_weather_df = self.aggregate_daily_weather(
            city, start_date, end_date
        )
        self.write_to_cassandra(
            daily_weather_df,
            "weather",
            "daily_aggregate"
        )