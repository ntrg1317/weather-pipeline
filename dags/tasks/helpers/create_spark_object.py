import os
from typing import Optional, Dict, Any, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType


class SparkCassandraConnector:
    """
    A comprehensive utility class for connecting and interacting with Cassandra using PySpark
    """

    def __init__(
            self,
            app_name: str = "CassandraSparkApp",
            master: str = "local[*]",
            secure_connect_bundle: Optional[str] = None,
            username: Optional[str] = None,
            password: Optional[str] = None
    ):
        """
        Initialize Spark Session with Cassandra connection

        :param app_name: Name of the Spark application
        :param master: Spark master URL
        :param username: Cassandra username
        :param password: Cassandra password
        :param secure_connect_bundle: Path to Astra secure connect bundle
        """
        # Spark session configuration
        self.spark_builder = (
            SparkSession.builder
            .appName(app_name)
            .master(master)
            .config("spark.jars.packages",
                    "com.datastax.spark:spark-cassandra-connector_2.12-3.0.1.jar")
        )

        # Cassandra connection configurations
        # if cassandra_host:
        #     self.spark_builder = (
        #         self.spark_builder
        #         .config("spark.cassandra.connection.host", cassandra_host)
        #         .config("spark.cassandra.connection.port", cassandra_port)
        #     )

        # Authentication configurations
        if username and password:
            self.spark_builder = (
                self.spark_builder
                .config("spark.cassandra.auth.username", username)
                .config("spark.cassandra.auth.password", password)
            )

        # Secure connect bundle for Astra DB
        if secure_connect_bundle and os.path.exists(secure_connect_bundle):
            self.spark_builder = (
                self.spark_builder
                .config("spark.cassandra.connection.config.cloud.path", secure_connect_bundle)
            )

        # Create Spark session
        self.spark = self.spark_builder.getOrCreate()

    def read_from_cassandra(
            self,
            keyspace: str,
            table: str,
            columns: Optional[List[str]] = None,
            where_clause: Optional[str] = None
    ) -> DataFrame:
        """
        Read data from Cassandra table

        :param keyspace: Cassandra keyspace name
        :param table: Cassandra table name
        :param columns: List of columns to read (optional)
        :param where_clause: Optional WHERE clause for filtering
        :return: Spark DataFrame
        """
        options = {
            "keyspace": keyspace,
            "table": table
        }

        # Select specific columns if provided
        df_reader = self.spark.read.format("org.apache.spark.sql.cassandra")

        if columns:
            # Read only specified columns
            selected_df = df_reader.options(**options).load()
            return selected_df.select(*columns)

        # Read entire table
        return df_reader.options(**options).load()

    def write_to_cassandra(
            self,
            dataframe: DataFrame,
            keyspace: str,
            table: str,
            mode: str = "append"
    ) -> None:
        """
        Write Spark DataFrame to Cassandra

        :param dataframe: Spark DataFrame to write
        :param keyspace: Target Cassandra keyspace
        :param table: Target Cassandra table
        :param mode: Write mode (append/overwrite/ignore)
        """
        (dataframe.write
         .format("org.apache.spark.sql.cassandra")
         .mode(mode)
         .options(keyspace=keyspace, table=table)
         .save())

    def execute_cassandra_query(
            self,
            query: str,
            params: Optional[Dict[str, Any]] = None
    ) -> DataFrame:
        """
        Execute a CQL-like query using Spark

        :param query: SQL-like query
        :param params: Optional query parameters
        :return: Spark DataFrame with query results
        """
        return self.spark.sql(query)

    def create_cassandra_table(
            self,
            keyspace: str,
            table: str,
            schema: StructType
    ) -> None:
        """
        Create a new Cassandra table from Spark DataFrame schema

        :param keyspace: Target keyspace
        :param table: Table name
        :param schema: Spark DataFrame schema
        """
        # Convert Spark schema to CREATE TABLE statement
        schema_ddl = ", ".join([
            f"{field.name} {field.dataType.simpleString()}"
            for field in schema.fields
        ])

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.{table} (
            {schema_ddl}
        )
        """

        # Note: This is a simplified approach and might need adjustments
        self.spark.sql(create_table_query)

    def close(self):
        """
        Close the Spark session
        """
        self.spark.stop()