import json

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
import logging
from time import sleep
from pyspark.sql import SparkSession

class CassandraDatabase:
    def __init__(self, secure_connect_bundle, username=None, password=None, keyspace=None, max_retries=5, retry_delay=60):
        """
        Initialize the CassandraDatabase class.
        :param hosts: List of Cassandra nodes (e.g., ['cassandra'])
        :param username: Cassandra username
        :param password: Cassandra password
        :param keyspace: Default keyspace to use
        :param max_retries: Maximum number of retries for connecting
        :param retry_delay: Delay (in seconds) between retries
        """
        self.secure_connect_bundle = secure_connect_bundle
        self.username = username
        self.password = password
        self.keyspace = keyspace
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.cluster = None
        self.session = None
        self.connect()
    # def __init__(self, hosts, username="cassandra", password="cassandra", keyspace=None, max_retries=1):
    #     """
    #     Initialize the CassandraDatabase class.
    #     :param hosts: List of Cassandra nodes (e.g., ['cassandra'])
    #     :param username: Cassandra username
    #     :param password: Cassandra password
    #     :param keyspace: Default keyspace to use
    #     :param max_retries: Maximum number of retries for connecting
    #     :param retry_delay: Delay (in seconds) between retries
    #     """
    #     self.hosts= hosts
    #     self.username = username
    #     self.password = password
    #     self.keyspace = keyspace
    #     self.max_retries = max_retries
    #     # self.retry_delay = retry_delay
    #     self.cluster = None
    #     self.session = None
    #     self.connect()

    def connect(self):
        """
        Connect to the Cassandra cluster with retries.
        """
        auth_provider = PlainTextAuthProvider(self.username, self.password)
        retries = 0
        while retries < self.max_retries:
            try:
                cloud_config = {
                    'secure_connect_bundle': self.secure_connect_bundle,
                }
                self.cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
                # self.cluster = Cluster(self.hosts, auth_provider=auth_provider)
                self.session = self.cluster.connect()
                if self.keyspace:
                    self.session.set_keyspace(self.keyspace)
                logging.info("Connected to Cassandra successfully.")
                break
            except Exception as e:
                logging.error(f"Failed to connect to Cassandra: {e}")
                retries += 1
                if retries >= self.max_retries:
                    raise Exception("Exceeded maximum retries to connect to Cassandra")
                sleep(self.retry_delay)

    def execute_query(self, query, parameters=None):
        """
        Execute a Cassandra query.
        :param query: CQL query string
        :param parameters: Parameters for the query (optional)
        :return: Result set
        """
        try:
            statement = SimpleStatement(query)
            result = self.session.execute(statement, parameters)
            logging.info(f"Query executed successfully: {query}")
            return result
        except Exception as e:
            logging.error(f"Failed to execute query: {query}, Error: {e}")
            raise

    def load_to_cassandra(self, table, data):
        """
        Insert data into a Cassandra table.
        :param table: Target table name
        :param data: List of dictionaries representing rows to insert
        """
        if not data:
            logging.info("No data to insert.")
            return
        try:
            data = json.dumps(data)
            query = f"INSERT INTO {table} JSON '{data}';"
            self.session.execute(query)
            logging.info(f"Data inserted successfully into {table}.")
        except Exception as e:
            logging.error(f"Failed to insert data into {table}: {e}")
            raise

    def read_table_to_spark(self, table, spark_master="local[*]", spark_app_name="CassandraApp"):
        """
        Load a Cassandra table into a Spark DataFrame.
        :param table: Table name
        :param spark_master: Spark master URL (default: local[*])
        :param spark_app_name: Spark application name
        :return: Spark DataFrame
        """
        try:
            spark = SparkSession.builder \
                .appName(spark_app_name) \
                .master(spark_master) \
                .config("spark.cassandra.connection.host", ','.join(self.hosts)) \
                .config("spark.cassandra.auth.username", self.username) \
                .config("spark.cassandra.auth.password", self.password) \
                .getOrCreate()
            df = spark.read \
                .format("org.apache.spark.sql.cassandra") \
                .options(table=table, keyspace=self.keyspace) \
                .load()
            logging.info(f"Loaded table {table} into Spark DataFrame.")
            return df
        except Exception as e:
            logging.error(f"Failed to load table {table} into Spark DataFrame: {e}")
            raise

    def close(self):
        """
        Close the Cassandra connection.
        """
        if self.cluster:
            self.cluster.shutdown()
            logging.info("Cassandra cluster connection closed.")
