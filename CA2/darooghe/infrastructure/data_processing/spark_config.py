from py4j.java_gateway import os
from pyspark.sql import SparkSession

from darooghe.infrastructure.persistence.mongo_config import Mongo


class Spark:

    class AppName:
        TRANSACTION_PATTERN_JOB = "TransactionPatternAnalysis"
        COMMISSION_ANALYSIS = "commission_analysis"

    class Config:
        JARS_PACKAGES = os.getenv("SPARK_JARS_PACKAGES")
        SPARK_MASTER = os.getenv("SPARK_MASTER")
        DRIVER_HOST = os.getenv("SPARK_DRIVER_HOST")
        DRIVER_BIND_ADDRESS = os.getenv("SPARK_DRIVER_BIND_ADDRESS")

    @classmethod
    def create_mongo_session(cls, app_name: str) -> SparkSession:
        return (
            SparkSession.Builder()
            .appName(app_name)
            .config("spark.master", cls.Config.SPARK_MASTER)
            .config("spark.mongodb.read.connection.uri", Mongo.Config.MONGO_URI)
            .config("spark.mongodb.write.connection.uri", Mongo.Config.MONGO_URI)
            .config("spark.driver.host", cls.Config.DRIVER_HOST)
            .config("spark.driver.bindAddress", cls.Config.DRIVER_BIND_ADDRESS)
            .config("spark.jars.packages", cls.Config.JARS_PACKAGES)
            .getOrCreate()
        )
