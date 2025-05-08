from pyspark.sql import SparkSession

from darooghe.infrastructure.data_processing.spark_config import Spark
from darooghe.infrastructure.persistence.mongo_config import Mongo


def create_mongo_session(app_name: str) -> SparkSession:
    return (
        SparkSession.Builder()
        .appName(app_name)
        .config("spark.master", Spark.Config.SPARK_MASTER)
        .config("spark.mongodb.read.connection.uri", Mongo.Config.MONGO_URI)
        .config("spark.mongodb.write.connection.uri", Mongo.Config.MONGO_URI)
        .config("spark.driver.host", Spark.Config.DRIVER_HOST)
        .config("spark.driver.bindAddress", Spark.Config.DRIVER_BIND_ADDRESS)
        .config("spark.jars.packages", Spark.Config.JARS_PACKAGES)
        .getOrCreate()
    )
