from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from darooghe.infrastructure.persistence.mongo_config import Mongo


class TransactionRepository:

    def __init__(self, spark) -> None:
        self.spark = spark

    def get_customer_average_amount(self) -> DataFrame:
        return (
            self.spark.read.format("mongodb")
            .option("database", Mongo.DB.DAROOGHE)
            .option("collection", Mongo.Collections.Transaction.get_name())
            .load()
            .groupBy("customer_id")
            .agg(F.avg("amount").alias("avg_amount"))
            .select(F.col("customer_id"), F.col("avg_amount"))
        )
