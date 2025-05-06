import logging
from datetime import UTC, datetime, timedelta
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from darooghe.domain.util.logging import configure_cli_log
from darooghe.infrastructure.batch_processing.spark_config import Spark
from darooghe.infrastructure.persistence.mongo_config import Mongo


class TransactionPatternJob:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def run(self, end_date: Optional[datetime] = None, lookback_days: int = 90):
        end_date = end_date or datetime.now()
        start_date = end_date - timedelta(days=lookback_days)

        transactions_df = self._load_and_preprocess_data(start_date, end_date)

        transaction_temporal_patterns = self._analyze_transction_temporal_patterns(
            transactions_df
        )
        self._save_result(
            result=transaction_temporal_patterns,
            collection=Mongo.Collections.TRANSACTION_TEMPORAL_PATTERNS,
        )

    def _load_and_preprocess_data(
        self, start_date: datetime, end_date: datetime
    ) -> DataFrame:

        df = (
            self.spark.read.format("mongodb")
            .option("database", Mongo.DB.DAROOGHE)
            .option("collection", Mongo.Collections.TRANSACTION)
            .load()
            .filter(
                (F.col("timestamp") >= start_date) & (F.col("timestamp") <= end_date)
            )
        )

        return df.withColumn("date", F.to_date("timestamp"))

    def _analyze_transction_temporal_patterns(self, df: DataFrame) -> DataFrame:
        return (
            df.groupBy("date")
            .agg(
                F.count("*").alias("transaction_count"),
                F.sum("amount").alias("total_amount"),
            )
            .withColumn("created_at", F.lit(datetime.now(UTC)))
            .orderBy("date")
        )

    def _save_result(self, result: DataFrame, collection: str):
        (
            result.write.format("mongodb")
            .mode("append")
            .option("database", Mongo.DB.DAROOGHE)
            .option("collection", collection)
            .save()
        )


def _create_spark_session() -> SparkSession:
    return (
        SparkSession.Builder()
        .appName(Spark.AppName.TRANSACTION_PATTERN_JOB)
        .config("spark.master", Spark.Config.SPARK_MASTER)
        .config("spark.mongodb.read.connection.uri", Mongo.Config.MONGO_URI)
        .config("spark.mongodb.write.connection.uri", Mongo.Config.MONGO_URI)
        .config("spark.driver.host", Spark.Config.DRIVER_HOST)
        .config("spark.driver.bindAddress", Spark.Config.DRIVER_BIND_ADDRESS)
        .config("spark.jars.packages", Spark.Config.JARS_PACKAGES)
        .getOrCreate()
    )


def _main():
    configure_cli_log()
    spark = None
    try:
        logging.info("Starting Transaction Pattern Analysis Job")
        spark = _create_spark_session()
        job = TransactionPatternJob(spark)
        job.run()
        logging.info("Job completed successfully")
    except Exception as e:
        logging.error(f"Job failed with error: {str(e)}", exc_info=True)
    except KeyboardInterrupt:
        logging.info("Received keyboard interrupt, stopping transaction pattern job")
    finally:
        if spark:
            spark.stop()
            logging.info("Spark session stopped")


if __name__ == "__main__":
    _main()
