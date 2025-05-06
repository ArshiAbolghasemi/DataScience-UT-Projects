import logging
from datetime import UTC, datetime, timedelta
from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from darooghe.domain.entity import customer
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
            collection=Mongo.Collections.TransactionTemporalPatterns.get_name(),
        )

        merchant_peaks = self._analyze_merchant_peaks(transactions_df)
        self._save_result(
            result=merchant_peaks, collection=Mongo.Collections.MerchantPeaks.get_name()
        )

        customer_segments = self._analyze_customer_segments(transactions_df)
        for collection, df in customer_segments.items():
            self._save_result(result=df, collection=collection)

    def _load_and_preprocess_data(
        self, start_date: datetime, end_date: datetime
    ) -> DataFrame:

        df = (
            self.spark.read.format("mongodb")
            .option("database", Mongo.DB.DAROOGHE)
            .option("collection", Mongo.Collections.Transaction.get_name())
            .load()
            .filter(
                (F.col("timestamp") >= start_date) & (F.col("timestamp") <= end_date)
            )
        )

        return df.withColumn("date", F.to_date("timestamp")).withColumn(
            "hour", F.hour("timestamp")
        )

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

    def _analyze_merchant_peaks(self, df: DataFrame) -> DataFrame:
        return (
            df.groupBy("merchant_category", "date", "hour")
            .agg(
                F.count("*").alias("transaction_count"),
                F.sum("amount").alias("total_amount"),
            )
            .withColumn("created_at", F.lit(datetime.now(UTC)))
            .orderBy("merchant_category", "date", F.desc("transaction_count"))
        )

    def _analyze_customer_segments(self, df: DataFrame) -> Dict[str, DataFrame]:
        customer_metrics = (
            df.groupBy("customer_id")
            .agg(
                F.count("*").alias("transaction_count"),
                F.sum("amount").alias("total_spend"),
                F.avg("amount").alias("avg_spend"),
                F.countDistinct("merchant_category").alias("category_variety"),
                F.datediff(F.max("date"), F.min("date")).alias("active_days"),
            )
            .withColumn(
                "segment",
                F.when(
                    (
                        F.col("total_spend")
                        >= customer.SEGMENT_RULES.HIGH_VALUE_MIN_SPEND
                    )
                    & (
                        F.col("transaction_count")
                        >= customer.SEGMENT_RULES.HIGH_VALUE_MIN_TRANSACTIONS
                    ),
                    customer.Segment.HIGH_VALUE,
                )
                .when(
                    F.col("transaction_count")
                    >= customer.SEGMENT_RULES.FREQUENT_MIN_TRANSACTIONS,
                    customer.Segment.FREQUENT,
                )
                .when(
                    F.col("active_days") <= customer.SEGMENT_RULES.NEW_MAX_DAYS,
                    customer.Segment.NEW,
                )
                .otherwise(customer.Segment.REGULAR),
            )
        )

        customer_segment_stats = customer_metrics.groupBy("segment").agg(
            F.count("*").alias("customer_count"),
            F.avg("transaction_count").alias("avg_transactions"),
            F.avg("total_spend").alias("avg_spend"),
            F.sum("total_spend").alias("total_spend"),
        )

        merchant_category_customer_segments = (
            df.join(customer_metrics, "customer_id")
            .groupBy("merchant_category", "segment")
            .agg(F.count("*").alias("transaction_count"))
            .orderBy("merchant_category", "segment")
        )

        return {
            Mongo.Collections.CustomerMetrics.get_name(): customer_metrics,
            Mongo.Collections.CustomerSegmentStats.get_name(): customer_segment_stats,
            Mongo.Collections.MerchantCategoryCustomerSegments.get_name(): merchant_category_customer_segments,
        }

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
