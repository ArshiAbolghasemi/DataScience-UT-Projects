import logging
from datetime import UTC, datetime, timedelta
from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from darooghe.domain.entity import customer
from darooghe.domain.util.logging import configure_cli_log
from darooghe.domain.util.time import TimeOfDay
from darooghe.infrastructure.data_processing.spark_config import Spark
from darooghe.infrastructure.data_processing.spark_session_manager import (
    create_mongo_session,
)
from darooghe.infrastructure.persistence.mongo_config import Mongo


class TransactionPatternJob:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def run(self, end_date: Optional[datetime] = None, lookback_days: int = 90):
        end_date = end_date or datetime.now(UTC)
        start_date = end_date - timedelta(days=lookback_days)

        transactions_df = self._load_transaction_data(start_date, end_date)

        results = (
            {}
            | self._analyze_transactions_per_time_of_day(transactions_df)
            | self._analyze_merchant_peaks(transactions_df)
            | self._analyze_customer_segments(transactions_df)
            | self._compare_merchant_categories(transactions_df)
            | self._analyze_transactions_per_time_of_day(transactions_df)
            | self._analyze_spend_trend(transactions_df)
        )

        for collection, result in results.items():
            self._save_result(result=result, collection=collection)

    def _load_transaction_data(
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

        return (
            df.withColumn("date", F.to_date("timestamp"))
            .withColumn("hour", F.hour("timestamp"))
            .withColumn(
                "time_of_day",
                F.when(
                    (F.col("hour") >= TimeOfDay.MORNING.start)
                    & (F.col("hour") <= TimeOfDay.MORNING.end),
                    TimeOfDay.MORNING.label,
                )
                .when(
                    (F.col("hour") >= TimeOfDay.AFTERNOON.start)
                    & (F.col("hour") <= TimeOfDay.AFTERNOON.end),
                    TimeOfDay.AFTERNOON.label,
                )
                .when(
                    (F.col("hour") >= TimeOfDay.EVENING.start)
                    & (F.col("hour") <= TimeOfDay.EVENING.end),
                    TimeOfDay.EVENING.label,
                )
                .otherwise(TimeOfDay.NIGHT.label),
            )
        )

    def _save_result(self, result: DataFrame, collection: str):
        (
            result.write.format("mongodb")
            .mode("append")
            .option("database", Mongo.DB.DAROOGHE)
            .option("collection", collection)
            .save()
        )

    def _analyze_transction_temporal_patterns(
        self, df: DataFrame
    ) -> Dict[str, DataFrame]:
        return {
            Mongo.Collections.TransactionTemporalPatterns.get_name(): (
                df.groupBy("date")
                .agg(
                    F.count("*").alias("transaction_count"),
                    F.sum("amount").alias("total_amount"),
                )
                .withColumn("created_at", F.lit(datetime.now(UTC)))
                .orderBy("date")
            )
        }

    def _analyze_merchant_peaks(self, df: DataFrame) -> Dict[str, DataFrame]:
        return {
            Mongo.Collections.MerchantPeaks.get_name(): (
                df.groupBy("merchant_category", "date", "hour")
                .agg(
                    F.count("*").alias("transaction_count"),
                    F.sum("amount").alias("total_amount"),
                )
                .withColumn("created_at", F.lit(datetime.now(UTC)))
                .orderBy("merchant_category", "date", F.desc("transaction_count"))
            )
        }

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
            .withColumn("created_at", F.lit(datetime.now(UTC)))
        )

        customer_segment_stats = (
            customer_metrics.groupBy("segment")
            .agg(
                F.count("*").alias("customer_count"),
                F.avg("transaction_count").alias("avg_transactions"),
                F.avg("total_spend").alias("avg_spend"),
                F.sum("total_spend").alias("total_spend"),
            )
            .withColumn("created_at", F.lit(datetime.now(UTC)))
        )

        merchant_category_customer_segments = (
            df.join(customer_metrics, "customer_id")
            .groupBy("merchant_category", "segment")
            .agg(F.count("*").alias("transaction_count"))
            .orderBy("merchant_category", "segment")
        ).withColumn("created_at", F.lit(datetime.now(UTC)))

        return {
            Mongo.Collections.CustomerMetrics.get_name(): customer_metrics,
            Mongo.Collections.CustomerSegmentStats.get_name(): customer_segment_stats,
            Mongo.Collections.MerchantCategoryCustomerSegments.get_name(): merchant_category_customer_segments,
        }

    def _compare_merchant_categories(self, df: DataFrame) -> Dict[str, DataFrame]:
        merchant_category_stats = (
            df.groupBy("merchant_category")
            .agg(
                F.count("*").alias("transaction_count"),
                F.sum("amount").alias("total_amount"),
                F.avg("amount").alias("avg_amount"),
                F.sum("commission_amount").alias("total_commission"),
                (F.sum("commission_amount") / F.sum("amount")).alias("commission_rate"),
                F.countDistinct("customer_id").alias("unique_customers"),
                F.expr("percentile(amount, 0.5)").alias("median_amount"),
            )
            .withColumn("created_at", F.lit(datetime.now(UTC)))
            .orderBy(F.desc("total_amount"))
        )

        merchant_category_time_dist = (
            df.groupBy("merchant_category", "time_of_day")
            .agg(
                F.count("*").alias("transaction_count"),
                F.sum("amount").alias("total_amount"),
            )
            .withColumn("created_at", F.lit(datetime.now(UTC)))
            .orderBy("merchant_category", "time_of_day")
        )

        return {
            Mongo.Collections.MerchantCategoryStats.get_name(): merchant_category_stats,
            Mongo.Collections.MerchantCategoryTimeDist.get_name(): merchant_category_time_dist,
        }

    def _analyze_transactions_per_time_of_day(
        self, df: DataFrame
    ) -> Dict[str, DataFrame]:
        return {
            Mongo.Collections.TransactionPerTimeOfDay.get_name(): (
                df.groupBy("time_of_day")
                .agg(
                    F.count("*").alias("transaction_count"),
                    F.sum("amount").alias("total_amount"),
                    F.avg("amount").alias("avg_amount"),
                    F.countDistinct("customer_id").alias("unique_customers"),
                )
                .withColumn("created_at", F.lit(datetime.now(UTC)))
                .orderBy(F.desc("transaction_count"))
            )
        }

    def _analyze_spend_trend(self, df: DataFrame) -> Dict[str, DataFrame]:
        now = datetime.now(UTC)
        return {
            Mongo.Collections.WeeklySpendingTrend.get_name(): (
                df.filter(
                    (F.col("timestamp") >= now - timedelta(days=365))
                    & (F.col("timestamp") <= now)
                )
                .withColumn("week", F.weekofyear("timestamp"))
                .alias("week")
                .groupBy("week")
                .agg(
                    F.count("*").alias("transaction_count"),
                    F.sum("amount").alias("total_amount"),
                    F.avg("amount").alias("avg_amount"),
                )
                .withColumn("created_at", F.lit(datetime.now(UTC)))
                .orderBy("week")
            )
        }


def _main():
    configure_cli_log()
    spark = None
    try:
        logging.info("Starting Transaction Pattern Analysis Job")
        spark = create_mongo_session(Spark.AppName.TRANSACTION_PATTERN_JOB)
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
