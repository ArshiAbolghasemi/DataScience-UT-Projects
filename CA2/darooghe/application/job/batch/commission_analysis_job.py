import logging
from datetime import UTC, datetime, timedelta
from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from darooghe.domain.entity.commission import CommissionModel
from darooghe.domain.util.logging import configure_cli_log
from darooghe.domain.util.time import JAVA_DATE_FORMAT_YEAR_MONTH
from darooghe.infrastructure.data_processing.spark_config import Spark
from darooghe.infrastructure.persistence.mongo_config import Mongo


class CommissionAnalysisJob:

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def run(self, end_date: Optional[datetime] = None, lookback_days: int = 90) -> None:
        end_date = end_date or datetime.now(UTC)
        start_date = end_date - timedelta(days=lookback_days)
        transaction_df = self._load_transaction_data(start_date, end_date)

        results = (
            {}
            | self._analyze_commission_per_merchant_category(transaction_df)
            | self._analyze_commission_trends(transaction_df)
            | self._simulate_commission_models(transaction_df)
        )

        for collection, result in results.items():
            self._save_result(result=result, collection=collection)

    def _load_transaction_data(
        self, start_date: datetime, end_date: datetime
    ) -> DataFrame:
        return (
            self.spark.read.format("mongodb")
            .option("database", Mongo.DB.DAROOGHE)
            .option("collection", Mongo.Collections.Transaction.get_name())
            .load()
            .filter(
                (F.col("timestamp") >= start_date) & (F.col("timestamp") <= end_date)
            )
            .select(
                "transaction_id",
                "merchant_id",
                "merchant_category",
                "amount",
                "commission_amount",
                "timestamp",
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

    def _analyze_commission_per_merchant_category(
        self, df: DataFrame
    ) -> Dict[str, DataFrame]:
        return {
            Mongo.Collections.CommissionPerMerchantCategroy.get_name(): (
                df.groupBy("merchant_category")
                .agg(
                    F.sum("amount").alias("total_amount"),
                    F.sum("commission_amount").alias("total_commission"),
                    F.avg("commission_amount").alias("avg_commission"),
                    (F.sum("commission_amount") / F.sum("amount")).alias(
                        "commission_ratio"
                    ),
                    F.countDistinct("merchant_id").alias("unique_merchants"),
                )
                .withColumn("created_at", F.lit(datetime.now(UTC)))
            )
        }

    def _analyze_commission_trends(self, df: DataFrame) -> Dict[str, DataFrame]:
        return {
            Mongo.Collections.CommissionTrends.get_name(): (
                df.withColumn(
                    "month", F.date_format("timestamp", JAVA_DATE_FORMAT_YEAR_MONTH)
                )
                .groupBy("merchant_category", "month")
                .agg(
                    F.sum("amount").alias("total_amount"),
                    F.sum("commission_amount").alias("total_commission"),
                    (F.sum("commission_amount") / F.sum("amount")).alias(
                        "commission_ratio"
                    ),
                )
                .withColumn("created_at", F.lit(datetime.now(UTC)))
                .orderBy("month")
            )
        }

    def _simulate_commission_models(self, df: DataFrame) -> Dict[str, DataFrame]:
        current_metrics = df.groupBy("merchant_category").agg(
            F.sum("amount").alias("current_total_amount"),
            F.sum("commission_amount").alias("current_total_commission"),
        )

        flat_rate = current_metrics.withColumn(
            "flat_rate_projection_commission",
            F.col("current_total_amount") * CommissionModel.Flat.RATE,
        )

        tiered_model = current_metrics.withColumn(
            "tiered_projection_commission",
            F.when(
                F.col("current_total_amount")
                <= CommissionModel.Tiered.Tier1.MAX_AMOUNT_THRESHOLD,
                F.col("current_total_amount") * CommissionModel.Tiered.Tier1.RATE,
            )
            .when(
                F.col("current_total_amount")
                <= CommissionModel.Tiered.Tier2.MAX_AMOUNT_THRESHOLD,
                CommissionModel.Tiered.Tier2.BASE_COMMISSION
                + (
                    F.col("current_total_amount")
                    - CommissionModel.Tiered.Tier1.MAX_AMOUNT_THRESHOLD
                )
                * CommissionModel.Tiered.Tier2.RATE,
            )
            .otherwise(
                CommissionModel.Tiered.Tier3.BASE_COMMISSION
                + (
                    F.col("current_total_amount")
                    - CommissionModel.Tiered.Tier2.MAX_AMOUNT_THRESHOLD
                )
                * CommissionModel.Tiered.Tier3.RATE
            ),
        )

        volume_based = current_metrics.withColumn(
            "volume_based_projection_commission",
            F.col("current_total_amount")
            * F.when(
                F.col("current_total_amount")
                < CommissionModel.Volume.Small.MAX_AMOUNT_THRESHOLD,
                CommissionModel.Volume.Small.RATE,
            )
            .when(
                F.col("current_total_amount")
                < CommissionModel.Volume.Medium.MAX_AMOUNT_THRESHOLD,
                CommissionModel.Volume.Medium.RATE,
            )
            .otherwise(CommissionModel.Volume.Large.RATE),
        )

        simulations = (
            current_metrics.join(
                flat_rate.select(
                    "merchant_category", "flat_rate_projection_commission"
                ),
                "merchant_category",
            )
            .join(
                tiered_model.select(
                    "merchant_category", "tiered_projection_commission"
                ),
                "merchant_category",
            )
            .join(
                volume_based.select(
                    "merchant_category", "volume_based_projection_commission"
                ),
                "merchant_category",
            )
            .withColumn("created_at", F.lit(datetime.now(UTC)))
        )

        return {Mongo.Collections.CommissionModelSimulations.get_name(): simulations}


def _main():
    configure_cli_log()
    spark = None
    try:
        logging.info("Starting Commission Analysis Job")
        spark = Spark.create_mongo_session(Spark.AppName.COMMISSION_ANALYSIS)
        job = CommissionAnalysisJob(spark)
        job.run()
        logging.info("Job completed successfully")
    except Exception as e:
        logging.error(f"job failed with error: {str(e)}", exc_info=True)
    except KeyboardInterrupt:
        logging.info("Received keyboard interrupt, stopping transaction pattern job")
    finally:
        if spark:
            spark.stop()
            logging.info("Spark session stopped")


if __name__ == "__main__":
    _main()
