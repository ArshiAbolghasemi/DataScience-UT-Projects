from typing import List

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from darooghe.application.repository.transaction_repository import TransactionRepository
from darooghe.domain.entity.fraud import Fraud


class FraudDetectorService:

    _FRAUD_TYPE_COL = "fraud_type"
    _RULE_DETAILS_COL = "rule_details"
    _METADATA_COL = "metadata"
    _CUSTOMER_ID_COL = "customer_id"

    def __init__(self, transaction_repo: TransactionRepository) -> None:
        self.transaction_repo = transaction_repo

    def execute(self, transaction_df: DataFrame) -> DataFrame:

        velocity_fraud = self._detect_velocity_fraud(transaction_df)
        geo_fraud = self._detect_geo_fraud(transaction_df)
        amount_anomaly_fraud = self._detect_amount_anomaly(transaction_df)

        return velocity_fraud.unionByName(geo_fraud).unionByName(amount_anomaly_fraud)

    def _get_fraud_alert_colums(self) -> List[Column]:
        return [
            F.col(self._CUSTOMER_ID_COL),
            F.col(self._FRAUD_TYPE_COL),
            F.col(self._RULE_DETAILS_COL),
            F.col(self._METADATA_COL),
        ]

    def _detect_velocity_fraud(self, transaction_df: DataFrame) -> DataFrame:
        return (
            transaction_df.withWatermark("timestamp", "10 minutes")
            .groupBy(
                F.window(F.col("timestamp"), "2 minutes", "20 seconds"),
                F.col("customer_id"),
            )
            .agg(F.count("*").alias("transaction_count"))
            .filter(
                F.col("transaction_count")
                > Fraud.Velocity.MAXIMUM_TRANSACTION_COUNT_PER_INTERVAL
            )
            .withColumn(self._FRAUD_TYPE_COL, F.lit(Fraud.Velocity.LABEL))
            .withColumn(self._RULE_DETAILS_COL, F.lit(Fraud.Velocity.DESCRIPTION))
            .withColumn(
                self._METADATA_COL,
                F.create_map(
                    F.lit("transaction_count"),
                    F.col("transaction_count").cast("string"),
                    F.lit("window_start"),
                    F.col("window.start").cast("string"),
                    F.lit("window_end"),
                    F.col("window.end").cast("string"),
                ),
            )
            .select(self._get_fraud_alert_colums())
        )

    def _detect_geo_fraud(self, transaction_df: DataFrame) -> DataFrame:
        windowed_transactions = (
            transaction_df.withWatermark("timestamp", "15 minutes")
            .groupBy(
                F.window(F.col("timestamp"), "5 minutes", "30 seconds"),
                F.col("customer_id"),
            )
            .agg(
                F.collect_list(
                    F.struct(
                        "transaction_id",
                        "timestamp",
                        "location.lat",
                        "location.lng",
                        "customer_id",
                    )
                ).alias("transactions"),
                F.count("*").alias("transaction_count"),
            )
            .filter(F.size("transactions") > 1)
        )

        exploded_transactions = (
            windowed_transactions.select(
                "customer_id",
                F.explode("transactions").alias("current"),
            )
            .join(
                windowed_transactions.select(
                    "customer_id",
                    F.explode("transactions").alias("previous"),
                ),
                "customer_id",
                "inner",
            )
            .filter(F.col("current.transaction_id") != F.col("previous.transaction_id"))
        )

        return (
            exploded_transactions.withColumn(
                "distance_km",
                6371
                * 2
                * F.asin(
                    F.sqrt(
                        F.pow(
                            F.sin(
                                (
                                    F.radians(F.col("current.lat"))
                                    - F.radians(F.col("previous.lat"))
                                )
                                / 2
                            ),
                            2,
                        )
                        + F.cos(F.radians(F.col("current.lat")))
                        * F.cos(F.radians(F.col("previous.lat")))
                        * F.pow(
                            F.sin(
                                (
                                    F.radians(F.col("current.lng"))
                                    - F.radians(F.col("previous.lng"))
                                )
                                / 2
                            ),
                            2,
                        )
                    )
                ),
            )
            .filter(F.col("distance_km") > Fraud.Geographical.MAX_DISTANCE_KM)
            .withColumn(self._FRAUD_TYPE_COL, F.lit(Fraud.Geographical.LABEL))
            .withColumn(self._RULE_DETAILS_COL, F.lit(Fraud.Geographical.DESCRIPTION))
            .withColumn(
                self._METADATA_COL,
                F.create_map(
                    F.lit("distance_km"),
                    F.col("distance_km"),
                    F.lit("current_transaction_id"),
                    F.col("current.transaction_id"),
                    F.lit("previous_transaction_id"),
                    F.col("previous.transaction_id"),
                ),
            )
            .select(self._get_fraud_alert_colums())
        )

    def _detect_amount_anomaly(self, transaction_df: DataFrame) -> DataFrame:
        customer_avg_amount = self.transaction_repo.get_customer_average_amount()

        windowed_transactions = (
            transaction_df.withWatermark("timestamp", "2 hours")
            .groupBy(
                F.window(F.col("timestamp"), "1 hour", "1 minutes"),
                F.col("customer_id"),
            )
            .agg(
                F.count("*").alias("transaction_count"),
                F.collect_list(
                    F.struct("transaction_id", "amount", "timestamp", "customer_id")
                ).alias("transactions"),
            )
            .select(F.explode("transactions").alias("transaction"))
            .select("transaction.*")
            .filter(
                F.col("transaction_count")
                > Fraud.AmountAnomaly.MINIMUM_TRANSACTION_COUNT
            )
        )

        return (
            windowed_transactions.join(
                F.broadcast(customer_avg_amount), "customer_id", "left"
            )
            .filter(
                (F.col("avg_amount").isNotNull())
                & (
                    (
                        F.col("amount")
                        > (
                            Fraud.AmountAnomaly.AMOUNT_ANOMALY_THRESHOLD_RATIO
                            * F.col("avg_amount")
                        )
                    )
                )
            )
            .withColumn(self._FRAUD_TYPE_COL, F.lit(Fraud.AmountAnomaly.LABEL))
            .withColumn(self._RULE_DETAILS_COL, F.lit(Fraud.AmountAnomaly.DESCRIPTION))
            .withColumn(
                self._METADATA_COL,
                F.create_map(
                    F.lit("transaction_amount"),
                    F.col("amount").cast("string"),
                    F.lit("transaction_avg_amount"),
                    F.col("avg_amount").cast("string"),
                    F.lit("timestamp"),
                    F.col("timestamp").cast("string"),
                ),
            )
            .select(self._get_fraud_alert_colums())
        )
