from typing import Dict, List, Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from darooghe.domain.entity import commission
from darooghe.infrastructure.data_processing.spark_config import Spark
from darooghe.infrastructure.messaging.kafka_config import Kafka

_CommissionAnalyseType = Dict[str, Union[str, DataFrame]]
CommissionAnalysisType = List[_CommissionAnalyseType]


class CommissionAnalytics:

    KEY_DF = "df"
    KEY_TOPIC = "topic"
    KEY_CHECKPOINT_DIR = "cpdir"

    def execute(self, transactions_df: DataFrame) -> CommissionAnalysisType:
        return [
            {
                self.KEY_TOPIC: Kafka.Topics.DAROOGHE_COMMISSION_BY_TYPE,
                self.KEY_CHECKPOINT_DIR: Spark.Config.CheckPoints.COMMISSION_BY_TYPE_DIR,
                self.KEY_DF: self._calculate_commission_by_type(transactions_df),
            },
            {
                self.KEY_TOPIC: Kafka.Topics.DAROOGHE_COMMISSION_RATIO_BY_MERCHANT_CATEGORY,
                self.KEY_CHECKPOINT_DIR: Spark.Config.CheckPoints.COMMISSION_RATIO_BY_MERCHANT_CATEGORY,
                self.KEY_DF: self._calculate_commission_ratio_by_merchant_category(
                    transactions_df
                ),
            },
            {
                self.KEY_TOPIC: Kafka.Topics.DAROOGHE_TOP_MERHCANT_COMMISSION,
                self.KEY_CHECKPOINT_DIR: Spark.Config.CheckPoints.TOP_MERCHANT_COMMISSION,
                self.KEY_DF: self._calculate_top_merchant_commission(transactions_df),
            },
        ]

    def _calculate_commission_by_type(self, transactions_df: DataFrame) -> DataFrame:
        return (
            transactions_df.withWatermark("timestamp", "5 minutes")
            .groupBy(F.window("timestamp", "1 minute"), "commission_type")
            .agg(F.sum("commission_amount").alias("total_commission"))
            .select(
                F.col("window.start").alias("window_start"),
                F.col("commission_type"),
                F.col("total_commission"),
            )
        )

    def _calculate_commission_ratio_by_merchant_category(
        self, transactions_df: DataFrame
    ) -> DataFrame:
        return (
            transactions_df.withWatermark("timestamp", "15 minutes")
            .groupBy(
                F.window("timestamp", "5 minutes", "1 minute"), "merchant_category"
            )
            .agg(
                (F.sum("commission_amount") / F.sum("amount")).alias(
                    "commission_ratio"
                ),
                F.count("*").alias("transaction_count"),
            )
            .filter(
                F.col("transaction_count")
                > commission.STATISTICAL_SIGNIFANCE_COMMISSION_RATION_PER_MERCHANT_CATEGORY_TRANSACTION_COUNT
            )
            .select(
                F.col("window.start").alias("window_start"),
                F.col("merchant_category"),
                F.col("commission_ratio"),
                F.col("transaction_count"),
            )
        )

    def _calculate_top_merchant_commission(self, df: DataFrame) -> DataFrame:
        windowed = (
            df.withWatermark("timestamp", "10 minutes")
            .groupBy(F.window("timestamp", "5 minutes"), "merchant_category")
            .agg(F.sum("commission_amount").alias("total_commission"))
        )

        return (
            windowed.groupBy("window")
            .agg(
                F.max_by(F.col("merchant_category"), F.col("total_commission")).alias(
                    "top_merchant"
                ),
                F.max("total_commission").alias("max_commission"),
            )
            .select(
                F.col("window.start").alias("window_start"),
                "top_merchant",
                "max_commission",
            )
        )
