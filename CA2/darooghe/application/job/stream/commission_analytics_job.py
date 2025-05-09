from typing import cast

from py4j.java_gateway import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from darooghe.application.service.commission_analytics import (
    CommissionAnalysisType,
    CommissionAnalytics,
)
from darooghe.domain.util.logging import configure_cli_log
from darooghe.infrastructure.data_processing import schemas
from darooghe.infrastructure.data_processing.spark_config import Spark
from darooghe.infrastructure.data_processing.spark_session_manager import (
    create_spark_session,
)
from darooghe.infrastructure.data_processing.stream import (
    read_from_kafka_stream,
    write_stream_to_kafks,
)
from darooghe.infrastructure.messaging.kafka_config import Kafka


class CommissionAnalyticsJob:

    def __init__(
        self, spark: SparkSession, commission_analytics: CommissionAnalytics
    ) -> None:
        self.spark = spark
        self.commission_analytics = commission_analytics

    def run(self) -> None:
        logging.info("Starting Kafka stream for topic")
        transactions = self._load_transaction_streams()

        logging.info("Starting commission analytics process...")
        analysis = self.commission_analytics.execute(transactions)

        logging.info(
            f"Commission analysis results will be written to the Kafka topic.: {Kafka.Topics.DAROOGHE_FRAUD_ALERTS}"
        )
        self._write_frauds_to_kafka_topic(analysis)

    def _load_transaction_streams(self) -> DataFrame:
        stream_df = read_from_kafka_stream(
            spark=self.spark,
            kafka_brokers=[Kafka.Config.KAFKA_BROKER],
            kafka_topic=Kafka.Topics.DAROOGHE_TRANSACTIONS,
        )

        return stream_df.select(
            F.from_json(
                F.col("value").cast("string"), schemas.transaction_schema
            ).alias("data")
        ).select(
            "data.*",
        )

    def _write_frauds_to_kafka_topic(self, analysis: CommissionAnalysisType) -> None:
        for analyse in analysis:
            df = cast(DataFrame, analyse[CommissionAnalytics.KEY_DF])
            topic = str(analyse[CommissionAnalytics.KEY_TOPIC])
            checkpoint_dir = str(analyse[CommissionAnalytics.KEY_CHECKPOINT_DIR])
            write_stream_to_kafks(
                df=df,
                kafka_brokers=[Kafka.Config.KAFKA_BROKER],
                kafka_topic=topic,
                checkpoint_location=checkpoint_dir,
            )

        self.spark.streams.awaitAnyTermination()


def _main():
    configure_cli_log()
    spark = None
    try:
        logging.info("Starting Commission Analytics Job...")
        spark = create_spark_session(Spark.AppName.COMMISSION_ANALYTICS)
        commission_analytics = CommissionAnalytics()
        job = CommissionAnalyticsJob(spark, commission_analytics)
        logging.info("Spark session for Commission Analytics Job created successfully")
        job.run()
        logging.info("Commission Analytics Job completed successfully")
    except Exception as e:
        logging.error(f"Job failed with error: {str(e)}", exc_info=True)
    except KeyboardInterrupt:
        logging.info("Received keyboard interrupt, stopping transaction pattern job")
    finally:
        if spark:
            spark.stop()
            logging.info("Spark sessoion stopped")


if __name__ == "__main__":
    _main()
