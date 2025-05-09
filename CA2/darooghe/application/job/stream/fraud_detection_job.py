from py4j.java_gateway import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from darooghe.application.repository.transaction_repository import TransactionRepository
from darooghe.application.service.fraud_detector import FraudDetectorService
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


class FraudDetectionJob:

    def __init__(
        self, spark: SparkSession, fraud_detection_service: FraudDetectorService
    ) -> None:
        self.spark = spark
        self.fraud_detection_service = fraud_detection_service

    def run(self) -> None:
        logging.info("Starting Kafka stream for topic")
        transactions = self._load_transaction_streams()

        logging.info("Starting fraud detection process...")
        frauds = self.fraud_detection_service.execute(transactions)

        logging.info(
            f"Detected frauds will be written to Kafka topic: {Kafka.Topics.DAROOGHE_FRAUD_ALERTS}"
        )
        self._write_frauds_to_kafka_topic(frauds)

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

    def _write_frauds_to_kafka_topic(self, frauds: DataFrame) -> None:
        query = write_stream_to_kafks(
            df=frauds,
            kafka_brokers=[Kafka.Config.KAFKA_BROKER],
            kafka_topic=Kafka.Topics.DAROOGHE_FRAUD_ALERTS,
            checkpoint_location=Spark.Config.CheckPoints.FRAUD_DETECTION_DIR,
        )
        query.awaitTermination()


def _main():
    configure_cli_log()
    spark = None
    try:
        logging.info("Starting Fraud Detection Job...")
        spark = create_spark_session(Spark.AppName.FRAUD_DETECTION_SYSTEM)
        transaction_repo = TransactionRepository(spark)
        fraud_detection_service = FraudDetectorService(transaction_repo)
        logging.info("Spark session for Fraud Detection Job created successfully")
        job = FraudDetectionJob(spark, fraud_detection_service)
        job.run()
        logging.info("Fraud Detection Job completed successfully")
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
