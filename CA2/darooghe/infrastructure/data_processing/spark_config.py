from py4j.java_gateway import os


class Spark:

    class AppName:
        TRANSACTION_PATTERN_JOB = "TransactionPatternAnalysis"
        COMMISSION_ANALYSIS = "commission_analysis"

    class Config:
        MONGO_JARS_PACKAGES = os.getenv("SPARK_MONGO_JARS_PACKAGES")
        KAFKA_JARS_PACKAGES = os.getenv("SPARK_KAFKA_JARS_PACKAGES")
        SPARK_MASTER = os.getenv("SPARK_MASTER")
        DRIVER_HOST = os.getenv("SPARK_DRIVER_HOST")
        DRIVER_BIND_ADDRESS = os.getenv("SPARK_DRIVER_BIND_ADDRESS")

        STREAMING_KAFKA_MAX_RATE_PER_PARTITION = os.getenv(
            "STREAMING_KAFKA_MAX_RATE_PER_PARTITION", 100
        )
        SPARK_SQL_SUFFLE_PARTITION = os.getenv("SPARK_SQL_SUFFLE_PARTITION", 4)

        class CheckPoints:
            FRAUD_DETECTION_DIR = ""
