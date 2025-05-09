from py4j.java_gateway import os


class Spark:

    class AppName:
        TRANSACTION_PATTERN_JOB = "darooghe_transaction_pattern_analysis"
        COMMISSION_ANALYSIS = "darooghe_commission_analysis"
        FRAUD_DETECTION_SYSTEM = "darooghe_fraud_detection_system"
        COMMISSION_ANALYTICS = "darooghe_commission_analytics"

    class Config:
        MONGO_JARS_PACKAGES = os.getenv("SPARK_MONGO_JARS_PACKAGES", "")
        KAFKA_JARS_PACKAGES = os.getenv("SPARK_KAFKA_JARS_PACKAGES", "")
        SPARK_JARS_PACAKGES = ",".join([MONGO_JARS_PACKAGES, KAFKA_JARS_PACKAGES])
        SPARK_MASTER = os.getenv("SPARK_MASTER")
        DRIVER_HOST = os.getenv("SPARK_DRIVER_HOST")
        DRIVER_BIND_ADDRESS = os.getenv("SPARK_DRIVER_BIND_ADDRESS")

        STREAMING_KAFKA_MAX_RATE_PER_PARTITION = os.getenv(
            "STREAMING_KAFKA_MAX_RATE_PER_PARTITION", 100
        )

        class CheckPoints:
            BASE_DIR = "/tmp/checkpoints"
            FRAUD_DETECTION_DIR = BASE_DIR + "/fraud_detection"
            COMMISSION_BY_TYPE_DIR = BASE_DIR + "/commission_by_type"
            COMMISSION_RATIO_BY_MERCHANT_CATEGORY = BASE_DIR + "/commission_ratio_by_merchant_category"
            TOP_MERCHANT_COMMISSION = BASE_DIR + "/top-merchant-commission"
