from py4j.java_gateway import os


class Spark:

    class AppName:
        TRANSACTION_PATTERN_JOB = "TransactionPatternAnalysis"

    class Config:
        JARS_PACKAGES = os.getenv("SPARK_JARS_PACKAGES")
        SPARK_MASTER = os.getenv("SPARK_MASTER")
        DRIVER_HOST = os.getenv("SPARK_DRIVER_HOST")
        DRIVER_BIND_ADDRESS = os.getenv("SPARK_DRIVER_BIND_ADDRESS")
