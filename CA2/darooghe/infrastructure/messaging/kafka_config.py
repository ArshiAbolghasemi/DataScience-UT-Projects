import os


class Kafka:

    class Config:
        KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

    class Topics:
        DAROOGHE_TRANSACTIONS = "darooghe.transactions"
        DAROOGHE_ERROR_LOGS = "darooghe.error_logs"

    class Groups:
        DAROOGHE_TRANSACTIONS_CONSUMER = "darooghe-transactions-consumer"
