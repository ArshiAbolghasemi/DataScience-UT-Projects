import os


class Kafka:

    class Config:
        KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "")

    class Topics:
        DAROOGHE_TRANSACTIONS = "darooghe.transactions"
        DAROOGHE_ERROR_LOGS = "darooghe.error_logs"
        DAROOGHE_FRAUD_ALERTS = "darooghe.fraud_alerts"

    class Groups:
        DAROOGHE_TRANSACTIONS_CONSUMER = "darooghe-transactions-consumer"
