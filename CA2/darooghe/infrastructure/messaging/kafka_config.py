import os


class Kafka:

    class Config:
        KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "")

    class Topics:
        DAROOGHE_TRANSACTIONS = "darooghe.transactions"
        DAROOGHE_ERROR_LOGS = "darooghe.error_logs"
        DAROOGHE_FRAUD_ALERTS = "darooghe.fraud_alerts"
        DAROOGHE_COMMISSION_BY_TYPE = "darooghe.commission_by_type"
        DAROOGHE_COMMISSION_RATIO_BY_MERCHANT_CATEGORY = (
            "darooghe.commission_ratio_per_merchant_category"
        )
        DAROOGHE_TOP_MERHCANT_COMMISSION = "darooghe.top_merchant_commission"

    class Groups:
        DAROOGHE_TRANSACTIONS_CONSUMER = "darooghe-transactions-consumer"
