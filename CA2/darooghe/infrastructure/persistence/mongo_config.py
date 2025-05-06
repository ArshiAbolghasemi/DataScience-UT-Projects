import os


class Mongo:

    class DB:
        DAROOGHE = "darooghe"

    class Config:
        MONGO_DB_TRANSACTION_DATA_TTL = int(
            os.getenv("MONGO_DB_TRANSACTION_DATA_TTL", 86400)
        )
        MONGO_URI = os.getenv("MONGO_URI")

    class Collections:
        TRANSACTION = "transaction"
        DAILY_TRANSACTION_TEMPORAL_PATTERNS = "daily_transaction_temporal_patterns"
