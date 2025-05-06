from datetime import UTC, datetime
import logging
from typing import cast

from darooghe.domain.factory.transaction_factory import TransactionFactory
from darooghe.domain.util.logging import configure_cli_log
from darooghe.domain.util.serialization import Serializer
from darooghe.infrastructure.persistence.mongo import MongoDBClient
from darooghe.infrastructure.persistence.mongo_config import Mongo


class TransactionDataLoader:
    def __init__(self, mongo_client: MongoDBClient):
        self.__mongo_client: MongoDBClient = mongo_client
        self.__factory: TransactionFactory = TransactionFactory()

    def load_initial_data(
        self, num_transactions: int = 20000, batch_size: int = 100
    ) -> None:
        logging.info(f"Generating {num_transactions} historical transactions...")
        documents = []
        for _ in range(num_transactions):
            transaction = self.__factory.create_historical_transactions(
                count=1, days_back=14
            )[0]
            document = cast(Serializer, transaction).to_dict()
            document["created_at"] = datetime.now(UTC)
            document["timestamp"] = transaction.timestamp
            documents.append(document)

            if len(documents) >= batch_size:
                logging.info("Loading  batch transactions into __mongo_clientDB...")
                self.__mongo_client.insert_many(
                    Mongo.Collections.TRANSACTION, documents
                )
                documents.clear()

        logging.info("Initial transaction load completed")


def __main():
    configure_cli_log()
    with MongoDBClient(Mongo.Config.MONGO_URI, Mongo.DB.DAROOGHE) as mongo_client:
        transaction_data_loader = TransactionDataLoader(mongo_client)
        transaction_data_loader.load_initial_data()


if __name__ == "__main__":
    __main()
