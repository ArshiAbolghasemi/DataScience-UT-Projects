import logging
from typing import cast

from darooghe.domain.factory.transaction_factory import TransactionFactory
from darooghe.domain.util.serialization import Serializer
from darooghe.infrastructure.persistence.mongo import MongoDBClient
from darooghe.infrastructure.persistence.mongo_config import Mongo


class TransactionDataLoader:
    def __init__(self, mongo_client: MongoDBClient):
        self.__mongo_client: MongoDBClient = mongo_client
        self.__factory: TransactionFactory = TransactionFactory()
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
        )

    def load_initial_data(
        self, num_transactions: int = 20000, batch_size: int = 100
    ) -> None:
        logging.info(f"Generating {num_transactions} historical transactions...")
        documents = []
        for _ in range(num_transactions):
            transaction = self.__factory.create_transaction()
            documents.append(cast(Serializer, transaction).to_dict())

            if len(documents) >= batch_size:
                logging.info("Loading  batch transactions into __mongo_clientDB...")
                self.__mongo_client.insert_many(
                    Mongo.Collections.TRANSACTION, documents
                )
                documents.clear()

        logging.info("Initial transaction load completed")


def __main():
    with MongoDBClient(Mongo.Config.MONGO_URI, Mongo.DB.DAROOGHE) as mongo_client:
        transaction_data_loader = TransactionDataLoader(mongo_client)
        transaction_data_loader.load_initial_data()


if __name__ == "__main__":
    __main()
