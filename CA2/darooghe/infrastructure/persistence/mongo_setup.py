import logging
from pymongo import IndexModel, ASCENDING
from typing import List

from darooghe.infrastructure.persistence.mongo import MongoDBClient
from darooghe.infrastructure.persistence.mongo_config import Mongo


class CollectionSetup:
    def __init__(self, mongo_client: MongoDBClient):
        self.client: MongoDBClient = mongo_client
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
        )

    def setup_collections(self):
        logging.info(f"Start to setup collections for db: {self.client.db}")
        self.setup_transaction_collection()
        logging.info("All collections setup completed")

    def __setup_collection(
        self, collection_name: str, indexes: List[IndexModel]
    ) -> None:
        self.client.create_collection(collection_name=collection_name)
        for index in indexes:
            self.client.create_index(collection_name=collection_name, index=index)

    def setup_transaction_collection(self):
        indexes = [
            IndexModel(
                [("timestamp", ASCENDING)],
                expireAfterSeconds=Mongo.Config.MONGO_DB_TRANSACTION_DATA_TTL,
                name="timestamp_ttl_idx",
            ),
        ]
        self.__setup_collection(
            collection_name=Mongo.Collections.TRANSACTION, indexes=indexes
        )


if __name__ == "__main__":
    with MongoDBClient(
        connection_string=Mongo.Config.MONGO_URI, db_name=Mongo.DB.DAROOGHE
    ) as mongo_client:
        collection_setup = CollectionSetup(mongo_client=mongo_client)
        collection_setup.setup_collections()
