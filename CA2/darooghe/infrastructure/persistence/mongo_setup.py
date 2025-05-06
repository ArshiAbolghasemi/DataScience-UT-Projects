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
        self.setup_daily_transaction_temproral_patterns()
        logging.info("All collections setup completed")

    def __setup_collection(
        self, collection_name: str, indexes: List[IndexModel]
    ) -> None:
        self.client.create_collection(collection_name=collection_name)
        for index in indexes:
            self.client.create_index(collection_name=collection_name, index=index)

    def __index_created_at_ttl(self):
        return IndexModel(
            [("created_at", ASCENDING)],
            expireAfterSeconds=Mongo.Config.MONGO_DB_TRANSACTION_DATA_TTL,
            name="created_at_ttl_idx",
        )

    def setup_transaction_collection(self):
        indexes = [self.__index_created_at_ttl()]
        self.__setup_collection(
            collection_name=Mongo.Collections.TRANSACTION, indexes=indexes
        )

    def setup_daily_transaction_temproral_patterns(self):
        indexes = [self.__index_created_at_ttl()]
        self.__setup_collection(
            collection_name=Mongo.Collections.TRANSACTION_TEMPORAL_PATTERNS,
            indexes=indexes,
        )


if __name__ == "__main__":
    with MongoDBClient(
        connection_string=Mongo.Config.MONGO_URI, db_name=Mongo.DB.DAROOGHE
    ) as mongo_client:
        collection_setup = CollectionSetup(mongo_client=mongo_client)
        collection_setup.setup_collections()
