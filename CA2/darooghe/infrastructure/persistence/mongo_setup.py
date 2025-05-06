import logging
from typing import List

from pymongo import IndexModel

from darooghe.domain.util.logging import configure_cli_log
from darooghe.infrastructure.persistence.mongo import MongoDBClient
from darooghe.infrastructure.persistence.mongo_config import Mongo


class CollectionSetup:
    def __init__(self, mongo_client: MongoDBClient):
        self.client: MongoDBClient = mongo_client

    def setup_collections(self):
        logging.info("Starting setup collections...")
        collections = Mongo.Collections.get_all_collectinos()
        for collection in collections:
            self.__setup_collection(
                collection_name=collection.get_name(), indexes=collection.get_indexes()
            )
        logging.info("Collections were setup successfully")

    def __setup_collection(
        self, collection_name: str, indexes: List[IndexModel]
    ) -> None:
        self.client.create_collection(collection_name=collection_name)
        for index in indexes:
            self.client.create_index(collection_name=collection_name, index=index)


if __name__ == "__main__":
    configure_cli_log()
    with MongoDBClient(
        connection_string=Mongo.Config.MONGO_URI, db_name=Mongo.DB.DAROOGHE
    ) as mongo_client:
        collection_setup = CollectionSetup(mongo_client=mongo_client)
        collection_setup.setup_collections()
