import logging
from typing import Any, Dict, List

from pymongo import IndexModel, MongoClient
from pymongo.errors import PyMongoError


class MongoDBClient:

    def __init__(self, connection_string: str, db_name: str, max_pool_size: int = 100):
        try:
            self.client = MongoClient(
                connection_string,
                maxPoolSize=max_pool_size,
                connectTimeoutMS=5000,
                serverSelectionTimeoutMS=5000,
            )
            self.db = self.client[db_name]
            self.client.admin.command("ping")
            logging.basicConfig(
                level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
            )
            logging.info("Successfully connected to MongoDB")
        except PyMongoError as e:
            logging.error(f"Failed to connect to MongoDB: {e}")
            raise

    def insert_one(self, collection_name: str, document: Dict[str, Any]) -> str:
        try:
            collection = self.db[collection_name]
            result = collection.insert_one(document)
            return str(result.inserted_id)
        except PyMongoError as e:
            logging.error(f"Error inserting document: {e}")
            raise

    def insert_many(
        self, collection_name: str, documents: List[Dict[str, Any]]
    ) -> List[str]:
        try:
            if not documents:
                return []

            collection = self.db[collection_name]
            result = collection.insert_many(documents)
            return [str(id) for id in result.inserted_ids]
        except PyMongoError as e:
            logging.error(f"Error inserting documents: {e}")
            raise

    def create_index(self, collection_name: str, index: IndexModel) -> str:
        try:
            collection = self.db[collection_name]
            return collection.create_indexes([index])[0]
        except PyMongoError as e:
            logging.error(f"Error creating index: {e}")
            raise

    def create_collection(
        self,
        collection_name: str,
    ) -> None:
        try:
            if collection_name not in self.db.list_collection_names():
                self.db.create_collection(collection_name)
        except PyMongoError as e:
            logging.error(f"Error creating partitioned collection: {e}")
            raise

    def close(self) -> None:
        try:
            self.client.close()
            logging.info("MongoDB connection closed")
        except PyMongoError as e:
            logging.error(f"Error closing connection: {e}")
            raise

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        if exc_type is not None:
            logging.error(
                f"Exception in context: {exc_type.__name__}: {exc_val}",
                exc_info=(exc_type, exc_val, exc_tb),
            )
