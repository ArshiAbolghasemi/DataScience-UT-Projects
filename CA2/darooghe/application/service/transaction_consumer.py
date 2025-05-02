from datetime import UTC, datetime
import json
import logging
import os
from typing import Counter, Dict, Optional, cast

from confluent_kafka import KafkaError

from darooghe.domain.entity.error import TransactionErrorLog
from darooghe.domain.entity.transaction import Transaction
from darooghe.domain.util.serialization import Serializer
from darooghe.infrastructure.service.messaging.kafka import KafkaErrorLog, KafkaService
from darooghe.infrastructure.service.messaging.kafka_config import (
    LOCAL_KAFKA_BROKER,
    KafkaGroups,
    KafkaTopics,
)


class TransactionConsumer:

    def __init__(self) -> None:
        kafka_broker = os.getenv("KAFKA_BROKER", LOCAL_KAFKA_BROKER)
        self.__kafka_service = KafkaService([kafka_broker])
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
        )

    def execute(self, batch_size: int = 100):
        while True:
            results = self.__kafka_service.consume(
                topic=KafkaTopics.DAROOGHE_TRANSACTIONS,
                group_id=KafkaGroups.DAROOGHE_TRANSACTIONS_CONSUMER,
                callback=self.__process_transaction,
                batch_size=batch_size,
                max_messages=int(os.getenv("TRANSACTION_CONSUMER_MAX_MSG", 10000)),
            )

            counter = Counter(results)
            valid_count = counter[True]
            invalid_count = counter[False]
            logging.info(
                f"Processed batch: {valid_count} valid, {invalid_count} invalid transactions"
            )

    def __process_transaction(
        self, msg_value: Optional[Dict], err: Optional[KafkaError]
    ):
        if err:
            self.__kafka_service.produce_message(
                topic_name=KafkaTopics.DAROOGHE_ERROR_LOGS,
                message=json.dumps(
                    cast(Serializer, KafkaErrorLog.create_error(err)).to_dict()
                ),
                key=f"kafka-transaction-consumer-{datetime.now(UTC).isoformat()}",
            )
            return False

        if msg_value is None:
            self.__kafka_service.produce_message(
                topic_name=KafkaTopics.DAROOGHE_ERROR_LOGS,
                message=json.dumps("msg_value is empty"),
                key=f"kafka-transaction-consumer-{datetime.now(UTC).isoformat()}",
            )
        try:
            if msg_value is None:

                return False
            transaction = cast(
                Transaction, cast(Serializer, Transaction).from_dict(msg_value)
            )
            errors = transaction.validate()
            if not errors:
                return True

            error_data = TransactionErrorLog.create_error(
                transaction_id=transaction.transaction_id,
                errors=errors,
                msg_value=msg_value,
            )
            self.__kafka_service.produce_message(
                topic_name=KafkaTopics.DAROOGHE_ERROR_LOGS,
                message=json.dumps(cast(Serializer, error_data).to_dict()),
                key=f"kafka-transaction-consumer-validations-{datetime.now(UTC).isoformat()}",
            )
            return True
        except Exception as e:
            error = TransactionErrorLog.create_error(
                transaction_id=(
                    msg_value.get("transaction_id", None) if msg_value else None
                ),
                errors=str(e),
                msg_value=msg_value,
            )
            self.__kafka_service.produce_message(
                topic_name=KafkaTopics.DAROOGHE_ERROR_LOGS,
                message=json.dumps(cast(Serializer, error).to_dict()),
                key=f"kafka-transaction-consumer-{datetime.now(UTC).isoformat()}",
            )
            return False


if __name__ == "__main__":
    transaction_consumer = TransactionConsumer()
    transaction_consumer.execute()
