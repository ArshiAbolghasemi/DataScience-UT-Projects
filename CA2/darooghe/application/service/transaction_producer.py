import json
import logging
import os
import random
import time
from datetime import UTC, datetime
from typing import Optional, cast

from confluent_kafka import KafkaError, Message

from darooghe.domain.factory.transaction_factory import TransactionFactory
from darooghe.domain.util.logging import configure_cli_log
from darooghe.domain.util.serialization import Serializer
from darooghe.infrastructure.messaging.kafka import KafkaService
from darooghe.infrastructure.messaging.kafka_config import (
    Kafka,
)


class TransactionProducer:
    def __init__(self):
        self.__config = self.__load_config()
        self.__transaction_factory = TransactionFactory()
        kafka_broker = Kafka.Config.KAFKA_BROKER
        self.__kafka_service = KafkaService(
            bootstrap_servers=[kafka_broker],
        )

    def __load_config(self) -> dict:
        return {
            "event_rate": float(
                os.getenv("EVENT_RATE", 100)
            ),  # Default: 100, Range: 10-1000
            "peak_factor": float(
                os.getenv("PEAK_FACTOR", 2.5)
            ),  # Default: 2.5, Range: 1.0-5.0
        }

    def __nhpp_rate(self) -> float:
        current_hour = datetime.now(UTC).hour
        multiplier = self.__config["peak_factor"] if 9 <= current_hour < 18 else 1.0
        effective_rate = self.__config["event_rate"] * multiplier
        lambda_per_sec = effective_rate / 60.0
        wait_time = random.expovariate(lambda_per_sec)
        return wait_time

    def __msg_key(self, msg: Message) -> str:
        return f"{msg.topic()}/{msg.partition()}/{msg.key() or 'NULL'}"

    def __delivery_report(self, err: Optional[KafkaError], msg: Message) -> None:
        if err is not None:
            logging.error(
                f"Message delivery failed for {(self.__msg_key(msg))}: {err}",
                extra={
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "error": str(err),
                },
            )
        else:
            logging.debug(
                "Message delivered to %s [%s]",
                msg.topic(),
                msg.partition(),
                extra={
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                },
            )

    def produce_stream(self) -> None:
        if self.__kafka_service.is_topic_existed(
            topic=Kafka.Topics.DAROOGHE_TRANSACTIONS
        ):
            self.__kafka_service.delete_topic(topic=Kafka.Topics.DAROOGHE_TRANSACTIONS)

        historical_transactions = (
            self.__transaction_factory.create_historical_transactions()
        )
        for transaction in historical_transactions:
            self.__kafka_service.produce_message(
                topic_name=Kafka.Topics.DAROOGHE_TRANSACTIONS,
                message=json.dumps(cast(Serializer, transaction).to_dict()),
                key=transaction.customer_id,
                callback=self.__delivery_report,
            )

        while True:
            time.sleep(self.__nhpp_rate())
            transaction = self.__transaction_factory.create_transaction()
            self.__kafka_service.produce_message(
                topic_name=Kafka.Topics.DAROOGHE_TRANSACTIONS,
                message=json.dumps(cast(Serializer, transaction).to_dict()),
                key=transaction.customer_id,
                callback=self.__delivery_report,
            )


def __main():
    configure_cli_log()
    try:
        transaction_producer = TransactionProducer()
        transaction_producer.produce_stream()
    except KeyboardInterrupt:
        logging.info("Received keyboard interrupt, stopping transaction producer")


if __name__ == "__main__":
    __main()
