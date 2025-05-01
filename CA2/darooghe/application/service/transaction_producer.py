import json
import logging
import os
import random
from datetime import datetime, UTC
import time
from typing import Optional

from confluent_kafka import KafkaError, Message

from darooghe.domain.factory.transaction_factory import TransactionFactory
from darooghe.infrastructure.service.messaging.kafka import KafkaService
from darooghe.infrastructure.service.messaging.kafka_config import (
    KafkaGroups,
    KafkaTopics,
)


class TransactionProducer:
    def __init__(self):
        self.__config = self.__load_config()
        self.__transaction_factory = TransactionFactory(self.__config)
        kafka_broker = os.getenv("KAFKA_BROKER", "kafka:9092")
        self.__kafka_service = KafkaService(
            bootstrap_servers=[kafka_broker],
            group_id=KafkaGroups.DAROOGHE_TRANSACTIONS_PRODUCER,
        )
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
        )

    def __load_config(self) -> dict:
        return {
            "event_rate": float(
                os.getenv("EVENT_RATE", 100)
            ),  # Default: 100, Range: 10-1000
            "peak_factor": float(
                os.getenv("PEAK_FACTOR", 2.5)
            ),  # Default: 2.5, Range: 1.0-5.0
            "fraud_rate": float(
                os.getenv("FRAUD_RATE", 0.02)
            ),  # Default: 0.02, Range: 0.0-0.1
            "declined_rate": float(
                os.getenv("DECLINED_RATE", 0.05)
            ),  # Default: 0.05, Range: 0.0-0.2
            "merchant_count": int(
                os.getenv("MERCHANT_COUNT", 50)
            ),  # Default: 50, Range: 10-500
            "customer_count": int(
                os.getenv("CUSTOMER_COUNT", 1000)
            ),  # Default: 1000, Range: 100-10000
            "min_amount": int(os.getenv("MIN_TRANSACTION_AMOUNT", 50000)),
            "max_amount": int(os.getenv("MAX_TRANSACTION_AMOUNT", 2000000)),
            "commission_ratio": float(os.getenv("COMMISSION_RATIO", 0.02)),
            "vat_ratio": float(os.getenv("VAT_RATIO", 0.09)),
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
        self.__kafka_service.flush_topic(topic=KafkaTopics.DAROOGHE_TRANSACTIONS)
        historical_transactions = (
            self.__transaction_factory.create_historical_transactions()
        )
        for tx in historical_transactions:
            self.__kafka_service.produce_message(
                topic_name=KafkaTopics.DAROOGHE_TRANSACTIONS,
                message=json.dumps(tx.to_dict()),
                key=tx.customer_id,
                callback=self.__delivery_report,
            )

        while True:
            time.sleep(self.__nhpp_rate())
            transaction = self.__transaction_factory.create_transaction()
            self.__kafka_service.produce_message(
                topic_name=KafkaTopics.DAROOGHE_TRANSACTIONS,
                message=json.dumps(transaction.to_dict()),
                key=transaction.customer_id,
                callback=self.__delivery_report,
            )


if __name__ == "__main__":
    transaction_producer = TransactionProducer()
    transaction_producer.produce_stream()
