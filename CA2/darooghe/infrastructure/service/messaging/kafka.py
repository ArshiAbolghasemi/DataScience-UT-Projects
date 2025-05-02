import logging

from confluent_kafka import (
    Producer,
    KafkaException,
    KafkaError,
)
from typing import Any, List, Optional, Callable, Union

from confluent_kafka.admin import AdminClient


class KafkaService:
    def __init__(self, bootstrap_servers: List[str]):
        self.bootstrap_servers = bootstrap_servers

        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
        )
        self.__logger: logging.Logger = logging.getLogger(__name__)
        self.__admin_clinet: AdminClient = AdminClient(
            {"bootstrap.servers": self.bootstrap_servers, "request.timeout.ms": 5000}
        )

    @property
    def bootstrap_servers(self) -> str:
        return self.__bootstrap_servers

    @bootstrap_servers.setter
    def bootstrap_servers(self, value: List[str]):
        cleaned_servers = [str(s).strip() for s in value if str(s).strip()]
        self.__bootstrap_servers = ",".join(cleaned_servers)

    def __get_producer(self, **kwargs) -> Producer:
        producer_conf = {
            "bootstrap.servers": self.bootstrap_servers,
        }
        message_timeout_ms = kwargs.get("message_timeout_ms", None)
        if not message_timeout_ms is None:
            producer_conf["message.timeout.ms"] = message_timeout_ms
        return Producer(producer_conf)

    def is_topic_existed(self, topic) -> bool:
        metadata = self.__admin_clinet.list_topics(timeout=5)
        return topic in metadata.topics

    def delete_topic(
        self, topic: str, timeout: float = 30.0, operation_timeout: float = 10.0
    ) -> bool:
        if not isinstance(topic, str) or not topic.strip():
            raise ValueError("Topic name must be a non-empty string")

        if not isinstance(timeout, (int, float)) or timeout <= 0:
            raise ValueError("Timeout must be positive number")
        print(int(operation_timeout * 1000))
        try:
            fs = self.__admin_clinet.delete_topics(
                [topic], operation_timeout=int(operation_timeout * 1000)
            )
            fs[topic].result(timeout=timeout)
            self.__logger.info(f"Successfully deleted topic: {topic}")
            return True

        except KafkaException as e:
            if e.args[0].code() == "UNKNOWN_TOPIC_OR_PART":
                self.__logger.info(f"Topic {topic} does not exist")
                return True
            self.__logger.error(f"Failed to delete topic {topic}: {e}")
            return False
        except Exception as e:
            self.__logger.error(f"Unexpected error deleting topic {topic}: {e}")
            return False

    def produce_message(
        self,
        topic_name: str,
        message: Union[str, bytes],
        key: Optional[Union[str, bytes]] = None,
        callback: Optional[Callable[[Optional[KafkaError], Any], None]] = None,
    ) -> None:
        producer = self.__get_producer(message_timeout_ms=5000)

        try:
            producer.poll(0)
            producer.produce(
                topic=topic_name, key=key, value=message, callback=callback
            )
            producer.flush()

        except KafkaException as e:
            self.__logger.error(f"Failed to produce message: {e}")
        finally:
            producer.flush()
