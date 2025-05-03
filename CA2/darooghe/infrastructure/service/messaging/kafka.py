from dataclasses import dataclass
from datetime import UTC, datetime
import json
import logging

from confluent_kafka import (
    Consumer,
    Producer,
    KafkaException,
    KafkaError,
)
from typing import Any, Dict, List, Optional, Callable, Union

from confluent_kafka.admin import AdminClient

from darooghe.domain.util.serialization import serializable


@serializable
@dataclass
class KafkaErrorLog:
    error: str
    code: str
    timestamp: datetime

    @classmethod
    def create_error(cls, err: KafkaError):
        return cls(error=err.str(), code=err.code(), timestamp=datetime.now(UTC))


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
        self.__consumers: Dict[str, Consumer] = {}

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
        if message_timeout_ms is not None:
            producer_conf["message.timeout.ms"] = message_timeout_ms
        return Producer(producer_conf)

    def __get_consumer(self, group_id: str, **kwargs) -> Consumer:
        if group_id in self.__consumers:
            return self.__consumers[group_id]

        consumer_conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": kwargs.get("auto_offset_reset", "earliest"),
            "enable.auto.commit": kwargs.get("enable_auto_commit", False),
            "session.timeout.ms": kwargs.get("session_timeout_ms", 10000),
        }
        consumer = Consumer(consumer_conf)
        self.__consumers[group_id] = consumer
        return consumer

    def __close_consumer(self, group_id: str) -> None:
        if group_id not in self.__consumers:
            self.__logger.warning(f"No Cunsumer found for Group ID: {group_id}")
            return

        consumer = self.__consumers[group_id]
        del self.__consumers[group_id]
        consumer.close()

    def is_topic_existed(self, topic) -> bool:
        metadata = self.__admin_clinet.list_topics(timeout=5)
        return topic in metadata.topics

    def delete_topic(
        self, topic: str, timeout: float = 30.0, operation_timeout: float = 30.0
    ) -> bool:
        if not isinstance(topic, str) or not topic.strip():
            raise ValueError("Topic name must be a non-empty string")

        if not isinstance(timeout, (int, float)) or timeout <= 0:
            raise ValueError("Timeout must be positive number")

        try:
            fs = self.__admin_clinet.delete_topics(
                [topic], operation_timeout=operation_timeout
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

    def consume(
        self,
        topic: str,
        group_id: str,
        callback: Callable[[Optional[Any], Optional[KafkaError]], Any],
        timeout_ms: int = 1000,
        max_messages: Optional[int] = None,
        batch_size: int = 100,
        **consumer_config,
    ) -> List[Any]:
        results = []
        message_count = 0
        consumer = self.__get_consumer(group_id, **consumer_config)
        consumer.subscribe([topic])

        try:
            self.__logger.info(f"Starting to consume from topic: {topic}")

            while True:
                if max_messages and message_count >= max_messages:
                    self.__logger.info(f"Reached max messages limit: {max_messages}")
                    break

                messages = consumer.consume(
                    num_messages=batch_size, timeout=timeout_ms / 1000
                )

                if not messages:
                    continue

                for msg in messages:
                    message_value = None
                    error = msg.error()

                    try:
                        if error:
                            self.__logger.error("Message error: {error}")
                            result = callback(None, error)
                        else:
                            message_value = json.loads(msg.value().decode("utf-8"))
                            result = callback(message_value, None)
                            consumer.commit(message=msg, asynchronous=False)

                        results.append(result)
                        message_count += 1

                    except Exception as e:
                        error = KafkaError(KafkaError.UNKNOWN, str(e))
                        result = callback(None, error)
                        results.append(result)
                        self.__logger.error(f"Error processing message: {e}")

        except Exception as e:
            self.__logger.error(f"Unexpected error in consumer: {e}")
        finally:
            self.__close_consumer(group_id)
            self.__logger.info(
                f"Finished consuming. Processed {message_count} messages"
            )

        return results
