import time
import logging

from confluent_kafka import (
    Producer,
    Consumer,
    KafkaException,
    KafkaError,
    TopicPartition,
)
from typing import Any, List, Optional, Callable, Tuple, Union


class KafkaService:
    def __init__(self, bootstrap_servers: List[str], group_id: str):
        self.bootstrap_servers = bootstrap_servers
        self.__group_id: str = group_id

        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
        )
        self.__logger: logging.Logger = logging.getLogger(__name__)

    @property
    def bootstrap_servers(self) -> str:
        return self.__bootstrap_servers

    @bootstrap_servers.setter
    def bootstrap_servers(self, value: List[str]):
        cleaned_servers = [str(s).strip() for s in value if str(s).strip()]
        self.__bootstrap_servers = ",".join(cleaned_servers)

    def __get_cosumer(self, **kwargs) -> Consumer:
        group_id = kwargs.get("group_id", None)
        if group_id is None:
            raise ValueError("group_id is missed")
        consumer_conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": kwargs.get("auto_offset_rest", "earliest"),
            "enable.auto.commit": kwargs.get("enable_auto_commit", False),
        }
        auto_commit_interval_ms = kwargs.get("auto_commit_interval_ms", None)
        if not auto_commit_interval_ms is None:
            consumer_conf["auto_commit_interval_ms"] = auto_commit_interval_ms
        return Consumer(consumer_conf)

    def __get_producer(self, **kwargs) -> Producer:
        producer_conf = {
            "bootstrap.servers": self.bootstrap_servers,
        }
        message_timeout_ms = kwargs.get("message_timeout_ms", None)
        if not message_timeout_ms is None:
            producer_conf["message.timeout.ms"] = message_timeout_ms
        return Producer(producer_conf)

    def flush_topic(
        self,
        topic: str,
        timeout: Optional[float] = 30.0,
        max_empty_polls: int = 5,
        message_callback: Optional[Callable] = None,
    ) -> Tuple[bool, int]:
        if not isinstance(topic, str) or not topic.strip():
            raise ValueError("Topic name must be a non-empty string")

        if timeout is not None and (
            not isinstance(timeout, (int, float)) or timeout <= 0
        ):
            raise ValueError("Timeout must be positive number or None")

        if not isinstance(max_empty_polls, int) or max_empty_polls <= 0:
            raise ValueError("max_empty_polls must be positive integer")

        start_time = time.time()
        message_count = 0
        empty_polls = 0
        consumer = None
        try:
            consumer = self.__get_cosumer(
                group_id=f"{self.__group_id}-flush-{time.time()}",
                auto_commit_interval_ms=100,
            )
            consumer.subscribe([topic])

            while True:
                if timeout and (time.time() - start_time > timeout):
                    self.__logger.warning(f"Flush timeout reached for {topic}")
                    break

                if empty_polls >= max_empty_polls:
                    self.__logger.info(
                        f"Stopping flush after {max_empty_polls} empty polls"
                    )
                    break

                msg = consumer.poll(1.0)

                if msg is None:
                    empty_polls += 1
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    self.__logger.error(f"Consumer error: {msg.error()}")
                    return (False, message_count)

                empty_polls = 0
                message_count += 1

                if message_callback:
                    try:
                        message_callback(msg)
                    except Exception as e:
                        self.__logger.error(f"Callback error: {e}")

            return (True, message_count)

        except Exception as e:
            self.__logger.error(f"Flush failed: {e}")
            return (False, message_count)
        finally:
            if consumer is not None:
                try:
                    consumer.close()
                except Exception as close_error:
                    self.__logger.error(f"Error closing consumer: {close_error}")

    def topic_has_messages(self, topic: str) -> bool:
        self.__logger.info(f"Checking if topic {topic} has messages")

        consumer = None
        try:
            consumer = self.__get_cosumer(
                group_id=f"{self.__group_id}-metadata-checker-{time.time()}"
            )

            cluster_metadata = consumer.list_topics(topic)
            if topic not in cluster_metadata.topics:
                return False

            partitions = cluster_metadata.topics[topic].partitions
            if not partitions:
                return False

            for partition_id in partitions:
                tp = TopicPartition(topic, partition_id)
                low, high = consumer.get_watermark_offsets(tp)
                if high > low:
                    return True

            return False

        except Exception as e:
            self.__logger.error(f"Error checking topic: {e}")
            return False
        finally:
            if consumer is not None:
                consumer.close()

    def produce_message(
        self,
        topic_name: str,
        message: Union[str, bytes],
        key: Optional[Union[str, bytes]] = None,
        callback: Optional[Callable[[Optional[KafkaException], Any], None]] = None,
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
