from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.query import StreamingQuery


def read_from_kafka_stream(
    spark: SparkSession, kafka_brokers: List, kafka_topic: str, **kwrgs
) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", ",".join(kafka_brokers))
        .option("subscribe", kafka_topic)
        .option("startingOffsets", kwrgs.get("starting_offsets", "latest"))
        .option("failOnDataLoss", kwrgs.get("fail_on_dataloss", "false"))
        .load()
    )


def write_stream_to_kafks(
    df: DataFrame,
    kafka_brokers: List,
    kafka_topic: str,
    checkpoint_location: str,
    outout_mode: str = "append",
) -> StreamingQuery:
    return (
        df.select(F.to_json(F.struct("*")).alias("value"))
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", ",".join(kafka_brokers))
        .option("topic", kafka_topic)
        .option("checkpointLocation", checkpoint_location)
        .outputMode(outout_mode)
        .start()
    )
