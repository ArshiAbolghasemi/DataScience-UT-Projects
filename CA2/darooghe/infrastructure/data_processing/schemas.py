from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

location_schema = StructType(
    [
        StructField("lat", DecimalType(scale=6), False),
        StructField("lng", DecimalType(scale=6), False),
    ]
)

device_schema = StructType(
    [
        StructField("os", StringType(), False),
        StructField("app_version", StringType(), False),
        StructField("device_model", StringType(), False),
    ]
)

transaction_schema = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("customer_id", StringType(), False),
        StructField("merchant_id", StringType(), False),
        StructField("merchant_category", StringType(), False),
        StructField("payment_method", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("location", location_schema, False),
        StructField("device_info", device_schema),
        StructField("status", StringType(), False),
        StructField("commission_type", StringType(), False),
        StructField("commission_amount", DoubleType(), False),
        StructField("vat_amount", DoubleType(), False),
        StructField("total_amount", DoubleType(), False),
        StructField("customer_type", StringType(), False),
        StructField("risk_level", StringType(), False),
        StructField("failure_reason", StringType()),
    ]
)
