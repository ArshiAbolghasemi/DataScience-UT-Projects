from pyspark.sql import DataFrame, SparkSession

from darooghe.domain.entity.transaction import merchant_business_hours
from darooghe.infrastructure.data_processing import schemas


class MerchantRepository:

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def get_merchant_business_hours(self) -> DataFrame:
        return self.spark.createDataFrame(
            data=[
                (
                    business_hour.merchant_category.value,
                    business_hour.opening_hour,
                    business_hour.closing_hour,
                )
                for business_hour in merchant_business_hours
            ],
            schema=schemas.merchant_hours_schema,
        )
