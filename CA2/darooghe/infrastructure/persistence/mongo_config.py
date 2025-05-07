import os
from typing import List, Protocol, Type, runtime_checkable

from pymongo import ASCENDING, IndexModel


@runtime_checkable
class CollectionProtocol(Protocol):
    @classmethod
    def get_name(cls) -> str: ...

    @classmethod
    def get_indexes(cls) -> List[IndexModel]: ...


class Mongo:

    class DB:
        DAROOGHE = "darooghe"

    class Config:
        MONGO_DB_TRANSACTION_DATA_TTL = int(
            os.getenv("MONGO_DB_TRANSACTION_DATA_TTL", 86400)
        )
        MONGO_URI = os.getenv("MONGO_URI", "")

    class Collections:
        @classmethod
        def get_all_collectinos(cls) -> List[Type[CollectionProtocol]]:
            collections = []

            for _, attr in vars(cls).items():
                if isinstance(attr, type) and isinstance(attr, CollectionProtocol):
                    collections.append(attr)

            return collections

        @classmethod
        def _get_index_created_at_ttl(cls):
            return IndexModel(
                [("created_at", ASCENDING)],
                expireAfterSeconds=Mongo.Config.MONGO_DB_TRANSACTION_DATA_TTL,
                name="created_at_ttl_idx",
            )

        class Transaction:
            @classmethod
            def get_name(cls) -> str:
                return "transaction"

            @classmethod
            def get_indexes(cls) -> List[IndexModel]:
                return [Mongo.Collections._get_index_created_at_ttl()]

        class TransactionTemporalPatterns:
            @classmethod
            def get_name(cls) -> str:
                return "transaction_temporal_patterns"

            @classmethod
            def get_indexes(cls) -> List[IndexModel]:
                return [Mongo.Collections._get_index_created_at_ttl()]

        class MerchantPeaks:
            @classmethod
            def get_name(cls) -> str:
                return "merchant_peaks"

            @classmethod
            def get_indexes(cls) -> List[IndexModel]:
                return [Mongo.Collections._get_index_created_at_ttl()]

        class CustomerMetrics:
            @classmethod
            def get_name(cls) -> str:
                return "customer_metrics"

            @classmethod
            def get_indexes(cls) -> List[IndexModel]:
                return [Mongo.Collections._get_index_created_at_ttl()]

        class CustomerSegmentStats:
            @classmethod
            def get_name(cls) -> str:
                return "customer_segment_stats"

            @classmethod
            def get_indexes(cls) -> List[IndexModel]:
                return [Mongo.Collections._get_index_created_at_ttl()]

        class MerchantCategoryCustomerSegments:
            @classmethod
            def get_name(cls) -> str:
                return "merchant_category_customer_segments"

            @classmethod
            def get_indexes(cls) -> List[IndexModel]:
                return [Mongo.Collections._get_index_created_at_ttl()]

        class MerchantCategoryStats:
            @classmethod
            def get_name(cls) -> str:
                return "merchant_category_stats"

            @classmethod
            def get_indexes(cls) -> List[IndexModel]:
                return [Mongo.Collections._get_index_created_at_ttl()]

        class MerchantCategoryTimeDist:
            @classmethod
            def get_name(cls) -> str:
                return "merchant_category_time_dist"

            @classmethod
            def get_indexes(cls) -> List[IndexModel]:
                return [Mongo.Collections._get_index_created_at_ttl()]

        class TransactionPerTimeOfDay:
            @classmethod
            def get_name(cls) -> str:
                return "transaction_per_time_of_day"

            @classmethod
            def get_indexes(cls) -> List[IndexModel]:
                return [Mongo.Collections._get_index_created_at_ttl()]

        class WeeklySpendingTrend:
            @classmethod
            def get_name(cls) -> str:
                return "weeklt_spending_trend"

            @classmethod
            def get_indexes(cls) -> List[IndexModel]:
                return [Mongo.Collections._get_index_created_at_ttl()]

        class CommissionPerMerchantCategroy:
            @classmethod
            def get_name(cls) -> str:
                return "commission_per_merhcant_category"

            @classmethod
            def get_indexes(cls) -> List[IndexModel]:
                return [Mongo.Collections._get_index_created_at_ttl()]

        class CommissionTrends:
            @classmethod
            def get_name(cls) -> str:
                return "commission_trends"

            @classmethod
            def get_indexes(cls) -> List[IndexModel]:
                return [Mongo.Collections._get_index_created_at_ttl()]
