from enum import Enum
from typing import NamedTuple


class Segment(str, Enum):
    HIGH_VALUE = "high_value"
    FREQUENT = "frequent"
    NEW = "new"
    REGULAR = "regular"


class _SegmentRules(NamedTuple):
    HIGH_VALUE_MIN_SPEND: float = 1000.0
    HIGH_VALUE_MIN_TRANSACTIONS: int = 5
    FREQUENT_MIN_TRANSACTIONS: int = 10
    NEW_MAX_DAYS: int = 7


SEGMENT_RULES = _SegmentRules()
