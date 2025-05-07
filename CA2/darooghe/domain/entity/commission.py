from enum import Enum


class CommissionFlat(Enum):
    RATE = 0.025


class CommissionTiered:

    class Tier1(Enum):
        MAX_AMOUNT_THRESHOLD = 10_000.0
        RATE = 0.03

    class Tier2(Enum):
        MAX_AMOUNT_THRESHOLD = 50_000.0
        RATE = 0.02
        BASE_COMMISSION = 300.0

    class Tier3(Enum):
        RATE = 0.015
        BASE_COMMISSION = 1100.0


class CommissionVolume:
    class Small(Enum):
        MAX_AMOUNT_THRESHOLD = 10_000.0
        RATE = 0.02

    class Medium(Enum):
        MAX_AMOUNT_THRESHOLD = 50_000.0
        RATE = 0.018

    class Large(Enum):
        RATE = 0.015
