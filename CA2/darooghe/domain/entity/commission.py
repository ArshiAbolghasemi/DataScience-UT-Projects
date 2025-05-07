class CommissionModel:

    class Flat:
        RATE = 0.025

    class Tiered:

        class Tier1:
            MAX_AMOUNT_THRESHOLD = 10_000.0
            RATE = 0.03

        class Tier2:
            MAX_AMOUNT_THRESHOLD = 50_000.0
            RATE = 0.02
            BASE_COMMISSION = 300.0

        class Tier3:
            RATE = 0.015
            BASE_COMMISSION = 1100.0

    class Volume:
        class Small:
            MAX_AMOUNT_THRESHOLD = 10_000.0
            RATE = 0.02

        class Medium:
            MAX_AMOUNT_THRESHOLD = 50_000.0
            RATE = 0.018

        class Large:
            RATE = 0.015
