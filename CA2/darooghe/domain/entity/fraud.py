class Fraud:

    class Velocity:
        MAXIMUM_TRANSACTION_COUNT_PER_INTERVAL = 5
        LABEL = "velocity_check"
        DESCRIPTION = ">5 transactions in 2 minutes"

    class Geographical:
        MAX_DISTANCE_KM = 50
        LABEL = "geographical_impossibility"
        DESCRIPTION = ">50km movement in 5 minutes"

    class AmountAnomaly:
        AMOUNT_ANOMALY_THRESHOLD_RATIO = 10
        MINIMUM_TRANSACTION_COUNT = 3
        LABEL = "amount_anomaly"
        DESCRIPTION = f"Amount >{AMOUNT_ANOMALY_THRESHOLD_RATIO}x customer average"
