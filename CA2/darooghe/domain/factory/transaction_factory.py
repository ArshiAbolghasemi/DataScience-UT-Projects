import os
import random
import uuid
from datetime import UTC, datetime
from typing import List, Optional, Tuple

from faker import Faker

from darooghe.domain.entity import transaction
from darooghe.domain.entity.device import OS, Device
from darooghe.domain.entity.location import Location


class TransactionFactory:
    def __init__(self):
        self.__faker = Faker()
        self.__config = self.__load_config()

    def __load_config(self) -> dict:
        return {
            "fraud_rate": float(
                os.getenv("FRAUD_RATE", 0.02)
            ),  # Default: 0.02, Range: 0.0-0.1
            "declined_rate": float(
                os.getenv("DECLINED_RATE", 0.05)
            ),  # Default: 0.05, Range: 0.0-0.2
            "merchant_count": int(
                os.getenv("MERCHANT_COUNT", 50)
            ),  # Default: 50, Range: 10-500
            "customer_count": int(
                os.getenv("CUSTOMER_COUNT", 1000)
            ),  # Default: 1000, Range: 100-10000
            "min_amount": int(os.getenv("MIN_TRANSACTION_AMOUNT", 50000)),
            "max_amount": int(os.getenv("MAX_TRANSACTION_AMOUNT", 2000000)),
            "commission_ratio": float(os.getenv("COMMISSION_RATIO", 0.02)),
            "vat_ratio": float(os.getenv("VAT_RATIO", 0.09)),
        }

    def create_transaction(self, **kwargs) -> transaction.Transaction:
        timestamp = kwargs.get("timestamp", datetime.now(UTC))
        transaction_id = kwargs.get("transaction_id", str(uuid.uuid4()))
        customer_id = kwargs.get(
            "customer_id", f"cust_{random.randint(1, self.__config['customer_count'])}"
        )
        merchant_id = kwargs.get(
            "merchant_id", f"merhc_{random.randint(1, self.__config['merchant_count'])}"
        )
        merchant_category = kwargs.get(
            "merchant_category", random.choice(list(transaction.MerchantCategory))
        )
        payment_method = kwargs.get(
            "payment_method", random.choice(list(transaction.PaymentMethod))
        )
        amount = kwargs.get(
            "amount",
            random.randint(self.__config["min_amount"], self.__config["max_amount"]),
        )
        location = kwargs.get("location", self.__random_location())
        device_info = kwargs.get("device_info", self.__random_device())
        status, failure_reason = self.__determine_status()
        risk_level = self.__determine_risk_level()
        commission_type = random.choice(list(transaction.CommissionType))
        commission_amount = int(amount * self.__config["commission_ratio"])
        vat_amount = int(amount * self.__config["vat_ratio"])
        total_amount = amount + vat_amount + commission_amount
        customer_type = random.choice(list(transaction.CustomerType))

        return transaction.Transaction(
            transaction_id=transaction_id,
            timestamp=timestamp,
            customer_id=customer_id,
            merchant_id=merchant_id,
            merchant_category=merchant_category,
            payment_method=payment_method,
            amount=amount,
            location=location,
            device_info=device_info,
            status=status,
            commission_type=commission_type,
            commission_amount=commission_amount,
            vat_amount=vat_amount,
            total_amount=total_amount,
            risk_level=risk_level,
            failure_reason=failure_reason,
            customer_type=customer_type,
            created_at=datetime.now(UTC),
        )

    def create_historical_transactions(
        self, count=20000, days_back=7
    ) -> List[transaction.Transaction]:
        transactions = []

        for _ in range(count):
            event_time = datetime.fromtimestamp(
                self.__faker.date_time_between(
                    start_date=f"-{days_back}d",
                    end_date="now",
                ).timestamp(),
                UTC,
            )
            transaction = self.create_transaction(timestamp=event_time)
            transactions.append(transaction)

        return transactions

    def __random_location(self) -> Location:
        return Location(
            lat=self.__faker.latitude(),
            lng=self.__faker.longitude(),
        )

    def __random_device(self) -> Device:
        return random.choice(
            [
                Device(
                    os=OS.ANDROID,
                    app_version="2.4.1",
                    device_model="Samsung Galaxy S25",
                ),
                Device(os=OS.IOS, app_version="3.1.0", device_model="iPhone15"),
                Device(
                    os=OS.ANDROID, app_version="1.9.5", device_model="Google Pixel 6"
                ),
            ]
        )

    def __determine_status(
        self,
    ) -> Tuple[transaction.Status, Optional[transaction.FailureReason]]:
        if random.random() < self.__config["declined_rate"]:
            status = transaction.Status.DECLINED
            failure_reason = random.choice(list(transaction.FailureReason))
        else:
            status = transaction.Status.APPROVED
            failure_reason = None
        return status, failure_reason

    def __determine_risk_level(self) -> transaction.RiskLevel:
        if random.random() < self.__config["fraud_rate"]:
            return transaction.RiskLevel.CRITICAL
        return random.choice(
            [
                transaction.RiskLevel.LOW,
                transaction.RiskLevel.MEDIUM,
                transaction.RiskLevel.HIGH,
            ]
        )
