import os
import uuid
import random
from datetime import UTC, datetime
from faker import Faker
from domain.entity.location import Location
from domain.entity import transaction, device

__faker = Faker()

EVENT_RATE = float(os.getenv("EVENT_RATE", 100))  # Default: 100, Range: 10-1000
PEAK_FACTOR = float(os.getenv("PEAK_FACTOR", 2.5))  # Default: 2.5, Range: 1.0-5.0
FRAUD_RATE = float(os.getenv("FRAUD_RATE", 0.02))  # Default: 0.02, Range: 0.0-0.1
DECLINED_RATE = float(os.getenv("DECLINED_RATE", 0.05))  # Default: 0.05, Range: 0.0-0.2
MERCHANT_COUNT = int(os.getenv("MERCHANT_COUNT", 50))  # Default: 50, Range: 10-500
CUSTOMER_COUNT = int(
    os.getenv("CUSTOMER_COUNT", 1000)
)  # Default: 1000, Range: 100-10000
MIN_TRANSACTION_AMOUNT = int(os.getenv("MIN_TRANSACTION_AMOUNT", 50000))
MAX_TRANSACTION_AMOUNT = int(os.getenv("MAX_TRANSACTION_AMOUNT", 2000000))


def generate_transaction_event(**kwargs):
    timestamp = kwargs.get("timestamp", datetime.now(UTC))
    transaction_id = kwargs.get("transaction_id", str(uuid.uuid4()))
    customer_id = kwargs.get("customer_id", f"cust_{random.randint(1, CUSTOMER_COUNT)}")
    merchant_id = kwargs.get(
        "merchant_id", f"merhc_{random.randint(1, MERCHANT_COUNT)}"
    )
    merchant_category = kwargs.get(
        "merchant_category", random.choice(list(transaction.MerchantCategory))
    )
    payment_method = kwargs.get(
        "payment_method", random.choice(list(transaction.PaymentMethod))
    )
    amount = kwargs.get(
        "amount", random.randint(MAX_TRANSACTION_AMOUNT, MAX_TRANSACTION_AMOUNT)
    )
    location = kwargs.get(
        "location",
        Location(
            lat=__faker.latitude(),
            lng=__faker.longitude(),
        ),
    )
    device_info = kwargs.get(
        "device_info",
        random.choice(
            [
                device.Device(
                    os="Android", app_version="2.4.1", device_model="Samsung Galaxy S25"
                ),
                device.Device(os="IOS", app_version="3.1.0", device_model="iPhone15"),
                device.Device(
                    os="Android", app_version="1.9.5", device_model="Google Pixel 6"
                ),
            ]
        ),
    )
    if random.random() < DECLINED_RATE:
        status = transaction.Status.DECLINED
        failure_reason = random.choice(list(transaction.FailureReason))
    else:
        status = transaction.Status.APPROVED
        failure_reason = None
    if random.random() < FRAUD_RATE:
        risk_level = transaction.RiskLevel.CRITICAL
    else:
        risk_level = random.choice(
            [
                transaction.RiskLevel.LOW,
                transaction.RiskLevel.MEDIUM,
                transaction.RiskLevel.HIGH,
            ]
        )
    commission_type = random.choice(list(transaction.CommissionType))
    commission_amount = int(amount * transaction.COMMISSION_RATIO)
    vat_amount = int(amount * transaction.VAT_RATIO)
    total_amount = amount + vat_amount
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
    )


def generate_historical_transactions(count=20000, days_back=7):
    transactions = []

    for _ in range(count):
        event_time = __faker.date_time_between(
            start_date=f"-{days_back}d",
            end_date="now",
        )
        transaction = generate_transaction_event(timestamp=event_time)
        transactions.append(transaction)

    return transactions


def generate_transactions_batch(count=100):
    transactions = []
    for _ in range(count):
        transaction = generate_transaction_event()
        transactions.append(transaction)

    return transactions
