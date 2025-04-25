from dataclasses import dataclass
from typing import Optional
from entitiy import device, location
from enum import Enum


class MerchantCategory(str, Enum):
    RETAIL = "retail"
    FOOD_SERVICE = "food_service"
    ENTERTAINMENT = "entertainment"
    TRANSPORTATION = "transportation"
    GOVERNMENT = "government"


class PaymentMethod(str, Enum):
    ONLINE = "online"
    POS = "pos"
    MOBILE = "mobile"
    NFC = "nfc"


class CommissionType(str, Enum):
    FLAT = "flat"
    PROGRESSIVE = "progressive"
    TIERED = "tiered"


class CustomerType(str, Enum):
    INDIVIDUAL = "individual"
    CIP = "cip"
    BUSINESS = "business"


class FailureReason(str, Enum):
    CANCELLED = "cancelled"
    INSUFFICIENT_FUNDS = "insufficient_funds"
    SYSTEM_ERROR = "system_error"
    FRAUD_PREVENTED = "fraud_prevented"


@dataclass
class Transaction:
    transaction_id: str
    timestamp: str
    customer_id: str
    merchant_id: str
    merchant_category: MerchantCategory
    payment_method: PaymentMethod
    amount: int
    location: location.Location
    device_info: Optional[device.Device]
    status: str
    commission_type: CommissionType
    commission_amount: int
    vat_amount: int
    total_amount: int
    customer_type: CustomerType
    risk_level: int
    failure_reason: Optional[FailureReason]
