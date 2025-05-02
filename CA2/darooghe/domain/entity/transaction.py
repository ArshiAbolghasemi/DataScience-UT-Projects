from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from enum import Enum

from darooghe.domain.entity.device import Device
from darooghe.domain.entity.location import Location

from darooghe.domain.util.serialization import serializable


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


class Status(str, Enum):
    DECLINED = "declined"
    APPROVED = "approved"


class RiskLevel(str, Enum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3
    CRITICAL = 5


@serializable
@dataclass
class Transaction:
    transaction_id: str
    timestamp: datetime
    customer_id: str
    merchant_id: str
    merchant_category: MerchantCategory
    payment_method: PaymentMethod
    amount: int
    location: Location
    device_info: Optional[Device]
    status: str
    commission_type: CommissionType
    commission_amount: int
    vat_amount: int
    total_amount: int
    customer_type: CustomerType
    risk_level: RiskLevel
    failure_reason: Optional[FailureReason]
