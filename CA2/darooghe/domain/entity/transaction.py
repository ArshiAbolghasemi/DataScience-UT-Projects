from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import os
from typing import Dict, Optional, Tuple
from enum import Enum

from darooghe.domain.entity.device import Device, OS
from darooghe.domain.entity.error import ErrorCode
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

    def validate(self) -> Dict[ErrorCode, str]:
        errors = {}

        amount_consistent, amount_msg = self.__has_amount_consistency()
        if not amount_consistent:
            errors[ErrorCode.AMOUNT_CONSISTENCY] = amount_msg

        time_valid, time_msg = self.__has_time_warping()
        if not time_valid:
            errors[ErrorCode.TIME_WARPING] = time_msg

        device_valid, device_msg = self.__has_device_mismatch()
        if not device_valid:
            errors[ErrorCode.DEVICE_MISMATCH] = device_msg

        return errors

    def __has_amount_consistency(self) -> Tuple[bool, Optional[str]]:
        if self.total_amount == self.amount + self.vat_amount + self.commission_amount:
            return True, None

        return (
            False,
            "total amount does not match sum amount, vat_amount and commission_amount",
        )

    def __has_time_warping(self) -> Tuple[bool, Optional[str]]:
        current_time = datetime.now(UTC)
        transaction_time = self.timestamp
        if transaction_time > current_time:
            return (
                False,
                f"Future timestamp: {transaction_time}, current_time: {current_time}",
            )
        elif transaction_time < (
            current_time
            - timedelta(
                days=int(os.getenv("TRANSACTION_OLD_TIMESTAMP_THRESHOLD_DAYS", 1))
            )
        ):
            return False, f"Timestamp too old: {transaction_time}"

        return True, None

    def __has_device_mismatch(self) -> Tuple[bool, Optional[str]]:
        if self.payment_method != PaymentMethod.MOBILE:
            return True, None

        if self.device_info is None:
            return True, None

        if self.device_info.os in [OS.IOS, OS.ANDROID]:
            return True, None

        return False, f"Invalid OS '{self.device_info.os.value}' for mobile payment"
