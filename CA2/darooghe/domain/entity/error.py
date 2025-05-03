from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from darooghe.domain.util.serialization import serializable


class ErrorCode(Enum):
    AMOUNT_CONSISTENCY = "ERR_AMOUNT"
    TIME_WARPING = "ERR_TIME"
    DEVICE_MISMATCH = "ERR_DEVICE"


@serializable
@dataclass
class ErrorCodeDetail:
    code: str
    message: str

    @classmethod
    def create_error(cls, errors: Dict[ErrorCode, str]) -> List["ErrorCodeDetail"]:
        return [
            cls(code=err_code.value, message=msg) for err_code, msg in errors.items()
        ]


@serializable
@dataclass
class TransactionErrorLog:
    transaction_id: Optional[str]
    timestamp: datetime
    errors: Union[str, List[ErrorCodeDetail]]
    msg_value: Dict[str, Any]

    @classmethod
    def create_error(cls, **kwargs) -> "TransactionErrorLog":
        error_data = kwargs.get("errors", {})
        if isinstance(error_data, dict) and all(
            isinstance(k, ErrorCode) and isinstance(v, str)
            for k, v in error_data.items()
        ):
            errors = ErrorCodeDetail.create_error(error_data)
        elif isinstance(error_data, str):
            errors = error_data
        else:
            raise ValueError("invalid error type")

        return cls(
            transaction_id=kwargs.get("transaction_id", None),
            timestamp=datetime.now(UTC),
            errors=errors,
            msg_value=kwargs.get("msg_value", {}),
        )
