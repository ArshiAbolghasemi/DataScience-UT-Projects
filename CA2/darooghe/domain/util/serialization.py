from dataclasses import is_dataclass, fields
from datetime import datetime, date
from enum import Enum
from typing import Any, Dict, TypeVar, Type

T = TypeVar("T")


def serializable(cls: Type[T]) -> Type[T]:
    if not is_dataclass(cls):
        raise TypeError("@serializable should be used on dataclasses")

    def to_dict(self) -> Dict[str, Any]:
        result = {}
        for field in fields(self):
            value = getattr(self, field.name)
            result[field.name] = __convert_value(value)
        return result

    def __convert_value(value: Any) -> Any:
        if __has_serializable(value):
            return value.to_dict()
        elif isinstance(value, Enum):
            return value.value
        elif isinstance(value, (datetime, date)):
            return value.isoformat()
        elif isinstance(value, (list, tuple)):
            return [__convert_value(item) for item in value]
        elif isinstance(value, dict):
            return {k: __convert_value(v) for k, v in value.items()}
        return None

    def __has_serializable(obj: object) -> bool:
        return is_dataclass(obj) and hasattr(obj, "to_dict")

    setattr(cls, "to_dict", to_dict)
    return cls
