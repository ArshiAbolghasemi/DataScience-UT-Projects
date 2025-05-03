from dataclasses import MISSING, is_dataclass, fields, fields
from datetime import UTC, datetime, date, tzinfo
from enum import Enum
from typing import (
    Any,
    Dict,
    List,
    TypeVar,
    Type,
    Protocol,
    Union,
    get_type_hints,
    get_origin,
    get_args,
)
import dateutil.parser

T = TypeVar("T")


class Serializer(Protocol):
    def to_dict(self) -> dict: ...
    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any]) -> T: ...


def serializable(cls: Type[T]) -> Type[T]:
    if not is_dataclass(cls):
        raise TypeError("@serializable should be used on dataclasses")

    def to_dict(self) -> Dict[str, Any]:
        result = {}
        for field in fields(self):
            value = getattr(self, field.name)
            result[field.name] = __convert_value(value)
        return result

    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
        processed_data = {}
        type_hints = get_type_hints(cls)

        for field in fields(cls):
            if field.name not in data:
                if field.default is not MISSING:
                    processed_data[field.name] = field.default
                elif field.default_factory is not MISSING:
                    processed_data[field.name] = field.default_factory()
                else:
                    raise ValueError(f"Missing required field: {field.name}")
            else:
                value = data[field.name]
                field_type = type_hints[field.name]
                processed_data[field.name] = __parse_value(value, field_type)

        return cls(**processed_data)

    def __convert_value(value: Any) -> Any:
        if value is None:
            return None
        elif __has_serializable(value):
            return value.to_dict()
        elif isinstance(value, Enum):
            return value.value
        elif isinstance(value, (datetime, date)):
            return value.isoformat()
        elif isinstance(value, (list, tuple)):
            return [__convert_value(item) for item in value]
        elif isinstance(value, dict):
            return {k: __convert_value(v) for k, v in value.items()}
        else:
            return str(value)

    def __parse_value(value: Any, target_type: Type) -> Any:
        if value is None:
            return None

        origin_type = get_origin(target_type)

        if origin_type is Union and type(None) in get_args(target_type):
            actual_type = next(t for t in get_args(target_type) if t is not type(None))
            return __parse_value(value, actual_type)

        if origin_type is list or origin_type is List:
            item_type = get_args(target_type)[0]
            return [__parse_value(item, item_type) for item in value]

        if origin_type is dict or origin_type is Dict:
            key_type, val_type = get_args(target_type)
            return {
                __parse_value(k, key_type): __parse_value(v, val_type)
                for k, v in value.items()
            }

        if isinstance(target_type, type) and issubclass(target_type, Enum):
            return target_type(value)

        if target_type is datetime:
            return dateutil.parser.parse(value).replace(tzinfo=UTC)
        if target_type is date:
            return dateutil.parser.parse(value).date()

        if __is_serializable_type(target_type):
            return target_type.from_dict(value)

        try:
            return target_type(value)
        except (TypeError, ValueError):
            return value

    def __has_serializable(obj: object) -> bool:
        return is_dataclass(obj) and hasattr(obj, "to_dict")

    def __is_serializable_type(type_obj: Type) -> bool:
        return (
            isinstance(type_obj, type)
            and is_dataclass(type_obj)
            and hasattr(type_obj, "from_dict")
        )

    setattr(cls, "to_dict", to_dict)
    setattr(cls, "from_dict", from_dict)
    return cls
