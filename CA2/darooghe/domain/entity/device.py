from dataclasses import dataclass
from enum import Enum

from darooghe.domain.util.serialization import serializable


class OS(str, Enum):
    ANDROID = "Android"
    IOS = "IOS"


@serializable
@dataclass
class Device:
    os: OS
    app_version: str
    device_model: str
