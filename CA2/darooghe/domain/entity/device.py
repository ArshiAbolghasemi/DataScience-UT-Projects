from dataclasses import dataclass

from darooghe.domain.util.serialization import serializable


@serializable
@dataclass
class Device:
    os: str
    app_version: str
    device_model: str
