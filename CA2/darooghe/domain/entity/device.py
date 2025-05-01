from dataclasses import dataclass

from darooghe.domain.util.serialization import Serializer, serializable


@dataclass
@serializable
class Device(Serializer):
    os: str
    app_version: str
    device_model: str
