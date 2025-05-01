from dataclasses import dataclass
from decimal import Decimal

from darooghe.domain.util.serialization import Serializer, serializable


@dataclass
@serializable
class Location(Serializer):
    lat: Decimal
    lng: Decimal
