from dataclasses import dataclass
from decimal import Decimal

from darooghe.domain.util.serialization import serializable


@serializable
@dataclass
class Location:
    lat: Decimal
    lng: Decimal
