from dataclasses import dataclass
from decimal import Decimal

from darooghe.domain.util.serialization import serializable


@dataclass
@serializable
class Location:
    lat: Decimal
    lng: Decimal
