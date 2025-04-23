from dataclasses import dataclass

@dataclass
class Device:
    os:           str
    app_version:  str
    device_model: str
