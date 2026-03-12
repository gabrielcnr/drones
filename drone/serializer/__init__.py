from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class DroneSerializer(ABC):
    @abstractmethod
    def serialize(self, obj: Any) -> bytes: ...

    @abstractmethod
    def deserialize(self, data: bytes) -> Any: ...
