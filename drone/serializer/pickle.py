from __future__ import annotations

import pickle
from typing import Any

from drone.serializer import DroneSerializer


class PickleSerializer(DroneSerializer):
    def __init__(self, protocol: int = pickle.HIGHEST_PROTOCOL):
        self._protocol = protocol

    def serialize(self, obj: Any) -> bytes:
        return pickle.dumps(obj, protocol=self._protocol)

    def deserialize(self, data: bytes) -> Any:
        return pickle.loads(data)  # noqa: S301
