from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class DroneEnvelope:
    node: str
    value: Any
    timestamp: float = field(default_factory=time.time)
    seq: int = 0
    source_id: str = ""
