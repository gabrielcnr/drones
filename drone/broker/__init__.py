from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Callable


class DroneBroker(ABC):
    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the broker."""
        ...

    @abstractmethod
    def disconnect(self) -> None:
        """Tear down connection."""
        ...

    @abstractmethod
    def publish(self, channel: str, message: bytes) -> None:
        """Publish a serialized message to a channel."""
        ...

    @abstractmethod
    def subscribe(self, channel: str, callback: Callable[[bytes], None]) -> None:
        """Subscribe to a channel. callback is invoked with raw bytes for each message."""
        ...

    @abstractmethod
    def unsubscribe(self, channel: str) -> None:
        """Unsubscribe from a channel."""
        ...
