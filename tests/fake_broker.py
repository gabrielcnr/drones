from __future__ import annotations

from collections.abc import Callable

from drones.broker import DroneBroker


class FakeBroker(DroneBroker):
    """In-memory broker for testing. Delivers messages synchronously."""

    def __init__(self) -> None:
        self._callbacks: dict[str, Callable[[bytes], None]] = {}
        self.published: list[tuple[str, bytes]] = []
        self.connected = False

    def connect(self) -> None:
        self.connected = True

    def disconnect(self) -> None:
        self.connected = False
        self._callbacks.clear()

    def publish(self, channel: str, message: bytes) -> None:
        self.published.append((channel, message))
        callback = self._callbacks.get(channel)
        if callback is not None:
            callback(message)

    def subscribe(self, channel: str, callback: Callable[[bytes], None]) -> None:
        self._callbacks[channel] = callback

    def unsubscribe(self, channel: str) -> None:
        self._callbacks.pop(channel, None)
