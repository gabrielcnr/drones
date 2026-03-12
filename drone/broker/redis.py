from __future__ import annotations

import logging
import threading
import time
from typing import Callable

import redis

from drone.broker import DroneBroker

logger = logging.getLogger(__name__)


class RedisBroker(DroneBroker):
    def __init__(
        self,
        url: str = "redis://localhost:6379",
        max_retry_delay: float = 30.0,
    ):
        self._url = url
        self._max_retry_delay = max_retry_delay
        self._client: redis.Redis | None = None
        self._pubsub: redis.client.PubSub | None = None
        self._listener_thread: threading.Thread | None = None
        self._callbacks: dict[str, Callable[[bytes], None]] = {}
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

    def connect(self) -> None:
        self._client = redis.Redis.from_url(self._url)
        self._client.ping()
        self._pubsub = self._client.pubsub()
        self._stop_event.clear()
        logger.info("Connected to Redis at %s", self._url)

    def disconnect(self) -> None:
        self._stop_event.set()
        if self._listener_thread is not None:
            self._listener_thread.join(timeout=5.0)
            self._listener_thread = None
        if self._pubsub is not None:
            try:
                self._pubsub.unsubscribe()
                self._pubsub.close()
            except Exception:
                pass
            self._pubsub = None
        if self._client is not None:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None
        self._callbacks.clear()
        logger.info("Disconnected from Redis")

    def publish(self, channel: str, message: bytes) -> None:
        if self._client is None:
            raise RuntimeError("Broker is not connected")
        self._client.publish(channel, message)

    def subscribe(self, channel: str, callback: Callable[[bytes], None]) -> None:
        if self._pubsub is None:
            raise RuntimeError("Broker is not connected")
        with self._lock:
            self._callbacks[channel] = callback
            self._pubsub.subscribe(**{channel: self._on_message})
            if self._listener_thread is None or not self._listener_thread.is_alive():
                self._listener_thread = threading.Thread(
                    target=self._listen_loop,
                    daemon=True,
                    name="drone-redis-listener",
                )
                self._listener_thread.start()

    def unsubscribe(self, channel: str) -> None:
        if self._pubsub is None:
            return
        with self._lock:
            self._callbacks.pop(channel, None)
            self._pubsub.unsubscribe(channel)

    def _on_message(self, message: dict) -> None:
        if message["type"] != "message":
            return
        channel = message["channel"]
        if isinstance(channel, bytes):
            channel = channel.decode("utf-8")
        callback = self._callbacks.get(channel)
        if callback is not None:
            callback(message["data"])

    def _listen_loop(self) -> None:
        retry_delay = 0.5
        while not self._stop_event.is_set():
            try:
                if self._pubsub is not None:
                    self._pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                    retry_delay = 0.5
                else:
                    self._stop_event.wait(retry_delay)
            except (redis.ConnectionError, redis.TimeoutError, OSError) as exc:
                logger.warning("Redis listener error: %s. Retrying in %.1fs", exc, retry_delay)
                self._stop_event.wait(retry_delay)
                retry_delay = min(retry_delay * 2, self._max_retry_delay)
            except Exception:
                if self._stop_event.is_set():
                    break
                logger.exception("Unexpected error in Redis listener")
                self._stop_event.wait(retry_delay)
                retry_delay = min(retry_delay * 2, self._max_retry_delay)
