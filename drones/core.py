from __future__ import annotations

import atexit
import contextlib
import logging
import threading
import uuid
import weakref
from dataclasses import dataclass, field
from typing import Any

from drones.broker import DroneBroker
from drones.envelope import DroneEnvelope
from drones.serializer import DroneSerializer
from drones.serializer.pickle import PickleSerializer

logger = logging.getLogger(__name__)


@dataclass
class _SubscriptionEntry:
    member_name: str
    broadcast_refs: list[weakref.ref] = field(default_factory=list)
    scoped_refs: dict[str, list[weakref.ref]] = field(default_factory=dict)


class _DroneEnvironment:
    def __init__(self) -> None:
        self.pubsub_channel: str | None = None
        self.broker: DroneBroker | None = None
        self.serializer: DroneSerializer = PickleSerializer()
        self.source_id: str = uuid.uuid4().hex
        self._seq: int = 0
        self._seq_lock = threading.Lock()
        self._subscriptions: dict[str, _SubscriptionEntry] = {}
        self._sub_lock = threading.Lock()
        self._bound = False
        self._error_count = 0

    @property
    def bound(self) -> bool:
        return self._bound

    def bind(
        self,
        pubsub_channel: str,
        broker: DroneBroker,
        serializer: DroneSerializer | None = None,
    ) -> None:
        if self._bound:
            raise RuntimeError("Drone is already bound. Call Drone.unbind() first.")
        self.pubsub_channel = pubsub_channel
        self.broker = broker
        if serializer is not None:
            self.serializer = serializer
        self.source_id = uuid.uuid4().hex
        self._seq = 0
        self._error_count = 0
        self.broker.connect()
        self.broker.subscribe(pubsub_channel, self._on_message)
        self._bound = True
        atexit.register(self.unbind)
        logger.info("Drone bound to channel '%s'", pubsub_channel)

    def unbind(self) -> None:
        if not self._bound:
            return
        self._bound = False
        if self.broker is not None:
            try:
                self.broker.disconnect()
            except Exception:
                logger.exception("Error disconnecting broker")
        with self._sub_lock:
            self._subscriptions.clear()
        self.pubsub_channel = None
        self.broker = None
        with contextlib.suppress(Exception):
            atexit.unregister(self.unbind)
        logger.info("Drone unbound")

    def _next_seq(self) -> int:
        with self._seq_lock:
            self._seq += 1
            return self._seq

    def publish_value(self, node: str, value: Any) -> None:
        if not self._bound or self.broker is None or self.pubsub_channel is None:
            logger.warning("Drone is not bound; skipping publish for node '%s'", node)
            return
        envelope = DroneEnvelope(
            node=node,
            value=value,
            seq=self._next_seq(),
            source_id=self.source_id,
        )
        try:
            data = self.serializer.serialize(envelope)
            self.broker.publish(self.pubsub_channel, data)
        except Exception:
            self._error_count += 1
            logger.exception("Failed to publish node '%s'", node)

    def register_subscriber(
        self,
        node: str,
        member_name: str,
        instance: Any,
        drone_id: str | None = None,
    ) -> None:
        ref = weakref.ref(instance, lambda r: self._cleanup_ref(node, r, drone_id))
        with self._sub_lock:
            entry = self._subscriptions.setdefault(
                node, _SubscriptionEntry(member_name=member_name)
            )
            if drone_id is None:
                entry.broadcast_refs.append(ref)
            else:
                entry.scoped_refs.setdefault(drone_id, []).append(ref)

    def unregister_subscriber(self, instance: Any) -> None:
        with self._sub_lock:
            for entry in self._subscriptions.values():
                entry.broadcast_refs = [r for r in entry.broadcast_refs if r() is not instance]
                for scope_id in list(entry.scoped_refs):
                    entry.scoped_refs[scope_id] = [
                        r for r in entry.scoped_refs[scope_id] if r() is not instance
                    ]
                    if not entry.scoped_refs[scope_id]:
                        del entry.scoped_refs[scope_id]

    def _cleanup_ref(
        self,
        node: str,
        ref: weakref.ref,
        drone_id: str | None,
    ) -> None:
        with self._sub_lock:
            entry = self._subscriptions.get(node)
            if entry is None:
                return
            if drone_id is None:
                with contextlib.suppress(ValueError):
                    entry.broadcast_refs.remove(ref)
            else:
                refs = entry.scoped_refs.get(drone_id, [])
                with contextlib.suppress(ValueError):
                    refs.remove(ref)
                if not refs:
                    entry.scoped_refs.pop(drone_id, None)

    def _on_message(self, raw: bytes) -> None:
        try:
            envelope: DroneEnvelope = self.serializer.deserialize(raw)
        except Exception:
            self._error_count += 1
            logger.exception("Failed to deserialize message")
            return

        if envelope.source_id == self.source_id:
            return

        node = envelope.node
        with self._sub_lock:
            entry = self._subscriptions.get(node)
            if entry is None:
                return
            member_name = entry.member_name
            targets: list[weakref.ref] = list(entry.broadcast_refs)
            # Check scoped: node might be "pnl.us-equities", parse scope id
            parts = node.rsplit(".", 1)
            if len(parts) == 2:
                scope_id = parts[1]
                base_entry = self._subscriptions.get(parts[0])
                if base_entry is not None:
                    member_name = base_entry.member_name
                    targets.extend(base_entry.scoped_refs.get(scope_id, []))

        for ref in targets:
            instance = ref()
            if instance is not None:
                try:
                    setattr(instance, member_name, envelope.value)
                except Exception:
                    logger.exception(
                        "Failed to set member '%s' on %r from node '%s'",
                        member_name,
                        instance,
                        node,
                    )

    @property
    def stats(self) -> dict[str, Any]:
        return {
            "error_count": self._error_count,
            "source_id": self.source_id,
            "seq": self._seq,
            "subscriptions": list(self._subscriptions.keys()),
            "bound": self._bound,
        }


# Global singleton
_env = _DroneEnvironment()


class _DroneMeta(type):
    @property
    def stats(cls) -> dict[str, Any]:
        return _env.stats


class Drone(metaclass=_DroneMeta):
    @staticmethod
    def bind(
        pubsub_channel: str,
        broker: DroneBroker,
        serializer: DroneSerializer | None = None,
    ) -> None:
        _env.bind(pubsub_channel, broker, serializer)

    @staticmethod
    def unbind() -> None:
        _env.unbind()

    @staticmethod
    def publish_value(node: str, value: Any) -> None:
        _env.publish_value(node, value)

    @staticmethod
    def register_subscriber(
        node: str,
        member_name: str,
        instance: Any,
        drone_id: str | None = None,
    ) -> None:
        _env.register_subscriber(node, member_name, instance, drone_id)

    @staticmethod
    def unregister_subscriber(instance: Any) -> None:
        _env.unregister_subscriber(instance)

    @staticmethod
    def is_bound() -> bool:
        return _env.bound

    @staticmethod
    def _get_env() -> _DroneEnvironment:
        return _env
