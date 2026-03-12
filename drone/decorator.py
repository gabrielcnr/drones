from __future__ import annotations

import logging
from typing import Any

from atom.api import Atom

from drone.core import Drone

logger = logging.getLogger(__name__)

# Store drone_id per instance by object id.
# Entries are cleaned up in the __init__ wrapper (weakref not available on plain Atom).
_drone_ids: dict[int, str | None] = {}


def get_drone_id(instance: Atom) -> str | None:
    return _drone_ids.get(id(instance))


def _ensure_weakrefable(cls: type[Atom]) -> type[Atom]:
    """Ensure the class supports weak references by adding __weakref__ to __slots__."""
    # Check if already weakrefable by walking the MRO
    for klass in cls.__mro__:
        if "__weakref__" in getattr(klass, "__slots__", ()) or "__weakref__" in getattr(
            klass, "__dict__", {}
        ):
            return cls

    # Create a subclass with __weakref__ in __slots__
    new_cls = type(cls.__name__, (cls,), {"__slots__": ("__weakref__",)})
    new_cls.__module__ = cls.__module__
    new_cls.__qualname__ = cls.__qualname__
    return new_cls


def drone(cls: type[Atom]) -> type[Atom]:
    if not issubclass(cls, Atom):
        raise TypeError(f"@drone can only be applied to Atom subclasses, got {cls}")

    publish_members: dict[str, str] = {}  # member_name -> node_name
    subscribe_members: dict[str, tuple[str, bool]] = {}  # member_name -> (node_name, is_scoped)

    for member_name, member in cls.members().items():
        metadata = member.metadata
        if metadata is None:
            continue
        if "drone_publish" in metadata:
            publish_members[member_name] = metadata["drone_publish"]
        if "drone_node" in metadata:
            is_scoped = metadata.get("drone_scope") == "instance"
            subscribe_members[member_name] = (metadata["drone_node"], is_scoped)

    if not publish_members and not subscribe_members:
        return cls

    # Make the class weakref-compatible (needed for instance registry)
    cls = _ensure_weakrefable(cls)

    # Install publish observers
    for member_name, node_name in publish_members.items():
        _install_publisher(cls, member_name, node_name)

    # Store subscription info on the class
    cls._drone_subscribe_members = subscribe_members
    cls._drone_publish_members = publish_members

    # Wrap __init__ to register instances
    original_init = cls.__init__

    def __init__(self: Any, *args: Any, drone_id: str | None = None, **kwargs: Any) -> None:
        original_init(self, *args, **kwargs)
        _drone_ids[id(self)] = drone_id
        for member_name, (node_name, is_scoped) in subscribe_members.items():
            if is_scoped and drone_id:
                scoped_node = f"{node_name}.{drone_id}"
                Drone.register_subscriber(scoped_node, member_name, self)
            else:
                Drone.register_subscriber(node_name, member_name, self)

    cls.__init__ = __init__

    return cls


def _install_publisher(cls: type[Atom], member_name: str, node_name: str) -> None:
    member = cls.members()[member_name]

    def _publish_observer(change: dict) -> None:
        instance = change["object"]
        value = change["value"]
        drone_id = _drone_ids.get(id(instance))
        is_scoped = member.metadata and member.metadata.get("drone_scope") == "instance"
        if is_scoped and drone_id:
            effective_node = f"{node_name}.{drone_id}"
        else:
            effective_node = node_name
        Drone.publish_value(effective_node, value)

    member.add_static_observer(_publish_observer)
