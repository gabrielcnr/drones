# DRONE — Distributed Reactive Orchestration Nodes Environment

## Overview

DRONE is a Python framework built on top of [Atom](https://atom.readthedocs.io/en/latest/) that enables reactive graphs across processes and machines to communicate via a message broker. When a member value changes on one process, it is automatically pushed to all subscribers on any machine, triggering local Atom observers as if the change happened locally.

## Core Concepts

### Drone Environment
A logical grouping bound to a single pub/sub channel. All nodes within the same environment communicate on the same channel.

### Drone Node
A named value within the environment. A member tagged with `drone_publish='name'` publishes changes under that node name. A member tagged with `drone_node='name'` subscribes to updates for that node name.

### Broker
An abstracted message transport. The first implementation is Redis Pub/Sub. The broker interface is pluggable.

## User-Facing API

### Entry Point

```python
from drone import Drone, RedisBroker

broker = RedisBroker("redis://localhost:6379")
Drone.bind(pubsub_channel="my-app-channel", broker=broker)
```

This configures the global Drone environment. The `pubsub_channel` is the single Redis Pub/Sub channel used for all communication within this environment. The channel name is chosen by the user to avoid clashes between applications, teams, or environments.

### Declaring a Drone-Enabled Atom Class

```python
from atom.api import Atom, Int, Float, observe
from drone import drone

@drone
class MarketFeed(Atom):
    price = Float().tag(drone_publish='price')
    volume = Int().tag(drone_publish='volume')
```

The `@drone` decorator instruments the class so that:
- Members tagged with `drone_publish` automatically publish their new value to the environment channel whenever they change.
- Members tagged with `drone_node` automatically receive updates from the environment channel.

```python
@drone
class Portfolio(Atom):
    price = Float().tag(drone_node='price')       # subscribes to 'price' updates
    quantity = Int(10)
    total = Float()

    @observe('price', 'quantity')
    def _refresh_total(self, change):
        self.total = self.price * self.quantity
```

When `MarketFeed.price` changes in Process A, the new value is pushed via Redis, and `Portfolio.price` is set in Process B — triggering `_refresh_total` through Atom's native observation.

### Chaining: Subscribe and Republish

A class can both subscribe to remote nodes and publish its own computed values:

```python
@drone
class RiskEngine(Atom):
    price = Float().tag(drone_node='price')         # subscribes
    risk_score = Float().tag(drone_publish='risk')   # publishes

    @observe('price')
    def _compute_risk(self, change):
        self.risk_score = some_model(self.price)
```

### Multiple Instances

**Broadcast (default):** All instances of a class with `drone_node='x'` receive every update to node `'x'`.

```python
@drone
class Listener(Atom):
    x = Int().tag(drone_node='x')

a = Listener()
b = Listener()
# Both a.x and b.x update when 'x' is published
```

**Instance-scoped:** When `drone_scope='instance'` is set, only the instance whose `drone_id` matches the target receives the update.

```python
@drone
class Portfolio(Atom):
    pnl = Float().tag(drone_node='pnl', drone_scope='instance')

p1 = Portfolio(drone_id="us-equities")   # subscribes to 'pnl' targeted at 'us-equities'
p2 = Portfolio(drone_id="eu-equities")   # subscribes to 'pnl' targeted at 'eu-equities'
```

On the publishing side:

```python
@drone
class PnlCalculator(Atom):
    pnl = Float().tag(drone_publish='pnl')

calc = PnlCalculator(drone_id="us-equities")
calc.pnl = 1000.0  # only p1 receives this
```

When `drone_scope='instance'` is used, the node name on the wire becomes `'{node}.{drone_id}'` (e.g., `'pnl.us-equities'`).

## Wire Protocol

All communication happens on a single Redis Pub/Sub channel per environment.

### Message Envelope

```python
{
    "node": "price",                    # node name (str)
    "value": b"...",                    # pickled value (bytes)
    "timestamp": 1710000000.123456,     # time.time() at publish (float)
    "seq": 42,                          # per-publisher sequence number (int)
    "source_id": "abc123",              # unique publisher instance ID (str)
}
```

The full envelope is serialized with pickle for transport (since values are arbitrary Python objects). An alternative: the envelope is msgpack/JSON with the `value` field being independently pickled bytes — this allows metadata to remain inspectable. Decision: **envelope is pickle** for v1 simplicity, with a future option for structured envelope + pickled value.

### Metadata Fields

| Field        | Type    | Description |
|------------- |---------|-------------|
| `node`       | `str`   | The drone node name (e.g., `'price'`, `'pnl.us-equities'`) |
| `value`      | `Any`   | The new value (pickled within the envelope) |
| `timestamp`  | `float` | `time.time()` at the moment of publish |
| `seq`        | `int`   | Monotonically increasing sequence number per source |
| `source_id`  | `str`   | UUID of the publishing DroneNode instance (to avoid echo) |

## Broker Abstraction

```python
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
```

### RedisBroker

First concrete implementation. Uses `redis-py` with a background thread running the pub/sub listen loop.

```python
class RedisBroker(DroneBroker):
    def __init__(self, url: str = "redis://localhost:6379"):
        ...
```

- The subscriber loop runs in a daemon thread.
- On connection failure: logs a warning, retries with exponential backoff.
- On message receive: deserializes the envelope and dispatches to the Drone registry.

## Serialization

Pluggable via a `DroneSerializer` interface:

```python
class DroneSerializer(ABC):
    @abstractmethod
    def serialize(self, obj: Any) -> bytes: ...

    @abstractmethod
    def deserialize(self, data: bytes) -> Any: ...
```

Default: `PickleSerializer` (uses `pickle` with highest protocol).

Configured at bind time:

```python
Drone.bind(
    pubsub_channel="my-channel",
    broker=RedisBroker("redis://..."),
    serializer=PickleSerializer(),  # default
)
```

## The `@drone` Decorator

The `@drone` decorator does the following at class definition time:

1. Scans all members for `drone_publish` and `drone_node` tags.
2. For `drone_publish` members: installs a static Atom observer that, on every change, serializes and publishes the new value to the environment channel.
3. For `drone_node` members: registers the member in a global subscription registry so incoming messages for that node name are routed to the correct member on all (or targeted) instances.
4. Injects `drone_id` support: adds a `drone_id` parameter to `__init__` (or uses an existing one) for instance-scoped targeting.

## Instance Tracking

DRONE maintains a weak-reference registry of all live instances of `@drone`-decorated classes:

```
_registry: dict[str, dict[str, list[weakref]]]
# node_name -> { "broadcast": [instances...], "instance:{id}": [instances...] }
```

When a message arrives for node `'price'`:
1. Look up `'price'` in the registry.
2. For broadcast subscribers: set the member value on all live instances.
3. For instance-scoped subscribers: set only on matching `drone_id`.

Setting the member value triggers Atom's native observation — no additional machinery needed.

## Threading Model

- The broker subscriber runs in a **daemon background thread**.
- When a message arrives, the member value is set on the Atom instance **from the background thread**.
- Atom observers fire synchronously in that thread.
- If the user needs thread safety (e.g., GUI integration), they are responsible for dispatching to their main thread. A future version could provide a `dispatcher` hook.

## Error Handling

### Publish Errors
If the broker is unreachable when publishing:
- Log the error.
- Optionally fire a local `Drone.on_error` callback if registered.
- Do NOT raise — the local Atom state change still succeeds.

### Subscribe Errors
If the broker connection drops:
- The background thread retries with exponential backoff.
- While disconnected, remote members retain their last known value (stale).
- An error message is published to `{channel}:errors` when failure is detected.

### Deserialization Errors
If a message cannot be deserialized:
- Log the error with the raw message bytes.
- Skip the message.
- Increment an error counter accessible via `Drone.stats`.

## Lifecycle

```python
# Startup
Drone.bind(pubsub_channel="my-channel", broker=RedisBroker("redis://..."))

# ... application runs, instances are created/destroyed ...

# Shutdown
Drone.unbind()  # unsubscribes, disconnects broker, clears registry
```

`Drone.unbind()` is also registered as an `atexit` handler.

## Package Structure

```
drone/
├── __init__.py          # public API: Drone, drone, DroneBroker, DroneSerializer
├── core.py              # Drone singleton, registry, message dispatch
├── decorator.py         # @drone decorator implementation
├── broker/
│   ├── __init__.py      # DroneBroker ABC
│   └── redis.py         # RedisBroker implementation
├── serializer/
│   ├── __init__.py      # DroneSerializer ABC
│   └── pickle.py        # PickleSerializer (default)
└── envelope.py          # DroneEnvelope dataclass, packing/unpacking
```

## Dependencies

- `atom` — the reactive object model
- `redis` — for `RedisBroker`
- Python 3.10+

## Future Considerations (Out of Scope for v1)

- Redis Streams backend (for persistence, replay, late joiners)
- Async broker implementation (asyncio)
- Dispatcher hook for thread-safe GUI integration
- Node discovery / registry service
- Message filtering / transformation middleware
- Compression for large payloads
- Authentication / encryption on the wire
- `drone_group` tag for pub/sub topic partitioning within a channel
- Monitoring dashboard / metrics export
