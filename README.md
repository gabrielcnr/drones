# DRONE

**Distributed Reactive Orchestration Nodes Environment**

[![build](https://github.com/gabrielcnr/drones/actions/workflows/main.yml/badge.svg)](https://github.com/gabrielcnr/drones/actions/workflows/main.yml)
[![PyPI](https://img.shields.io/pypi/v/drones.svg)](https://pypi.org/project/drones/)
[![Python](https://img.shields.io/pypi/pyversions/drones.svg)](https://pypi.org/project/drones/)

DRONE is a Python framework built on top of [Atom](https://atom.readthedocs.io/)
that turns reactive object graphs into *distributed* ones. When a member value
changes in one process, it is automatically pushed through a message broker to
all subscribers on any machine — triggering their local Atom observers as if the
change had happened locally.

## Installation

```bash
pip install drones
```

A running Redis instance is required for the default `RedisBroker`.

## Quick start

### Publisher (process A)

```python
from atom.api import Atom, Float
from drones import Drone, RedisBroker, drone


@drone
class MarketFeed(Atom):
    price = Float().tag(drone_publish="price")


Drone.bind(pubsub_channel="my-app", broker=RedisBroker("redis://localhost:6379"))

feed = MarketFeed()
feed.price = 185.0  # published to every subscriber on the "my-app" channel
```

### Subscriber (process B)

```python
from atom.api import Atom, Float, Int, observe
from drones import Drone, RedisBroker, drone


@drone
class Portfolio(Atom):
    price = Float().tag(drone_node="price")  # fed remotely
    quantity = Int(10)
    total = Float()

    @observe("price")
    def _refresh(self, change):
        self.total = self.price * self.quantity


Drone.bind(pubsub_channel="my-app", broker=RedisBroker("redis://localhost:6379"))

portfolio = Portfolio()
# portfolio.price is updated whenever MarketFeed.price changes in process A,
# firing _refresh through Atom's native observation.
```

See the [`examples/`](examples/) directory for a runnable publisher/subscriber
pair, and [`SPEC.md`](SPEC.md) for the full design specification.

## Core concepts

- **`@drone`** — class decorator that instruments an `Atom` subclass.
- **`drone_publish="name"`** — tag a member so its changes are published under `name`.
- **`drone_node="name"`** — tag a member so it receives updates for `name`.
- **`drone_scope="instance"`** — restrict delivery to the instance whose
  `drone_id` matches (node name on the wire becomes `name.<drone_id>`).
- **Broker** — pluggable transport (`DroneBroker` ABC); `RedisBroker` ships by default.
- **Serializer** — pluggable (`DroneSerializer` ABC); `PickleSerializer` is the default.

## Development

This project uses [pixi](https://pixi.sh/) for local development.

```bash
pixi run test        # run the test suite
pixi run lint        # ruff check
pixi run format      # ruff format
pixi run typecheck   # mypy
```

## Releasing

Versions are derived from git tags via
[`hatch-vcs`](https://github.com/ofek/hatch-vcs). To publish a release, push a
tag and the GitHub Actions workflow builds and publishes to PyPI using
[trusted publishing](https://docs.pypi.org/trusted-publishers/):

```bash
git tag v0.1.0
git push origin v0.1.0
```

## License

[MIT](LICENSE)
