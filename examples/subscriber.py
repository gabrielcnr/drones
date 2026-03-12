"""
Process B: Portfolio that reacts to remote market data.

Subscribes to price and volume from the MarketFeed publisher,
computes a total position value, and republishes a risk score.

Usage:
    python examples/subscriber.py
"""

import time

from atom.api import Atom, Float, Int, Str, observe

from drone import Drone, RedisBroker, drone


@drone
class Portfolio(Atom):
    name = Str()
    quantity = Int()

    # These are fed remotely from MarketFeed
    price = Float().tag(drone_node="price")
    volume = Float().tag(drone_node="volume")

    # Computed locally
    total = Float()
    risk = Float().tag(drone_publish="risk")

    @observe("price")
    def _on_price(self, change):
        self.total = self.price * self.quantity
        # Simple risk: higher price -> higher risk score
        self.risk = round(self.total / 10000, 4)
        print(
            f"  price={self.price:<10.2f} "
            f"total={self.total:<12.2f} "
            f"risk={self.risk:<8.4f}"
        )

    @observe("volume")
    def _on_volume(self, change):
        print(f"  volume={self.volume:<10.0f}")


def main():
    broker = RedisBroker("redis://localhost:6379")
    Drone.bind(pubsub_channel="drone-example", broker=broker)

    portfolio = Portfolio(name="My Portfolio", quantity=100)

    print("📊 Portfolio subscriber started (Ctrl+C to stop)")
    print(f"   Name: {portfolio.name}")
    print(f"   Quantity: {portfolio.quantity}")
    print(f"   Channel: drone-example")
    print()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        Drone.unbind()


if __name__ == "__main__":
    main()
