"""
Process A: Market data feed publisher.

Simulates a market data feed that publishes random price ticks every second.

Usage:
    python examples/publisher.py
"""

import random
import time

from atom.api import Atom, Float, Str

from drones import Drone, RedisBroker, drone


@drone
class MarketFeed(Atom):
    symbol = Str()
    price = Float().tag(drone_publish="price")
    volume = Float().tag(drone_publish="volume")


def main():
    broker = RedisBroker("redis://localhost:6379")
    Drone.bind(pubsub_channel="drone-example", broker=broker)

    feed = MarketFeed(symbol="AAPL")
    price = 185.0

    print("📡 MarketFeed publisher started (Ctrl+C to stop)")
    print(f"   Symbol: {feed.symbol}")
    print("   Channel: drone-example")
    print()

    try:
        while True:
            # Random walk
            price += random.uniform(-1.5, 1.5)
            price = max(price, 1.0)
            volume = random.uniform(1000, 50000)

            feed.price = round(price, 2)
            feed.volume = round(volume, 0)

            print(f"  Published  price={feed.price:<10.2f} volume={feed.volume:<10.0f}")
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        Drone.unbind()


if __name__ == "__main__":
    main()
