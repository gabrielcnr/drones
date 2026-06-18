from __future__ import annotations

import pytest
from atom.api import Atom, Float, Int, observe

from drones import Drone, DroneEnvelope, PickleSerializer, drone
from tests.fake_broker import FakeBroker


@pytest.fixture(autouse=True)
def _drone_env():
    """Bind Drone with a FakeBroker for each test, unbind after."""
    broker = FakeBroker()
    Drone.bind(pubsub_channel="test-channel", broker=broker)
    yield broker
    Drone.unbind()


def broker(drone_env=None) -> FakeBroker:
    """Helper to get the current broker from the fixture."""
    return Drone._get_env().broker


class TestPublish:
    def test_publish_fires_on_member_change(self, _drone_env):
        @drone
        class Source(Atom):
            x = Int().tag(drone_publish="x")

        s = Source()
        s.x = 42

        b = _drone_env
        assert len(b.published) == 1
        channel, data = b.published[0]
        assert channel == "test-channel"
        env = PickleSerializer().deserialize(data)
        assert isinstance(env, DroneEnvelope)
        assert env.node == "x"
        assert env.value == 42
        assert env.seq == 1

    def test_publish_increments_seq(self, _drone_env):
        @drone
        class Source(Atom):
            x = Int().tag(drone_publish="x")

        s = Source()
        s.x = 1
        s.x = 2
        s.x = 3

        b = _drone_env
        seqs = [PickleSerializer().deserialize(d).seq for _, d in b.published]
        assert seqs == [1, 2, 3]

    def test_publish_scoped_with_drone_id(self, _drone_env):
        @drone
        class Source(Atom):
            val = Float().tag(drone_publish="val", drone_scope="instance")

        s = Source(drone_id="abc")
        s.val = 9.5

        env = PickleSerializer().deserialize(_drone_env.published[0][1])
        assert env.node == "val.abc"


class TestSubscribe:
    def test_subscribe_receives_remote_value(self, _drone_env):
        @drone
        class Receiver(Atom):
            x = Int().tag(drone_node="x")

        r = Receiver()
        assert r.x == 0

        # Simulate remote publish (different source_id)
        serializer = PickleSerializer()
        env = DroneEnvelope(node="x", value=99, source_id="remote-1")
        _drone_env.publish("test-channel", serializer.serialize(env))

        assert r.x == 99

    def test_subscribe_broadcast_multiple_instances(self, _drone_env):
        @drone
        class Listener(Atom):
            val = Float().tag(drone_node="val_multi")

        a = Listener()
        b = Listener()

        serializer = PickleSerializer()
        env = DroneEnvelope(node="val_multi", value=3.14, source_id="remote-2")
        _drone_env.publish("test-channel", serializer.serialize(env))

        assert a.val == 3.14
        assert b.val == 3.14

    def test_subscribe_scoped_only_matching_id(self, _drone_env):
        @drone
        class Scoped(Atom):
            z = Int().tag(drone_node="z_scoped", drone_scope="instance")

        s1 = Scoped(drone_id="one")
        s2 = Scoped(drone_id="two")

        serializer = PickleSerializer()
        env = DroneEnvelope(node="z_scoped.one", value=10, source_id="remote-3")
        _drone_env.publish("test-channel", serializer.serialize(env))

        assert s1.z == 10
        assert s2.z == 0  # not targeted

    def test_self_echo_suppression(self, _drone_env):
        @drone
        class Echo(Atom):
            x = Int().tag(drone_node="echo_x")

        e = Echo()
        source_id = Drone._get_env().source_id

        serializer = PickleSerializer()
        env = DroneEnvelope(node="echo_x", value=5, source_id=source_id)
        _drone_env.publish("test-channel", serializer.serialize(env))

        assert e.x == 0  # should NOT have been set (same source)


class TestChaining:
    def test_subscribe_triggers_local_observer(self, _drone_env):
        @drone
        class Chained(Atom):
            price = Float().tag(drone_node="chain_price")
            quantity = Int(10)
            total = Float()

            @observe("price")
            def _refresh(self, change):
                self.total = self.price * self.quantity

        c = Chained()

        serializer = PickleSerializer()
        env = DroneEnvelope(node="chain_price", value=5.0, source_id="remote-4")
        _drone_env.publish("test-channel", serializer.serialize(env))

        assert c.price == 5.0
        assert c.total == 50.0

    def test_subscribe_and_republish(self, _drone_env):
        @drone
        class ChainedPublisher(Atom):
            input_val = Float().tag(drone_node="chain_in")
            output_val = Float().tag(drone_publish="chain_out")

            @observe("input_val")
            def _compute(self, change):
                self.output_val = self.input_val * 2

        cp = ChainedPublisher()

        serializer = PickleSerializer()
        env = DroneEnvelope(node="chain_in", value=7.0, source_id="remote-5")
        _drone_env.publish("test-channel", serializer.serialize(env))

        assert cp.input_val == 7.0
        assert cp.output_val == 14.0

        # Verify the republished message exists
        # First message is the incoming one, second is the outgoing chain_out
        assert len(_drone_env.published) == 2
        out_env = serializer.deserialize(_drone_env.published[1][1])
        assert out_env.node == "chain_out"
        assert out_env.value == 14.0


class TestWeakRefCleanup:
    def test_dead_instance_is_cleaned_up(self, _drone_env):
        @drone
        class Ephemeral(Atom):
            x = Int().tag(drone_node="ephemeral_x")

        e = Ephemeral()
        del e  # should be garbage collected

        # Publishing should not raise
        serializer = PickleSerializer()
        env = DroneEnvelope(node="ephemeral_x", value=1, source_id="remote-6")
        _drone_env.publish("test-channel", serializer.serialize(env))


class TestDecoratorErrors:
    def test_non_atom_raises(self):
        with pytest.raises(TypeError, match="Atom subclasses"):

            @drone
            class NotAnAtom:
                pass

    def test_no_tags_returns_class_unchanged(self):
        @drone
        class Plain(Atom):
            x = Int()

        assert not hasattr(Plain, "_drone_subscribe_members")


class TestEnvelope:
    def test_roundtrip(self):
        serializer = PickleSerializer()
        env = DroneEnvelope(node="test", value={"key": [1, 2, 3]}, seq=5, source_id="abc")
        data = serializer.serialize(env)
        restored = serializer.deserialize(data)
        assert restored.node == "test"
        assert restored.value == {"key": [1, 2, 3]}
        assert restored.seq == 5
        assert restored.source_id == "abc"
