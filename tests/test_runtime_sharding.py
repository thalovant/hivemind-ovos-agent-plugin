"""Safe runtime sharding through the documented AgentProtocol bus seam."""

from unittest.mock import MagicMock

import pytest
from hivemind_bus_client.message import HiveMessageType
from hivemind_plugin_manager.protocols import ClientCallbacks
from ovos_bus_client.message import Message
from ovos_utils.fakebus import FakeBus

from hivemind_ovos_agent_plugin import (
    OVOSAgentProtocol,
    _RuntimeMessageBusClient,
)


def _sharded_agent():
    agent = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
    first = FakeBus()
    second = FakeBus()
    agent.bus = first
    agent.config = {}
    agent._owned_bus = None
    agent._owned_buses = ()
    agent._runtime_buses = {"runtime-a": first, "runtime-b": second}
    agent.hm_protocol = MagicMock()
    agent.hm_protocol.clients = {}
    agent.callbacks = ClientCallbacks()
    agent.register_bus_handlers()
    return agent, first, second


def _peer_for(agent, expected_bus):
    for index in range(10_000):
        peer = f"VoiceSatellite::{index}"
        if agent._runtime_bus_for_key(peer) is expected_bus:
            return peer
    raise AssertionError("rendezvous hashing did not select expected shard")


def _client(peer):
    client = MagicMock()
    client.peer = peer
    return client


class TestShardConfiguration:
    def test_single_runtime_remains_the_default(self, agent):
        assert agent._configured_runtime_shards() == (
            ("default", "127.0.0.1", 8181),
        )

    def test_explicit_runtime_shards_preserve_stable_order(self, agent):
        agent.config = {
            "runtime_shards": [
                {"id": "runtime-a", "host": "runtime-a.internal"},
                {"id": "runtime-b", "host": "runtime-b.internal", "port": 8282},
            ]
        }

        assert agent._configured_runtime_shards() == (
            ("runtime-a", "runtime-a.internal", 8181),
            ("runtime-b", "runtime-b.internal", 8282),
        )

    def test_legacy_connection_pool_is_rejected(self, agent):
        agent.config = {"pool_size": 2}

        with pytest.raises(ValueError, match="runtime_shards"):
            agent._configured_runtime_shards()

    def test_repeated_broadcast_endpoint_is_rejected(self, agent):
        agent.config = {
            "runtime_shards": [
                {"id": "runtime-a", "host": "runtime.internal"},
                {"id": "runtime-b", "host": "runtime.internal"},
            ]
        }

        with pytest.raises(ValueError, match="endpoints must be unique"):
            agent._configured_runtime_shards()

    def test_equivalent_dns_endpoint_aliases_are_rejected(self, agent):
        agent.config = {
            "runtime_shards": [
                {"id": "runtime-a", "host": "Runtime-A.Internal"},
                {"id": "runtime-b", "host": "runtime-a.internal."},
            ]
        }

        with pytest.raises(ValueError, match="endpoints must be unique"):
            agent._configured_runtime_shards()

    @pytest.mark.parametrize(
        "runtime_shards, error",
        [
            ([], "non-empty list"),
            ([{"id": "runtime-a"}], "requires non-empty id and host"),
            (
                [
                    {"id": "runtime-a", "host": "a.internal"},
                    {"id": "runtime-a", "host": "b.internal"},
                ],
                "duplicate runtime shard id",
            ),
            ([{"id": "runtime-a", "host": "a.internal", "port": 0}], "between"),
        ],
    )
    def test_invalid_shard_configuration_fails_closed(
        self, agent, runtime_shards, error
    ):
        agent.config = {"runtime_shards": runtime_shards}

        with pytest.raises(ValueError, match=error):
            agent._configured_runtime_shards()


class TestShardOwnership:
    def test_same_peer_always_selects_same_runtime(self):
        agent, first, _ = _sharded_agent()
        peer = _peer_for(agent, first)

        assert agent.get_bus(_client(peer)) is first
        assert agent.get_bus(_client(peer)) is first

    def test_selection_does_not_depend_on_endpoint_order(self):
        agent, first, second = _sharded_agent()
        peers = [f"VoiceSatellite::{index}" for index in range(100)]
        expected = {
            peer: agent._runtime_bus_for_key(peer)
            for peer in peers
        }
        agent._runtime_buses = {
            "runtime-b": second,
            "runtime-a": first,
        }

        assert {
            peer: agent._runtime_bus_for_key(peer)
            for peer in peers
        } == expected

    def test_clients_are_distributed_across_all_shards(self):
        agent, first, second = _sharded_agent()
        selected = {
            agent._runtime_bus_for_key(f"VoiceSatellite::{index}")
            for index in range(100)
        }

        assert selected == {first, second}

    def test_four_hundred_clients_are_distributed_across_sixteen_shards(self):
        agent, _, _ = _sharded_agent()
        agent._runtime_buses = {
            f"runtime-{index}": FakeBus()
            for index in range(16)
        }
        counts = {bus: 0 for bus in agent._runtime_buses.values()}

        for index in range(400):
            bus = agent._runtime_bus_for_key(f"VoiceSatellite::{index}")
            counts[bus] += 1

        assert min(counts.values()) > 0
        assert max(counts.values()) - min(counts.values()) <= 20

    def test_unavailable_selected_shard_fails_without_remapping(self, agent):
        first = MagicMock()
        second = MagicMock()
        first.connected_event.is_set.return_value = False
        second.connected_event.is_set.return_value = True
        agent.bus = first
        agent._owned_bus = first
        agent._owned_buses = (first, second)
        agent._runtime_buses = {"runtime-a": first, "runtime-b": second}
        peer = _peer_for_raw_assignment(agent, first)

        with pytest.raises(ConnectionError, match="runtime-a"):
            agent.get_bus(_client(peer))

        second.connected_event.is_set.assert_not_called()

    def test_wait_for_bus_requires_every_shard_transport(self, agent):
        first = MagicMock(spec=_RuntimeMessageBusClient)
        second = MagicMock(spec=_RuntimeMessageBusClient)
        first._wait_for_live_transport.return_value = True
        second._wait_for_live_transport.return_value = False
        agent._owned_bus = first
        agent._owned_buses = (first, second)

        assert agent.wait_for_bus(0.5) is False
        first._wait_for_live_transport.assert_called_once()
        second._wait_for_live_transport.assert_called_once()

    def test_public_answer_query_uses_client_shard(self, monkeypatch):
        agent, first, _ = _sharded_agent()
        peer = _peer_for(agent, first)
        observed = {}

        def stream(utterance, lang, **kwargs):
            observed.update(
                utterance=utterance,
                lang=lang,
                bus=kwargs.get("bus"),
            )
            yield "answer"
            yield None

        monkeypatch.setattr(agent, "_stream_query", stream)

        assert list(agent.answer_query("hello", "en-US", _client(peer))) == [
            "answer",
            None,
        ]
        assert observed == {
            "utterance": "hello",
            "lang": "en-US",
            "bus": first,
        }


def _peer_for_raw_assignment(agent, expected_bus):
    owned = agent._owned_buses
    agent._owned_buses = ()
    agent._owned_bus = None
    try:
        return _peer_for(agent, expected_bus)
    finally:
        agent._owned_buses = owned
        agent._owned_bus = owned[0]


class TestShardReplyRouting:
    def test_reply_only_leaves_the_runtime_that_owns_the_peer(self):
        agent, first, second = _sharded_agent()
        peer = _peer_for(agent, first)
        client = _client(peer)
        agent.hm_protocol.clients = {peer: client}
        reply = Message(
            "speak",
            {"utterance": "hello"},
            {"destination": peer, "query_id": "query-1"},
        )

        agent.handle_internal_mycroft(reply.serialize(), bus=second)
        agent.handle_internal_mycroft(reply.serialize(), bus=first)

        client.send.assert_called_once()
        sent = client.send.call_args.args[0]
        assert sent.msg_type == HiveMessageType.BUS
        assert sent.payload.data["utterance"] == "hello"

    def test_correlated_duplicate_reply_is_suppressed(self):
        agent, first, _ = _sharded_agent()
        peer = _peer_for(agent, first)
        client = _client(peer)
        agent.hm_protocol.clients = {peer: client}
        reply = Message(
            "speak",
            {"utterance": "hello"},
            {
                "destination": peer,
                "query_id": "query-1",
                "session": {"session_id": "query-1"},
            },
        )

        agent.handle_internal_mycroft(reply.serialize(), bus=first)
        agent.handle_internal_mycroft(reply.serialize(), bus=first)

        client.send.assert_called_once()

    def test_uncorrelated_equal_messages_are_not_suppressed(self):
        agent, first, _ = _sharded_agent()
        peer = _peer_for(agent, first)
        client = _client(peer)
        agent.hm_protocol.clients = {peer: client}
        reply = Message(
            "speak",
            {"utterance": "notification"},
            {"destination": peer},
        )

        agent.handle_internal_mycroft(reply.serialize(), bus=first)
        agent.handle_internal_mycroft(reply.serialize(), bus=first)

        assert client.send.call_count == 2

    def test_broadcast_only_reaches_clients_owned_by_originating_shard(self):
        agent, first, second = _sharded_agent()
        first_peer = _peer_for(agent, first)
        second_peer = _peer_for(agent, second)
        first_client = _client(first_peer)
        second_client = _client(second_peer)
        agent.hm_protocol.clients = {
            first_peer: first_client,
            second_peer: second_client,
        }
        broadcast = Message(
            "hive.send.downstream",
            {
                "msg_type": HiveMessageType.BROADCAST,
                "payload": {"event": "ready"},
            },
        )

        agent.handle_send(broadcast, bus=first)

        first_client.send.assert_called_once()
        second_client.send.assert_not_called()
