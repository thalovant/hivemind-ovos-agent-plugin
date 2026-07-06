"""Client isolation invariant: a client must only receive messages targeted at it."""

from ovos_bus_client.message import Message
from hivemind_bus_client.message import HiveMessageType


def _ovos_internal(msg_type, destination=None, data=None):
    """Build the serialized JSON that handle_internal_mycroft expects."""
    msg = Message(msg_type, data or {}, {"destination": destination} if destination is not None else {})
    return msg.serialize()


class TestClientIsolation:
    def test_message_addressed_to_one_client_only_reaches_that_client(self, agent, make_client):
        alice = make_client("ws://alice")
        bob = make_client("ws://bob")
        agent.hm_protocol.clients = {"ws://alice": alice, "ws://bob": bob}

        agent.handle_internal_mycroft(_ovos_internal("speak", destination="ws://alice", data={"utterance": "hi"}))

        alice.send.assert_called_once()
        bob.send.assert_not_called()

    def test_destination_can_be_a_list(self, agent, make_client):
        alice = make_client("ws://alice")
        bob = make_client("ws://bob")
        carol = make_client("ws://carol")
        agent.hm_protocol.clients = {"ws://alice": alice, "ws://bob": bob, "ws://carol": carol}

        agent.handle_internal_mycroft(_ovos_internal("speak", destination=["ws://alice", "ws://bob"]))

        alice.send.assert_called_once()
        bob.send.assert_called_once()
        carol.send.assert_not_called()

    def test_message_with_no_destination_is_dropped(self, agent, make_client):
        alice = make_client("ws://alice")
        bob = make_client("ws://bob")
        agent.hm_protocol.clients = {"ws://alice": alice, "ws://bob": bob}

        agent.handle_internal_mycroft(_ovos_internal("speak", destination=None, data={"utterance": "hi"}))

        alice.send.assert_not_called()
        bob.send.assert_not_called()

    def test_message_addressed_to_unknown_peer_is_dropped(self, agent, make_client):
        alice = make_client("ws://alice")
        agent.hm_protocol.clients = {"ws://alice": alice}

        agent.handle_internal_mycroft(_ovos_internal("speak", destination="ws://stranger"))

        alice.send.assert_not_called()

    def test_message_addressed_to_stale_peer_is_dropped_without_raising(self, agent, make_client):
        alice = make_client("ws://alice")
        alice.send.side_effect = RuntimeError("closed")
        agent.hm_protocol.clients = {"ws://alice": alice}

        agent.handle_internal_mycroft(_ovos_internal("speak", destination="ws://alice"))

        alice.send.assert_called_once()
        assert agent.hm_protocol.clients == {}

    def test_stale_peer_does_not_block_other_targets(self, agent, make_client):
        alice = make_client("ws://alice")
        bob = make_client("ws://bob")
        alice.send.side_effect = RuntimeError("closed")
        agent.hm_protocol.clients = {"ws://alice": alice, "ws://bob": bob}

        agent.handle_internal_mycroft(_ovos_internal("speak", destination=["ws://alice", "ws://bob"]))

        alice.send.assert_called_once()
        bob.send.assert_called_once()
        assert agent.hm_protocol.clients == {"ws://bob": bob}

    def test_forwarded_message_is_wrapped_as_bus_hivemessage(self, agent, make_client):
        alice = make_client("ws://alice")
        agent.hm_protocol.clients = {"ws://alice": alice}

        agent.handle_internal_mycroft(_ovos_internal("speak", destination="ws://alice"))

        sent = alice.send.call_args[0][0]
        assert sent.msg_type == HiveMessageType.BUS
        # payload is the Mycroft Message
        assert sent.payload.msg_type == "speak"

    def test_forwarded_message_marks_source_as_hive(self, agent, make_client):
        """Downstream relays must rewrite source so the client sees it came from the hive."""
        alice = make_client("ws://alice")
        agent.hm_protocol.clients = {"ws://alice": alice}

        agent.handle_internal_mycroft(_ovos_internal("speak", destination="ws://alice"))

        sent = alice.send.call_args[0][0]
        assert sent.payload.context.get("source") == "hive"
