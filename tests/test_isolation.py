"""Client isolation invariant: a client must only receive messages targeted at it."""

from hivemind_bus_client.message import HiveMessageType
from ovos_bus_client.message import Message


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

    def test_forwarded_message_omits_registered_null_session_fields(self, agent, make_client):
        alice = make_client("ws://alice")
        agent.hm_protocol.clients = {"ws://alice": alice}
        message = Message(
            "speak",
            {"utterance": "hi"},
            {
                "destination": "ws://alice",
                "session": {
                    "session_id": "session-1",
                    "lang": None,
                    "persona_id": None,
                    "site_id": "kitchen",
                    "future_field": None,
                },
            },
        )

        agent.handle_internal_mycroft(message.serialize())

        session = alice.send.call_args[0][0].payload.context["session"]
        assert session == {
            "session_id": "session-1",
            "site_id": "kitchen",
            "future_field": None,
        }
