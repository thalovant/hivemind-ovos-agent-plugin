"""Client isolation invariant: a client must only receive messages targeted at it."""

from unittest.mock import patch

import pytest
from ovos_bus_client.message import Message
from hivemind_bus_client.message import HiveMessageType
from hivemind_ovos_agent_plugin._metrics import (
    RUNTIME_BUS_CONTROL,
    RUNTIME_BUS_FALLBACK_COORDINATION,
    RUNTIME_BUS_OTHER,
    RUNTIME_BUS_PUBLIC_REPLY,
    RUNTIME_BUS_SKILL_LIFECYCLE,
    SKILL_HANDLER,
)


def _ovos_internal(msg_type, destination=None, data=None):
    """Build the serialized JSON that handle_internal_mycroft expects."""
    msg = Message(msg_type, data or {}, {"destination": destination} if destination is not None else {})
    return msg.serialize()


class TestClientIsolation:
    @pytest.mark.parametrize(("message_type", "histogram"), [
        ("speak", RUNTIME_BUS_PUBLIC_REPLY),
        ("ovos.utterance.handled", RUNTIME_BUS_PUBLIC_REPLY),
        ("mycroft.skill.handler.start", RUNTIME_BUS_SKILL_LIFECYCLE),
        ("ovos.skills.fallback.ping", RUNTIME_BUS_FALLBACK_COORDINATION),
        ("thalovant.runtime.query.prepared", RUNTIME_BUS_CONTROL),
        ("recognizer_loop:utterance", RUNTIME_BUS_CONTROL),
        ("recognizer_loop:audio_output_end", RUNTIME_BUS_OTHER),
    ])
    def test_runtime_bus_events_use_fixed_metric_categories(
            self, agent, message_type, histogram):
        initial = histogram.snapshot()["count"]

        agent.handle_internal_mycroft(_ovos_internal(message_type))

        assert histogram.snapshot()["count"] == initial + 1

    def test_private_handler_lifecycle_is_measured_once(self, agent):
        initial = SKILL_HANDLER.snapshot()["count"]
        context = {
            "query_id": "query-1",
            "session": {"session_id": "query-1"},
        }

        agent.handle_internal_mycroft(Message(
            "mycroft.skill.handler.start", {}, context
        ).serialize())
        agent.handle_internal_mycroft(Message(
            "mycroft.skill.handler.complete", {}, context
        ).serialize())

        assert SKILL_HANDLER.snapshot()["count"] == initial + 1

    @pytest.mark.parametrize("message_type", [
        "mycroft.skill.handler.start",
        "mycroft.skill.handler.complete",
        "ovos.skills.fallback.ping",
        "ovos.skills.fallback.skill-id.request",
        "thalovant.runtime.query.prepared",
        "recognizer_loop:utterance",
        "mycroft.intents.is_ready",
        "mycroft.skills.is_ready.response",
        "mycroft.thalovant-skill-weather.thalovant.is_ready",
        "mycroft.ovos-skill-volume.openvoiceos.is_ready.response",
    ])
    def test_runtime_private_events_never_reach_clients(
            self, agent, make_client, message_type):
        alice = make_client("ws://alice")
        agent.hm_protocol.clients = {"ws://alice": alice}

        agent.handle_internal_mycroft(
            _ovos_internal(message_type, destination="ws://alice")
        )

        alice.send.assert_not_called()

    @pytest.mark.parametrize("message_type", [
        "speak",
        "ovos.utterance.handled",
    ])
    def test_public_sdk_replies_remain_routable(
            self, agent, make_client, message_type):
        alice = make_client("ws://alice")
        agent.hm_protocol.clients = {"ws://alice": alice}

        agent.handle_internal_mycroft(
            _ovos_internal(message_type, destination="ws://alice")
        )

        alice.send.assert_called_once()

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

    def test_unknown_destination_is_diagnosed(self, agent, make_client):
        alice = make_client("ws://alice")
        agent.hm_protocol.clients = {"ws://alice": alice}

        with patch("hivemind_ovos_agent_plugin.LOG.warning") as warning:
            agent.handle_internal_mycroft(
                _ovos_internal("speak", destination="voice_sat::deadbeef")
            )

        warning.assert_called_once()
        assert warning.call_args.args == (
            "%s - destination peer not connected: %s",
            "speak",
            "voice_sat::deadbeef",
        )

    def test_ordinary_ovos_destinations_do_not_warn(self, agent, make_client):
        alice = make_client("voice_sat::c0ffee")
        agent.hm_protocol.clients = {"voice_sat::c0ffee": alice}

        with patch("hivemind_ovos_agent_plugin.LOG.warning") as warning:
            for destination in (
                "audio",
                "enclosure",
                "skills",
                "ovos.gui",
                "ovos-skill-date-time.openvoiceos",
            ):
                agent.handle_internal_mycroft(
                    _ovos_internal("speak", destination=destination)
                )

        warning.assert_not_called()
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

    def test_forwarded_message_omits_registered_null_session_fields(
        self, agent, make_client
    ):
        alice = make_client("ws://alice")
        agent.hm_protocol.clients = {"ws://alice": alice}
        message = Message(
            "ovos.utterance.speak",
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
