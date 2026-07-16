"""hivescope e2e: OVOSAgentProtocol as the brain of a real HiveMind topology.

hivescope's ``TopologyBuilder`` boots a real ``hivemind-core`` listener
protocol in-process (handshake, ACL admission, QUERY routing — no real hive
sockets) with THIS plugin as the master's agent protocol. The OVOS side is an
in-process ``FakeBus`` standing in for the messagebus connection, with a fake
"skill" answering ``recognizer_loop:utterance`` — everything between the
satellite and that bus is the production code path:

    satellite QUERY
      -> hivemind-core calls OVOSAgentProtocol.natural_language_query
      -> utterance injected on the OVOS bus (query-scoped session)
      -> fake skill emits ``speak`` + ``ovos.utterance.handled``
      -> streamed back to the originating satellite as QUERY responses

plus the BUS path: an injected utterance answered via ``Message.reply`` is
reverse-routed to the right satellite by ``handle_internal_mycroft``.
"""
import pytest

from hivemind_bus_client.message import HiveMessage, HiveMessageType
from hivemind_ovos_agent_plugin import OVOSAgentProtocol
from ovos_bus_client.message import Message
from ovos_utils.fakebus import FakeBus

try:
    from hivescope.topology import TopologyBuilder
except ImportError:
    TopologyBuilder = None

pytestmark = pytest.mark.skipif(TopologyBuilder is None, reason="needs hivescope")


def _make_agent() -> OVOSAgentProtocol:
    """OVOSAgentProtocol on an in-process FakeBus.

    ``__post_init__`` would replace a FakeBus with a real ``MessageBusClient``
    and fail fast without a live ovos-messagebus, so the instance is built
    bare and wired to the FakeBus, then the production bus handlers are
    registered exactly as ``__post_init__`` would.
    """
    agent = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
    agent.config = {}
    agent.hm_protocol = None  # assigned by HiveMindListenerProtocol on bind
    agent.bus = FakeBus()
    agent.register_bus_handlers()
    return agent


def _fake_skill(agent: OVOSAgentProtocol, answer: str):
    """Answer any injected utterance like a skill: ``speak`` + handled signal,
    via ``Message.reply`` so source/destination swap for reverse routing."""
    def _responder(msg: Message):
        agent.bus.emit(msg.reply("speak", {"utterance": answer}))
        agent.bus.emit(msg.reply("ovos.utterance.handled", {}))
    agent.bus.on("recognizer_loop:utterance", _responder)


def _hive(agent: OVOSAgentProtocol) -> TopologyBuilder:
    assert TopologyBuilder is not None
    b = TopologyBuilder()
    m = b.add_master("M0", agent_protocol=agent)
    m.register_satellite("ovos-key", password="ovos-pw",
                         allowed_types=["recognizer_loop:utterance"])
    b.add_satellite("S0", upstream=m,
                    allowed_types=["recognizer_loop:utterance"])
    return b


def _speak_texts(records):
    """Unwrap recorded QUERY response chunks down to the spoken text."""
    texts = []
    for rec in records:
        payload = rec.payload
        for _ in range(4):
            if isinstance(payload, Message):
                if payload.msg_type == "speak":
                    texts.append(payload.data.get("utterance", ""))
                break
            if isinstance(payload, dict):
                if payload.get("type") == "speak":
                    texts.append(payload.get("data", {}).get("utterance", ""))
                    break
                payload = payload.get("payload")
            else:
                payload = getattr(payload, "payload", None)
            if payload is None:
                break
    return texts


def test_query_answered_by_ovos_agent():
    agent = _make_agent()
    _fake_skill(agent, "the hive says hello")
    b = _hive(agent)
    b.start_all()
    try:
        s = b.get_satellite("S0")
        inner = HiveMessage(HiveMessageType.BUS,
                            payload=Message("recognizer_loop:utterance",
                                            {"utterances": ["hello"]}))
        s.send(HiveMessage(HiveMessageType.QUERY, payload=inner,
                           metadata={"query_id": "q1",
                                     "originator_peer": s.peer}))
        recv = s.recorder.wait_for(HiveMessageType.QUERY.value,
                                   direction="in", timeout=8.0)
        assert recv is not None, "OVOS agent answer never reached the satellite"
        texts = _speak_texts(
            s.recorder.received(HiveMessageType.QUERY.value, direction="in"))
        assert any("the hive says hello" in t for t in texts), \
            f"expected the fake-skill answer, got {texts!r}"
    finally:
        b.stop_all()


def test_bus_utterance_reverse_routed_to_originating_satellite():
    """A plain BUS utterance is injected on the OVOS bus; the skill's
    ``Message.reply`` answer is reverse-routed by ``handle_internal_mycroft``
    only to the satellite that asked."""
    agent = _make_agent()
    _fake_skill(agent, "routed back")
    b = _hive(agent)
    b.start_all()
    try:
        s = b.get_satellite("S0")
        s.send(HiveMessage(HiveMessageType.BUS,
                           payload=Message("recognizer_loop:utterance",
                                           {"utterances": ["ping"]})))
        recv = s.recorder.wait_for(HiveMessageType.BUS.value,
                                   direction="in", timeout=8.0)
        assert recv is not None, \
            "skill reply never reverse-routed to the satellite"
    finally:
        b.stop_all()
