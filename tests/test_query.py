from hivemind_ovos_agent_plugin import OVOSAgentProtocol
from ovos_bus_client.message import Message
from ovos_utils.fakebus import FakeBus


def _agent():
    agent = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
    agent.bus = FakeBus()
    return agent


def test_query_accepts_session_correlated_speak():
    agent = _agent()

    def responder(request):
        query_id = request.context["query_id"]
        session = {"session_id": query_id}
        agent.bus.emit(Message(
            "speak",
            {"utterance": "session-correlated answer"},
            {"session": session, "skill_id": "test.skill"},
        ))
        agent.bus.emit(Message(
            "ovos.utterance.handled",
            {},
            {"query_id": query_id, "session": session},
        ))

    agent.bus.on("recognizer_loop:utterance", responder)

    assert list(agent.natural_language_query("hello", "en-US")) == [
        "session-correlated answer",
        None,
    ]


def test_query_rejects_speak_from_another_session():
    agent = _agent()

    def responder(request):
        query_id = request.context["query_id"]
        agent.bus.emit(Message(
            "speak",
            {"utterance": "wrong client"},
            {"session": {"session_id": "another-query"}},
        ))
        agent.bus.emit(Message(
            "speak",
            {"utterance": "right client"},
            {"session": {"session_id": query_id}},
        ))
        agent.bus.emit(Message(
            "ovos.utterance.handled",
            {},
            {"session": {"session_id": query_id}},
        ))

    agent.bus.on("recognizer_loop:utterance", responder)

    assert list(agent.natural_language_query("hello", "en-US")) == [
        "right client",
        None,
    ]


def test_query_accepts_ovos_utterance_speak_alias():
    agent = _agent()

    def responder(request):
        query_id = request.context["query_id"]
        context = {"session": {"session_id": query_id}}
        agent.bus.emit(Message(
            "ovos.utterance.speak",
            {"utterance": "aliased answer"},
            context,
        ))
        agent.bus.emit(Message("ovos.utterance.handled", {}, context))

    agent.bus.on("recognizer_loop:utterance", responder)

    assert list(agent.natural_language_query("hello", "en-US")) == [
        "aliased answer",
        None,
    ]


def test_answer_query_message_preserves_admitted_context():
    agent = _agent()
    emitted = []

    def responder(request):
        emitted.append(request)
        query_id = request.context["query_id"]
        context = {"session": {"session_id": query_id}}
        agent.bus.emit(Message(
            "speak",
            {"utterance": "policy-aware answer"},
            context,
        ))
        agent.bus.emit(Message("ovos.utterance.handled", {}, context))

    agent.bus.on("recognizer_loop:utterance", responder)
    admitted = Message(
        "recognizer_loop:utterance",
        {"utterances": ["hello"], "lang": "en-US"},
        {
            "destination": "skills",
            "source": "client::original-session",
            "session": {
                "session_id": "original-session",
                "site_id": "customer-site",
                "blacklisted_skills": ["blocked.skill"],
                "blacklisted_intents": ["blocked.intent"],
            },
        },
    )

    assert list(agent.answer_query_message(admitted)) == [
        "policy-aware answer",
        None,
    ]
    assert len(emitted) == 1
    query = emitted[0]
    assert query.context["destination"] == "skills"
    assert query.context["source"] == "client::original-session"
    assert query.context["session"]["site_id"] == "customer-site"
    assert query.context["session"]["blacklisted_skills"] == ["blocked.skill"]
    assert query.context["session"]["blacklisted_intents"] == ["blocked.intent"]
    assert query.context["session"]["session_id"] == query.context["query_id"]
    assert query.context["session"]["session_id"] != "original-session"
    assert admitted.context["session"]["session_id"] == "original-session"
