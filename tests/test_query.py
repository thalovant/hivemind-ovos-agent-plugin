import time
from threading import Thread
from unittest.mock import MagicMock

import hivemind_ovos_agent_plugin as agent_module
from hivemind_ovos_agent_plugin import OVOSAgentProtocol
from ovos_bus_client.message import Message
from ovos_utils.fakebus import FakeBus


def _agent():
    agent = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
    agent.bus = FakeBus()
    agent._ensure_query_correlation_state()
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


def test_query_waits_for_speak_immediately_after_handled():
    agent = _agent()

    def responder(request):
        query_id = request.context["query_id"]
        context = {
            "session": {"session_id": query_id},
            "skill_id": "late.skill",
        }
        agent.bus.emit(Message("ovos.utterance.handled", {}, context))
        agent.bus.emit(Message(
            "ovos.utterance.speak",
            {"utterance": "answer after handled"},
            context,
        ))

    agent.bus.on("recognizer_loop:utterance", responder)

    assert list(agent.natural_language_query("hello", "en-US")) == [
        "answer after handled",
        None,
    ]


def test_query_completes_after_reply_when_handled_correlation_is_missing(
    monkeypatch,
):
    agent = _agent()
    logger = MagicMock()
    monkeypatch.setattr(agent_module, "LOG", logger)
    agent.config = {
        "query_timeout": 0.5,
        "query_reply_grace": 0.01,
    }

    def responder(request):
        query_id = request.context["query_id"]
        agent.bus.emit(Message(
            "speak",
            {"utterance": "answer without correlated completion"},
            {
                "session": {"session_id": query_id},
                "skill_id": "fallback.skill",
            },
        ))

    agent.bus.on("recognizer_loop:utterance", responder)

    started = time.monotonic()
    assert list(agent.natural_language_query("hello", "en-US")) == [
        "answer without correlated completion",
        None,
    ]
    assert time.monotonic() - started < 0.2
    logger.warning.assert_not_called()


def test_context_aware_query_preserves_speak_message_provenance():
    agent = _agent()

    def responder(request):
        query_id = request.context["query_id"]
        context = {
            "session": {"session_id": query_id},
            "skill_id": "answer.skill",
        }
        agent.bus.emit(Message(
            "speak",
            {"utterance": "owned answer"},
            context,
        ))
        agent.bus.emit(Message("ovos.utterance.handled", {}, context))

    agent.bus.on("recognizer_loop:utterance", responder)
    admitted = Message(
        "recognizer_loop:utterance",
        {"utterances": ["hello"], "lang": "en-US"},
        {"session": {"session_id": "client-session"}},
    )

    chunks = list(agent.answer_query_message(admitted))
    assert isinstance(chunks[0], Message)
    assert chunks[0].data["utterance"] == "owned answer"
    assert chunks[0].context["skill_id"] == "answer.skill"
    assert chunks[1] is None


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

    chunks = list(agent.answer_query_message(admitted))
    assert isinstance(chunks[0], Message)
    assert chunks[0].data["utterance"] == "policy-aware answer"
    assert chunks[1] is None
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


def test_context_query_accepts_reply_with_unique_site_scope():
    agent = _agent()

    def responder(request):
        query_id = request.context["query_id"]
        agent.bus.emit(Message(
            "speak",
            {"utterance": "scope-correlated answer"},
            {
                "session": {"site_id": "customer-site"},
                "skill_id": "scope.skill",
            },
        ))
        agent.bus.emit(Message(
            "ovos.utterance.handled", {}, {"query_id": query_id}
        ))

    agent.bus.on("recognizer_loop:utterance", responder)
    admitted = Message(
        "recognizer_loop:utterance",
        {"utterances": ["hello"]},
        {
            "source": "client::one",
            "session": {
                "session_id": "client-session",
                "site_id": "customer-site",
            },
        },
    )

    chunks = list(agent.answer_query_message(admitted))
    assert chunks[0].data["utterance"] == "scope-correlated answer"
    assert chunks[0].context["skill_id"] == "scope.skill"
    assert chunks[1] is None
    assert agent._active_query_scopes == {}


def test_context_query_accepts_reply_routed_to_unique_client_source():
    agent = _agent()

    def responder(request):
        query_id = request.context["query_id"]
        agent.bus.emit(Message(
            "speak",
            {"utterance": "client-correlated answer"},
            {
                "destination": "client::one",
                "skill_id": "scope.skill",
            },
        ))
        agent.bus.emit(Message(
            "ovos.utterance.handled", {}, {"query_id": query_id}
        ))

    agent.bus.on("recognizer_loop:utterance", responder)
    admitted = Message(
        "recognizer_loop:utterance",
        {"utterances": ["hello"]},
        {"source": "client::one", "session": {"session_id": "one"}},
    )

    chunks = list(agent.answer_query_message(admitted))
    assert chunks[0].data["utterance"] == "client-correlated answer"
    assert chunks[1] is None


def test_context_query_rejects_uncorrelated_reply_from_wrong_scope(monkeypatch):
    agent = _agent()
    logger = MagicMock()
    monkeypatch.setattr(agent_module, "LOG", logger)
    agent.config = {"query_timeout": 0.05, "query_handled_grace": 0.01}

    def responder(request):
        query_id = request.context["query_id"]
        agent.bus.emit(Message(
            "speak",
            {"utterance": "wrong scope"},
            {"session": {"site_id": "another-site"}},
        ))
        agent.bus.emit(Message(
            "ovos.utterance.handled", {}, {"query_id": query_id}
        ))

    agent.bus.on("recognizer_loop:utterance", responder)
    admitted = Message(
        "recognizer_loop:utterance",
        {"utterances": ["hello"]},
        {"session": {"site_id": "customer-site"}},
    )

    assert list(agent.answer_query_message(admitted)) == [None]
    assert agent._active_query_scopes == {}
    logger.warning.assert_called_once_with(
        "OVOS query timed out before a correlated reply was observed"
    )


def test_context_query_rejects_foreign_active_query_id_on_matching_scope():
    agent = _agent()
    agent.config = {"query_timeout": 0.05, "query_handled_grace": 0.01}

    def responder(request):
        query_id = request.context["query_id"]
        agent._register_active_query(
            "foreign-query", {"session": {"site_id": "customer-site"}}
        )
        agent.bus.emit(Message(
            "speak",
            {"utterance": "foreign answer"},
            {
                "query_id": "foreign-query",
                "session": {"site_id": "customer-site"},
            },
        ))
        agent._unregister_active_query("foreign-query")
        agent.bus.emit(Message(
            "ovos.utterance.handled", {}, {"query_id": query_id}
        ))

    agent.bus.on("recognizer_loop:utterance", responder)
    admitted = Message(
        "recognizer_loop:utterance",
        {"utterances": ["hello"]},
        {"session": {"site_id": "customer-site"}},
    )

    assert list(agent.answer_query_message(admitted)) == [None]


def test_context_query_rejects_ambiguous_shared_site_scope():
    agent = _agent()
    agent.config = {"query_timeout": 0.1, "query_handled_grace": 0.01}
    requests = []
    results = {}

    def responder(request):
        requests.append(request)
        if len(requests) != 2:
            return
        agent.bus.emit(Message(
            "speak",
            {"utterance": "ambiguous answer"},
            {"session": {"site_id": "shared-site"}},
        ))
        for item in requests:
            agent.bus.emit(Message(
                "ovos.utterance.handled",
                {},
                {"query_id": item.context["query_id"]},
            ))

    def run_query(name):
        admitted = Message(
            "recognizer_loop:utterance",
            {"utterances": [name]},
            {
                "source": f"client::{name}",
                "session": {"site_id": "shared-site"},
            },
        )
        results[name] = list(agent.answer_query_message(admitted))

    agent.bus.on("recognizer_loop:utterance", responder)
    workers = [Thread(target=run_query, args=(name,)) for name in ("one", "two")]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=1)

    assert results == {"one": [None], "two": [None]}
    assert agent._active_query_scopes == {}


def test_context_query_prefers_unique_client_over_shared_site():
    agent = _agent()
    agent._register_active_query(
        "one",
        {"source": "client::one", "session": {"site_id": "shared-site"}},
    )
    agent._register_active_query(
        "two",
        {"source": "client::two", "session": {"site_id": "shared-site"}},
    )
    response = Message(
        "speak",
        {"utterance": "answer for one"},
        {
            "destination": "client::one",
            "session": {"site_id": "shared-site"},
        },
    )

    assert agent._uniquely_matches_active_scope(response, "one")
    assert not agent._uniquely_matches_active_scope(response, "two")


def test_context_query_cleans_registry_when_delivery_fails():
    agent = _agent()
    agent.bus.ensure_delivery_path = MagicMock(
        side_effect=ConnectionError("runtime unavailable")
    )
    admitted = Message(
        "recognizer_loop:utterance",
        {"utterances": ["hello"]},
        {"source": "client::one", "session": {"site_id": "one"}},
    )

    try:
        list(agent.answer_query_message(admitted))
    except ConnectionError:
        pass
    else:
        raise AssertionError("delivery failure should propagate")

    assert agent._active_query_scopes == {}


def test_runtime_delivery_probe_precedes_user_utterance():
    """Require both runtime liveness and receipt before waiting for an answer."""
    agent = _agent()
    order = []
    agent.bus.ensure_delivery_path = MagicMock(
        side_effect=lambda timeout: order.append(("probe", timeout))
    )

    def emit_confirmed(message, timeout):
        order.append(("accept", timeout))
        agent.bus.emit(message)

    agent.bus.emit_confirmed = MagicMock(side_effect=emit_confirmed)

    def responder(request):
        order.append(("utterance", request.data["utterances"][0]))
        query_id = request.context["query_id"]
        context = {"session": {"session_id": query_id}}
        agent.bus.emit(Message(
            "speak", {"utterance": "ready"}, context
        ))
        agent.bus.emit(Message("ovos.utterance.handled", {}, context))

    agent.bus.on("recognizer_loop:utterance", responder)

    assert list(agent.natural_language_query("hello", "en-US")) == [
        "ready", None,
    ]
    assert order == [
        ("probe", 2.0),
        ("accept", 2.0),
        ("utterance", "hello"),
    ]
