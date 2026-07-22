import time
from threading import Thread
from unittest.mock import MagicMock

import pytest

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


def test_query_waits_for_real_handler_completion_after_progress_speak():
    agent = _agent()
    agent.config = {
        "query_timeout": 0.5,
        "query_reply_grace": 0.01,
    }
    worker = None

    def responder(request):
        nonlocal worker
        query_id = request.context["query_id"]
        context = {
            "session": {"session_id": query_id},
            "skill_id": "slow.skill",
        }
        agent.bus.emit(Message(
            "mycroft.skill.handler.start", {}, context
        ))
        agent.bus.emit(Message(
            "speak", {"utterance": "working"}, context
        ))

        def finish_handler():
            time.sleep(0.03)
            agent.bus.emit(Message(
                "speak", {"utterance": "final answer"}, context
            ))
            agent.bus.emit(Message(
                "mycroft.skill.handler.complete", {}, context
            ))

        worker = Thread(target=finish_handler)
        worker.start()

    agent.bus.on("recognizer_loop:utterance", responder)

    assert list(agent.natural_language_query("hello", "en-US")) == [
        "working",
        "final answer",
        None,
    ]
    worker.join(timeout=1)


def test_query_handler_error_is_terminal_without_handled_event():
    agent = _agent()

    def responder(request):
        query_id = request.context["query_id"]
        context = {"session": {"session_id": query_id}}
        agent.bus.emit(Message(
            "mycroft.skill.handler.start", {}, context
        ))
        agent.bus.emit(Message(
            "mycroft.skill.handler.error", {}, context
        ))

    agent.bus.on("recognizer_loop:utterance", responder)

    assert list(agent.natural_language_query("hello", "en-US")) == [None]


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


def test_confirmed_query_receipt_is_the_runtime_liveness_probe():
    """Do not spend a second recovery window on a redundant runtime probe."""
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
    assert order == [("accept", 2.0), ("utterance", "hello")]
    agent.bus.ensure_delivery_path.assert_not_called()


def test_query_timeout_includes_confirmed_delivery_time(monkeypatch):
    """A slow receipt cannot reset the complete query timeout afterward."""
    agent = _agent()
    logger = MagicMock()
    monkeypatch.setattr(agent_module, "LOG", logger)
    agent.config = {"query_timeout": 0.05}

    def slow_delivery(_message, _timeout):
        time.sleep(0.06)

    agent.bus.emit_confirmed = MagicMock(side_effect=slow_delivery)

    started = time.monotonic()
    assert list(agent.natural_language_query("hello", "en-US")) == [None]

    assert time.monotonic() - started < 0.09
    logger.warning.assert_called_once()


def test_query_after_bounded_delivery_failure_still_flows():
    """One failed receipt window cannot poison the next independent query."""
    agent = _agent()
    attempts = 0

    def confirmed_delivery(message, _timeout):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise TimeoutError("receipt window expired")
        agent.bus.emit(message)

    agent.bus.emit_confirmed = MagicMock(side_effect=confirmed_delivery)

    def responder(request):
        query_id = request.context["query_id"]
        context = {"session": {"session_id": query_id}}
        agent.bus.emit(Message(
            "speak", {"utterance": "recovered"}, context
        ))
        agent.bus.emit(Message("ovos.utterance.handled", {}, context))

    agent.bus.on("recognizer_loop:utterance", responder)

    with pytest.raises(TimeoutError, match="receipt window expired"):
        list(agent.natural_language_query("first", "en-US"))

    assert list(agent.natural_language_query("second", "en-US")) == [
        "recovered", None,
    ]
    assert agent._active_query_scopes == {}


def test_sequential_queries_keep_runtime_bus_subscriptions_immutable():
    """Query cleanup must never mutate pyee from a worker thread."""
    agent = _agent()
    agent.bus.remove = MagicMock(
        side_effect=AssertionError("per-query bus removal is unsafe")
    )

    def responder(request):
        query_id = request.context["query_id"]
        context = {"session": {"session_id": query_id}}
        agent.bus.emit(Message(
            "speak", {"utterance": "ready"}, context
        ))
        agent.bus.emit(Message("ovos.utterance.handled", {}, context))

    agent.bus.on("recognizer_loop:utterance", responder)

    for index in range(100):
        assert list(agent.natural_language_query(
            f"query {index}", "en-US"
        )) == ["ready", None]

    agent.bus.remove.assert_not_called()
    assert agent._active_query_scopes == {}
    assert agent._active_query_callbacks == {}
    assert len(agent._query_dispatcher_buses) == 1
