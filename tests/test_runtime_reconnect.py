import logging
from types import SimpleNamespace
from threading import Event
from unittest.mock import MagicMock, call

import pytest
from websocket import WebSocketConnectionClosedException

import hivemind_ovos_agent_plugin as agent_module
from hivemind_ovos_agent_plugin import (
    _RuntimeMessageBusClient,
    _TransientWebsocketDisconnectFilter,
    _install_websocket_disconnect_log_filter,
)
from pyee import EventEmitter
from ovos_bus_client import MessageBusClient
from ovos_bus_client.message import Message


def _client():
    """Build a minimal runtime bus client with observable collaborators."""
    client = _RuntimeMessageBusClient.__new__(_RuntimeMessageBusClient)
    client._disconnect_started_at = None
    client._disconnect_escalated = False
    client._reconnect_error_after = 120.0
    client._message_send_timeout = 0.05
    client._ping_interval = 15.0
    client._ping_timeout = 5.0
    client.session_id = "runtime-probe-test"
    client.retry = 5
    client.connected_event = Event()
    client.connected_event.set()
    client.client = SimpleNamespace(
        keep_running=True,
        close=MagicMock(),
        run_forever=MagicMock(),
        send=MagicMock(),
        sock=SimpleNamespace(connected=True),
        url="ws://runtime:8181",
    )
    client.emitter = MagicMock()
    client.create_client = MagicMock(return_value=MagicMock())
    client.run_forever = MagicMock()
    return client


def _wait_for_reconnect(client):
    """Wait for a scheduled reconnect worker and prove it terminated."""
    worker = client._reconnect_worker
    assert worker is not None
    worker.join(timeout=1)
    assert not worker.is_alive()


def test_transient_runtime_disconnect_retries_without_warning_or_traceback(
        monkeypatch):
    """Treat a transient runtime outage as an informational reconnect."""
    client = _client()
    original_transport = client.client
    logger = MagicMock()
    monkeypatch.setattr(agent_module, "LOG", logger)
    monkeypatch.setattr(agent_module.time, "sleep", MagicMock())
    monkeypatch.setattr(agent_module.time, "monotonic", MagicMock(return_value=10))

    client.on_error(PermissionError(1, "Operation not permitted"))
    _wait_for_reconnect(client)

    logger.info.assert_called_once()
    logger.warning.assert_not_called()
    logger.exception.assert_not_called()
    logger.error.assert_not_called()
    original_transport.close.assert_called_once_with()
    assert not client.connected_event.is_set()
    client.emitter.emit.assert_called_once_with("reconnecting")
    client.create_client.assert_called_once_with()
    client.run_forever.assert_called_once_with()
    assert client.retry == 10


def test_closed_connection_uses_the_same_bounded_reconnect_path(monkeypatch):
    """Reconnect websocket connection-closed errors without warning noise."""
    client = _client()
    logger = MagicMock()
    monkeypatch.setattr(agent_module, "LOG", logger)
    monkeypatch.setattr(agent_module.time, "sleep", MagicMock())
    monkeypatch.setattr(agent_module.time, "monotonic", MagicMock(return_value=10))

    client.on_error(WebSocketConnectionClosedException("closed"))
    _wait_for_reconnect(client)

    logger.info.assert_called_once()
    logger.warning.assert_not_called()
    logger.exception.assert_not_called()
    logger.error.assert_not_called()


def test_clean_close_uses_the_same_bounded_reconnect_path(monkeypatch):
    """Reconnect after a clean runtime rollout close callback."""
    client = _client()
    logger = MagicMock()
    monkeypatch.setattr(agent_module, "LOG", logger)
    monkeypatch.setattr(agent_module.time, "sleep", MagicMock())
    monkeypatch.setattr(agent_module.time, "monotonic", MagicMock(return_value=10))

    client.on_close(client.client, 1000, "runtime rollout")
    _wait_for_reconnect(client)

    logger.info.assert_called_once()
    logger.warning.assert_not_called()
    logger.exception.assert_not_called()
    logger.error.assert_not_called()
    assert not client.connected_event.is_set()
    client.emitter.emit.assert_any_call("close")
    client.emitter.emit.assert_any_call("reconnecting")
    client.create_client.assert_called_once_with()
    client.run_forever.assert_called_once_with()


def test_reconnect_worker_survives_repeated_clean_closes(monkeypatch):
    """Use one worker to consume repeated close callbacks without leaking."""
    client = _client()
    logger = MagicMock()
    monkeypatch.setattr(agent_module, "LOG", logger)
    monkeypatch.setattr(agent_module.time, "sleep", MagicMock())
    monkeypatch.setattr(agent_module.time, "monotonic", MagicMock(return_value=10))
    reconnects = 0

    def run_forever():
        nonlocal reconnects
        reconnects += 1
        if reconnects == 1:
            client.on_close(client.client, 1000, "second runtime rollout")

    client.run_forever = MagicMock(side_effect=run_forever)

    client.on_close(client.client, 1000, "first runtime rollout")
    _wait_for_reconnect(client)

    assert reconnects == 2
    assert client.create_client.call_count == 2
    assert logger.info.call_count == 2
    assert client._reconnect_worker is None
    logger.warning.assert_not_called()
    logger.exception.assert_not_called()
    logger.error.assert_not_called()


def test_transient_disconnect_escalates_once_after_recovery_budget(monkeypatch):
    """Escalate a prolonged outage exactly once per outage window."""
    client = _client()
    client._disconnect_started_at = 10
    logger = MagicMock()
    monkeypatch.setattr(agent_module, "LOG", logger)
    monkeypatch.setattr(agent_module.time, "sleep", MagicMock())
    monkeypatch.setattr(agent_module.time, "monotonic", MagicMock(return_value=131))

    client.on_error(PermissionError(1, "Operation not permitted"))
    _wait_for_reconnect(client)
    client.on_error(PermissionError(1, "Operation not permitted"))
    _wait_for_reconnect(client)

    logger.error.assert_called_once()
    logger.info.assert_called_once()
    assert client._disconnect_escalated is True


def test_successful_connection_resets_disconnect_budget(monkeypatch):
    """Reset outage state and backoff after a successful connection."""
    client = _client()
    client._disconnect_started_at = 10
    client._disconnect_escalated = True
    client.retry = 60
    parent_on_open = MagicMock()
    monkeypatch.setattr(MessageBusClient, "on_open", parent_on_open)

    client.on_open("socket")

    assert client._disconnect_started_at is None
    assert client._disconnect_escalated is False
    assert client.retry == 5
    parent_on_open.assert_called_once_with("socket")


def test_run_forever_enables_websocket_heartbeat():
    """Detect a half-open runtime socket instead of waiting for a later send."""
    client = _client()

    _RuntimeMessageBusClient.run_forever(client)

    client.client.run_forever.assert_called_once_with(
        ping_interval=15.0,
        ping_timeout=5.0,
    )
    assert client.started_running is True


def test_send_recovers_a_stale_transport_without_leaking_worker(monkeypatch):
    """Wake the reconnect path and retry on the replacement websocket."""
    client = _client()
    stale = client.client
    stale.sock.connected = False
    replacement = SimpleNamespace(
        sock=SimpleNamespace(connected=True),
        send=MagicMock(),
    )

    def reconnect(_error):
        client.connected_event.clear()
        client.client = replacement
        client.connected_event.set()

    monkeypatch.setattr(client, "_schedule_reconnect", reconnect)

    client._send(Message("recognizer_loop:utterance", {"utterances": ["hi"]}))

    stale.send.assert_not_called()
    replacement.send.assert_called_once()


def test_closed_send_reconnects_and_retries_exact_frame(monkeypatch):
    """A send-detected close gets one bounded retry on the fresh socket."""
    client = _client()
    stale = client.client
    stale.send.side_effect = WebSocketConnectionClosedException("closed")
    replacement = SimpleNamespace(
        sock=SimpleNamespace(connected=True),
        send=MagicMock(),
    )

    def reconnect(_error):
        client.connected_event.clear()
        client.client = replacement
        client.connected_event.set()

    monkeypatch.setattr(client, "_schedule_reconnect", reconnect)
    message = Message("recognizer_loop:utterance", {"utterances": ["hi"]})

    client._send(message)

    stale.send.assert_called_once()
    replacement.send.assert_called_once_with(stale.send.call_args.args[0])


def test_send_reconnect_wait_is_hard_bounded(monkeypatch):
    """Never inherit ovos-bus-client's unbounded post-disconnect wait."""
    client = _client()
    client.client.sock.connected = False
    client.connected_event.clear()
    monkeypatch.setattr(client, "_schedule_reconnect", MagicMock())

    with pytest.raises(TimeoutError, match="did not reconnect"):
        client._send(Message("recognizer_loop:utterance", {}))

    client.client.send.assert_not_called()


def test_delivery_probe_accepts_only_its_exact_bus_echo():
    """Prove liveness with a broker round trip, not a local socket flag."""
    client = _client()
    client.emitter = EventEmitter()

    def echo(payload):
        message = Message.deserialize(payload)
        client.emitter.emit(message.msg_type, message)

    client.client.send.side_effect = echo

    assert client._probe_delivery_once(0.05) is True
    assert client.client.send.call_count == 1


def test_delivery_probe_reconnects_before_any_user_message(monkeypatch):
    """Replace a silent half-open path, then accept the fresh bus echo."""
    client = _client()
    probe = MagicMock(side_effect=[False, True])
    reconnect = MagicMock()
    monkeypatch.setattr(client, "_probe_delivery_once", probe)
    monkeypatch.setattr(client, "_schedule_reconnect", reconnect)
    monkeypatch.setattr(client, "_wait_for_live_transport", MagicMock(
        return_value=True
    ))

    client.ensure_delivery_path(0.01)

    assert probe.call_args_list == [call(0.01), call(0.01)]
    reconnect.assert_called_once()


def test_delivery_probe_fails_bounded_after_fresh_path_is_silent(monkeypatch):
    """Never send a user utterance when both delivery checks are blackholed."""
    client = _client()
    monkeypatch.setattr(
        client, "_probe_delivery_once", MagicMock(return_value=False)
    )
    monkeypatch.setattr(client, "_schedule_reconnect", MagicMock())
    monkeypatch.setattr(client, "_wait_for_live_transport", MagicMock(
        return_value=True
    ))

    with pytest.raises(TimeoutError, match="did not echo"):
        client.ensure_delivery_path(0.01)

    assert client._schedule_reconnect.call_count == 2


def test_unexpected_error_uses_upstream_error_semantics(monkeypatch):
    """Delegate non-transient errors to the upstream bus client."""
    client = _client()
    parent_on_error = MagicMock()
    monkeypatch.setattr(MessageBusClient, "on_error", parent_on_error)
    error = ValueError("unexpected")

    client.on_error(error)

    parent_on_error.assert_called_once_with(error)


def test_explicit_close_does_not_reconnect(monkeypatch):
    """Suppress reconnect scheduling after an intentional shutdown."""
    client = _client()
    parent_close = MagicMock()
    monkeypatch.setattr(MessageBusClient, "close", parent_close)

    client.close()
    client.on_close(client.client, 1000, "shutdown")

    parent_close.assert_called_once_with()
    assert client._reconnect_worker is None
    client.create_client.assert_not_called()


def test_websocket_filter_downgrades_only_exact_transient_disconnects():
    """Keep unexpected websocket failures at ERROR for the QA log gate."""
    filter_ = _TransientWebsocketDisconnectFilter()

    transient = logging.LogRecord(
        "websocket",
        logging.ERROR,
        __file__,
        1,
        "[Errno 1] Operation not permitted - goodbye",
        (),
        None,
    )
    refused = logging.LogRecord(
        "websocket",
        logging.ERROR,
        __file__,
        1,
        "[Errno 111] Connection refused - goodbye",
        (),
        None,
    )
    unexpected = logging.LogRecord(
        "websocket",
        logging.ERROR,
        __file__,
        1,
        "TLS certificate validation failed - goodbye",
        (),
        None,
    )

    assert filter_.filter(transient) is True
    assert transient.levelno == logging.INFO
    assert transient.levelname == "INFO"
    assert filter_.filter(refused) is True
    assert refused.levelno == logging.INFO
    assert refused.levelname == "INFO"
    assert filter_.filter(unexpected) is True
    assert unexpected.levelno == logging.ERROR
    assert unexpected.levelname == "ERROR"


def test_websocket_filter_installation_is_idempotent(monkeypatch):
    """Avoid accumulating filters when more than one agent is constructed."""
    logger = logging.getLogger("websocket")
    monkeypatch.setattr(logger, "filters", [])

    _install_websocket_disconnect_log_filter()
    _install_websocket_disconnect_log_filter()

    assert sum(
        isinstance(item, _TransientWebsocketDisconnectFilter)
        for item in logger.filters
    ) == 1
