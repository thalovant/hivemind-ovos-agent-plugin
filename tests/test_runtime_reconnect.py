import logging
import queue
import time
from types import SimpleNamespace
from threading import Event, Lock, Thread
from unittest.mock import MagicMock, call, patch

import pytest
from websocket import (WebSocketAddressException,
                       WebSocketConnectionClosedException)

import hivemind_ovos_agent_plugin as agent_module
from hivemind_ovos_agent_plugin import (
    _RuntimeMessageBusClient,
    _TransientWebsocketDisconnectFilter,
    _install_websocket_disconnect_log_filter,
)
from pyee import EventEmitter
from ovos_bus_client import MessageBusClient
from ovos_bus_client.message import Message
from ovos_bus_client.session import Session


def _client():
    """Build a minimal runtime bus client with observable collaborators.

    Deliberately built with ``__new__``: these tests drive the reconnect and
    delivery paths directly, and a real ``__init__`` would also start the bus
    writer thread and change which send path they exercise.

    The base ``__init__`` still runs, with ``create_client`` patched out so no
    socket is built. Enumerating the attributes it sets was the alternative,
    and every ovos-bus-client release that read a new one on the emit path
    broke every test here with an ``AttributeError`` that said nothing about
    the plugin -- ``session``, then ``_translator``, then
    ``_wire_legacy_twins``.
    """
    client = _RuntimeMessageBusClient.__new__(_RuntimeMessageBusClient)
    with patch.object(MessageBusClient, "create_client",
                      return_value=MagicMock()):
        MessageBusClient.__init__(
            client, host="runtime", port=8181, route="/core", ssl=False,
            emitter=MagicMock(),
        )
    client._disconnect_started_at = None
    client._disconnect_escalated = False
    client._reconnect_error_after = 120.0
    client._message_send_timeout = 0.05
    client._delivery_recovery_timeout = 0.05
    client._ping_interval = 15.0
    client._ping_timeout = 5.0
    client.session_id = "runtime-probe-test"
    client.session = Session(client.session_id)
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


def _queued_client(maxsize=2):
    """Add the production bounded writer state to a minimal test client."""
    client = _client()
    client._bus_write_queue = queue.Queue(maxsize=maxsize)
    client._bus_writer_stop = Event()
    client._bus_writer_thread = None
    client._schedule_reconnect = MagicMock()
    return client


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


def test_runtime_dns_miss_uses_the_bounded_reconnect_path(monkeypatch):
    """Treat a temporarily absent Kubernetes Service name as transient."""
    client = _client()
    logger = MagicMock()
    monkeypatch.setattr(agent_module, "LOG", logger)
    monkeypatch.setattr(agent_module.time, "sleep", MagicMock())
    monkeypatch.setattr(agent_module.time, "monotonic", MagicMock(return_value=10))

    client.on_error(WebSocketAddressException("runtime service not ready"))
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


def test_production_bus_writer_preserves_fifo_order():
    client = _queued_client()
    first_started = Event()
    release_first = Event()
    sent = []

    def send(message, *, reconnect):
        assert reconnect is False
        sent.append(message.msg_type)
        if message.msg_type == "first":
            first_started.set()
            assert release_first.wait(1)

    client._send_synchronously = MagicMock(side_effect=send)
    client._start_bus_writer()
    completions = [client._send(Message("first"))]
    assert first_started.wait(0.2)
    completions.extend([
        client._send(Message("second")),
        client._send(Message("third")),
    ])
    release_first.set()

    for completion in completions:
        assert completion.result(timeout=1) is None
    client._bus_writer_stop.set()
    client._bus_writer_thread.join(timeout=1)

    assert sent == ["first", "second", "third"]


def test_production_bus_writer_rejects_queue_overload():
    client = _queued_client(maxsize=1)

    first = client._send(Message("first"))
    with pytest.raises(TimeoutError, match="write queue is full"):
        client._send(Message("second"))

    assert not first.done()


def test_production_bus_writer_fails_immediately_when_disconnected():
    client = _queued_client()
    client.connected_event.clear()
    client.client.sock.connected = False

    with pytest.raises(ConnectionError, match="not connected"):
        client._send(Message("first"))

    assert client._bus_write_queue.empty()
    client._schedule_reconnect.assert_called_once()


def test_production_bus_writer_rejects_frames_after_shutdown_begins():
    """Never enqueue work after the only writer has been asked to stop."""
    client = _queued_client()
    client._ensure_reconnect_state()
    with client._reconnect_state_lock:
        client._close_requested = True
        client._bus_writer_stop.set()

    with pytest.raises(ConnectionError, match="is closing"):
        client._send(Message("too-late"))

    assert client._bus_write_queue.empty()


def test_canceled_queued_write_does_not_terminate_the_bus_writer():
    """Skip abandoned work and keep the single ordered writer available."""
    client = _queued_client()
    client._send_synchronously = MagicMock()
    canceled = client._send(Message("abandoned"))
    assert canceled.cancel()

    client._start_bus_writer()
    delivered = client._send(Message("still-live"))

    assert delivered.result(timeout=1) is None
    client._bus_writer_stop.set()
    client._bus_writer_thread.join(timeout=1)

    assert not client._bus_writer_thread.is_alive()
    client._send_synchronously.assert_called_once_with(
        Message("still-live"), reconnect=False
    )


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


def test_reconnect_wakeup_does_not_wait_for_stale_transport_close(monkeypatch):
    """A wedged WebSocketApp.close cannot strand the query recovery caller."""
    client = _client()
    client._message_send_timeout = 0.05
    release_close = Event()
    close_started = Event()

    def blocked_close():
        close_started.set()
        release_close.wait(1)

    client.client.close.side_effect = blocked_close
    client._reconnect_worker = MagicMock()
    client._reconnect_worker.is_alive.return_value = True

    started = time.monotonic()
    client._schedule_reconnect(TimeoutError("stale runtime path"))
    elapsed = time.monotonic() - started

    assert close_started.wait(0.1)
    assert elapsed < 0.2
    assert client.client.keep_running is False
    release_close.set()


def test_reconnect_supervisor_replaces_transport_after_bounded_close(monkeypatch):
    """The supervisor progresses even when the retired close never returns."""
    client = _client()
    client._ensure_reconnect_state()
    client._message_send_timeout = 0.05
    release_close = Event()
    client.client.close.side_effect = lambda: release_close.wait(1)
    replacement = MagicMock()
    client.create_client.return_value = replacement
    client._reconnect_error = TimeoutError("stale runtime path")
    monkeypatch.setattr(agent_module.time, "sleep", MagicMock())

    client._run_reconnect_loop()

    client.create_client.assert_called_once_with()
    client.run_forever.assert_called_once_with()
    assert client.client is replacement
    release_close.set()


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


def test_concurrent_query_frames_are_serialized_on_the_websocket():
    """Two HiveMind workers must never overlap websocket message writes."""
    client = _client()
    first_entered = Event()
    release_first = Event()
    state_lock = Lock()
    active = 0
    max_active = 0

    def blocking_send(_payload):
        nonlocal active, max_active
        with state_lock:
            active += 1
            max_active = max(max_active, active)
            if active == 1:
                first_entered.set()
        release_first.wait(0.5)
        with state_lock:
            active -= 1

    client.client.send.side_effect = blocking_send
    first = Thread(target=client._send, args=(Message("first"),))
    second = Thread(target=client._send, args=(Message("second"),))

    first.start()
    assert first_entered.wait(0.2)
    second.start()
    time.sleep(0.02)
    assert max_active == 1
    release_first.set()
    first.join(timeout=1)
    second.join(timeout=1)

    assert not first.is_alive()
    assert not second.is_alive()
    assert max_active == 1


def test_stalled_send_serialization_reconnects_and_retries_exact_frame(
        monkeypatch):
    """A blocked writer cannot strand every later HiveMind query worker."""
    client = _client()
    client._message_send_timeout = 0.1
    client._ensure_reconnect_state()
    assert client._send_lock.acquire(timeout=0.1)
    replacement = SimpleNamespace(
        sock=SimpleNamespace(connected=True),
        send=MagicMock(),
    )
    reconnect_errors = []

    def reconnect(error):
        reconnect_errors.append(error)
        client._send_lock.release()
        client.connected_event.clear()
        client.client = replacement
        client.connected_event.set()

    monkeypatch.setattr(client, "_schedule_reconnect", reconnect)
    second = Message("second", {"query_id": "query-2"})

    client._send(second)

    assert len(reconnect_errors) == 1
    assert isinstance(reconnect_errors[0], TimeoutError)
    assert "send serialization remained busy" in str(reconnect_errors[0])
    replacement.send.assert_called_once_with(second.serialize())


def test_stalled_frame_write_reconnects_and_retries_exact_frame(monkeypatch):
    """One serial query cannot hang forever inside websocket-client send."""
    client = _client()
    client._message_send_timeout = 0.1
    stale = client.client
    release_write = Event()
    stale.send.side_effect = lambda _payload: release_write.wait(1)
    stale.sock.shutdown = MagicMock(side_effect=release_write.set)
    replacement = SimpleNamespace(
        sock=SimpleNamespace(connected=True),
        send=MagicMock(),
    )
    reconnect_errors = []

    def reconnect(error):
        reconnect_errors.append(error)
        client.connected_event.clear()
        client.client = replacement
        client.connected_event.set()

    monkeypatch.setattr(client, "_schedule_reconnect", reconnect)
    message = Message("second", {"query_id": "query-2"})

    client._send(message)

    stale.sock.shutdown.assert_called_once_with()
    assert len(reconnect_errors) == 1
    assert isinstance(reconnect_errors[0], TimeoutError)
    assert "frame write remained blocked" in str(reconnect_errors[0])
    replacement.send.assert_called_once_with(message.serialize())


def test_send_reconnect_wait_is_hard_bounded(monkeypatch):
    """Never inherit ovos-bus-client's unbounded post-disconnect wait."""
    client = _client()
    client.client.sock.connected = False
    client.connected_event.clear()
    monkeypatch.setattr(client, "_schedule_reconnect", MagicMock())

    with pytest.raises(TimeoutError, match="did not reconnect"):
        client._send(Message("recognizer_loop:utterance", {}))

    client.client.send.assert_not_called()


def test_delivery_probe_accepts_only_intent_service_response():
    """Prove liveness with an application response, not a broker self-echo."""
    client = _client()
    client.emitter = EventEmitter()

    def respond(payload):
        message = Message.deserialize(payload)
        client.emitter.emit(message.msg_type, message)
        client.emitter.emit(
            "thalovant.runtime.probe.response",
            message.reply(
                "thalovant.runtime.probe.response",
                {"probe_id": message.data["probe_id"]},
            ),
        )

    client.client.send.side_effect = respond

    assert client._probe_delivery_once(0.05) is True
    assert client.client.send.call_count == 1


def test_delivery_probe_rejects_broker_self_echo_without_runtime_response():
    """A healthy broker alone cannot certify the OVOS application consumer."""
    client = _client()
    client.emitter = EventEmitter()

    def echo_only(payload):
        message = Message.deserialize(payload)
        client.emitter.emit(message.msg_type, message)

    client.client.send.side_effect = echo_only

    assert client._probe_delivery_once(0.01) is False


def test_confirmed_query_accepts_exact_post_transform_receipt():
    """Return only after the intent pipeline starts this exact query ID."""
    client = _client()
    client.emitter = EventEmitter()
    message = Message(
        "recognizer_loop:utterance",
        {"utterances": ["hello"]},
        {"query_id": "query-1"},
    )

    def acknowledge(payload):
        request = Message.deserialize(payload)
        if request.msg_type == "thalovant.runtime.query.prepare":
            response_type = "thalovant.runtime.query.prepared"
        else:
            response_type = "thalovant.runtime.query.started"
        client.emitter.emit(
            response_type,
            request.reply(
                response_type,
                {"query_id": "query-1", "duplicate": False},
            ),
        )

    client.client.send.side_effect = acknowledge

    client.emit_confirmed(message, 0.05)

    assert client.client.send.call_count == 2


def test_concurrent_confirmed_queries_keep_receipt_subscriptions_immutable():
    """Concurrent receipts dispatch by query ID without pyee mutations."""
    client = _client()
    client._message_send_timeout = 1
    client._delivery_recovery_timeout = 1
    client.emitter = EventEmitter()
    client._ensure_query_receipt_dispatchers()
    client.emitter.on = MagicMock(
        side_effect=AssertionError("query workers must not add handlers")
    )
    client.emitter.remove_listener = MagicMock(
        side_effect=AssertionError("query workers must not remove handlers")
    )

    def acknowledge(payload):
        request = Message.deserialize(payload)
        if request.msg_type == "thalovant.runtime.query.prepare":
            response_type = "thalovant.runtime.query.prepared"
            query_id = request.data["query_id"]
        else:
            response_type = "thalovant.runtime.query.started"
            query_id = request.context["query_id"]
        client.emitter.emit(
            response_type,
            request.reply(response_type, {"query_id": query_id}),
        )

    client.client.send.side_effect = acknowledge
    completed = []
    errors = []
    result_lock = Lock()

    def deliver(index):
        query_id = f"query-{index}"
        try:
            client.emit_confirmed(
                Message(
                    "recognizer_loop:utterance",
                    {"utterances": ["hello"]},
                    {"query_id": query_id},
                ),
                acceptance_timeout=0.5,
                recovery_timeout=1,
            )
            with result_lock:
                completed.append(query_id)
        except Exception as exc:  # pragma: no cover - asserted below
            with result_lock:
                errors.append(exc)

    workers = [Thread(target=deliver, args=(index,)) for index in range(25)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=3)

    assert all(not worker.is_alive() for worker in workers)
    assert errors == []
    assert len(completed) == 25
    assert client.client.send.call_count == 50
    client.emitter.on.assert_not_called()
    client.emitter.remove_listener.assert_not_called()
    assert client._query_receipt_events == {}


def test_confirmed_query_retries_until_the_runtime_consumer_recovers(monkeypatch):
    """Keep retrying the exact frame after one reconnect until core returns."""
    client = _client()
    client.emitter = EventEmitter()
    message = Message(
        "recognizer_loop:utterance",
        {"utterances": ["hello"]},
        {"query_id": "query-1"},
    )
    payloads = []

    def acknowledge_second(payload):
        request = Message.deserialize(payload)
        if request.msg_type == "thalovant.runtime.query.prepare":
            client.emitter.emit(
                "thalovant.runtime.query.prepared",
                request.reply(
                    "thalovant.runtime.query.prepared",
                    {"query_id": "query-1"},
                ),
            )
            return
        if request.msg_type != "recognizer_loop:utterance":
            return
        payloads.append(payload)
        if len(payloads) == 3:
            client.emitter.emit(
                "thalovant.runtime.query.started",
                request.reply(
                    "thalovant.runtime.query.started",
                    {"query_id": "query-1", "duplicate": True},
                ),
            )

    client.client.send.side_effect = acknowledge_second
    monkeypatch.setattr(client, "_schedule_reconnect", MagicMock())
    monkeypatch.setattr(
        client, "_wait_for_live_transport", MagicMock(return_value=True)
    )

    client.emit_confirmed(message, 0.01, recovery_timeout=0.2)

    assert payloads[0] == payloads[1] == payloads[2]
    client._schedule_reconnect.assert_called_once()


def test_confirmed_query_stages_share_one_total_recovery_budget(monkeypatch):
    """Reservation and pipeline start share one recovery window."""
    client = _client()
    client.emitter = EventEmitter()
    message = Message(
        "recognizer_loop:utterance",
        {"utterances": ["hello"]},
        {"query_id": "query-1"},
    )

    def acknowledge_before_pipeline_start(payload):
        request = Message.deserialize(payload)
        if request.msg_type == "thalovant.runtime.query.prepare":
            client.emitter.emit(
                "thalovant.runtime.query.prepared",
                request.reply(
                    "thalovant.runtime.query.prepared",
                    {"query_id": "query-1"},
                ),
            )
        elif request.msg_type == "recognizer_loop:utterance":
            client.emitter.emit(
                "thalovant.runtime.query.accepted",
                request.reply(
                    "thalovant.runtime.query.accepted",
                    {"query_id": "query-1", "duplicate": False},
                ),
            )

    client.client.send.side_effect = acknowledge_before_pipeline_start
    monkeypatch.setattr(client, "_schedule_reconnect", MagicMock())
    monkeypatch.setattr(
        client, "_wait_for_live_transport", MagicMock(return_value=True)
    )

    started = time.monotonic()
    with pytest.raises(TimeoutError, match="delivery window"):
        client.emit_confirmed(
            message,
            acceptance_timeout=0.01,
            recovery_timeout=0.05,
        )

    assert time.monotonic() - started < 0.15
    client._schedule_reconnect.assert_called_once()


def test_delivery_probe_reconnects_before_any_user_message(monkeypatch):
    """Replace a silent half-open path, then accept the fresh bus echo."""
    client = _client()
    probe = MagicMock(side_effect=[False, False, True])
    reconnect = MagicMock()
    monkeypatch.setattr(client, "_probe_delivery_once", probe)
    monkeypatch.setattr(client, "_schedule_reconnect", reconnect)
    monkeypatch.setattr(client, "_wait_for_live_transport", MagicMock(
        return_value=True
    ))

    client.ensure_delivery_path(0.01)

    assert probe.call_args_list == [call(0.01), call(0.01), call(0.01)]
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

    with pytest.raises(TimeoutError, match="did not answer"):
        client.ensure_delivery_path(0.01)

    assert client._schedule_reconnect.call_count == 1


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


def _acknowledging_sender(client, query_id, *, failures):
    """Fail the first ``failures`` frame writes, then answer every stage."""
    writes = []

    def send(payload):
        writes.append(payload)
        if len(writes) <= failures:
            raise WebSocketConnectionClosedException("runtime bus restarted")
        request = Message.deserialize(payload)
        if request.msg_type == "thalovant.runtime.query.prepare":
            response_type = "thalovant.runtime.query.prepared"
        elif request.msg_type == "recognizer_loop:utterance":
            response_type = "thalovant.runtime.query.started"
        else:
            raise AssertionError(
                f"unexpected confirmed-query frame: {request.msg_type}"
            )
        client.emitter.emit(
            response_type,
            request.reply(response_type, {"query_id": query_id}),
        )

    return send, writes


def test_confirmed_query_retries_a_transient_send_failure(monkeypatch):
    """A send that fails mid-window is retried, not raised to the caller.

    Two failed writes exhaust the synchronous send's own single retry, so
    ``emit`` raises ``ConnectionError`` into ``emit_confirmed``. The shared
    delivery window exists for exactly that stale path.
    """
    client = _client()
    client.emitter = EventEmitter()
    message = Message(
        "recognizer_loop:utterance",
        {"utterances": ["hello"]},
        {"query_id": "query-1"},
    )
    send, writes = _acknowledging_sender(client, "query-1", failures=2)
    client.client.send.side_effect = send
    monkeypatch.setattr(client, "_schedule_reconnect", MagicMock())
    monkeypatch.setattr(
        client, "_wait_for_live_transport", MagicMock(return_value=True)
    )

    client.emit_confirmed(message, 0.05, recovery_timeout=1.0)

    prepares = [
        payload for payload in writes
        if Message.deserialize(payload).msg_type
        == "thalovant.runtime.query.prepare"
    ]
    # The exact reservation frame was written three times: twice failing,
    # once acknowledged. The pipeline start then went through first time.
    assert len(prepares) == 3 and len(set(prepares)) == 1
    assert len(writes) == 4
    client._schedule_reconnect.assert_called()


def test_confirmed_query_does_not_spin_on_an_open_transport(monkeypatch):
    """A failure that leaves the transport open waits before each retry."""
    client = _client()
    client.emitter = EventEmitter()
    message = Message(
        "recognizer_loop:utterance",
        {"utterances": ["hello"]},
        {"query_id": "query-1"},
    )
    emit = MagicMock(side_effect=TimeoutError("write queue is full"))
    monkeypatch.setattr(client, "emit", emit)
    monkeypatch.setattr(client, "_schedule_reconnect", MagicMock())
    monkeypatch.setattr(
        client, "_wait_for_live_transport", MagicMock(return_value=True)
    )

    with pytest.raises(TimeoutError, match="delivery window"):
        client.emit_confirmed(message, 0.05, recovery_timeout=0.35)

    # One attempt per retry pause, not thousands in a busy loop.
    assert 2 <= emit.call_count <= 6
    client._schedule_reconnect.assert_called_once()


def test_confirmed_query_surfaces_a_send_failure_during_shutdown(monkeypatch):
    """An intentional close is not a stale path: fail now, not at the deadline."""
    client = _client()
    client.emitter = EventEmitter()
    client._ensure_reconnect_state()
    client._close_requested = True
    emit = MagicMock(side_effect=ConnectionError("OVOS messagebus is closing"))
    monkeypatch.setattr(client, "emit", emit)
    monkeypatch.setattr(client, "_wait_for_live_transport", MagicMock())
    message = Message(
        "recognizer_loop:utterance",
        {"utterances": ["hello"]},
        {"query_id": "query-1"},
    )

    started = time.monotonic()
    with pytest.raises(ConnectionError, match="closing"):
        client.emit_confirmed(message, 0.05, recovery_timeout=5.0)

    assert time.monotonic() - started < 1.0
    emit.assert_called_once()
    client._wait_for_live_transport.assert_not_called()


def test_confirmed_query_surfaces_a_non_transient_send_failure(monkeypatch):
    """Only stale-path failures are absorbed; a programming error is not."""
    client = _client()
    client.emitter = EventEmitter()
    monkeypatch.setattr(
        client, "emit", MagicMock(side_effect=ValueError("bad frame"))
    )
    message = Message(
        "recognizer_loop:utterance",
        {"utterances": ["hello"]},
        {"query_id": "query-1"},
    )

    with pytest.raises(ValueError, match="bad frame"):
        client.emit_confirmed(message, 0.05, recovery_timeout=5.0)


def test_delivery_probe_retries_a_transient_send_failure(monkeypatch):
    """A probe that raises spends the recovery window instead of escaping it."""
    client = _client()
    client._delivery_recovery_timeout = 1.0
    probe = MagicMock(side_effect=[
        ConnectionError("bus closed while sending"),
        TimeoutError("frame write remained blocked"),
        True,
    ])
    reconnect = MagicMock()
    monkeypatch.setattr(client, "_probe_delivery_once", probe)
    monkeypatch.setattr(client, "_schedule_reconnect", reconnect)
    monkeypatch.setattr(client, "_wait_for_live_transport", MagicMock(
        return_value=True
    ))

    client.ensure_delivery_path(0.01)

    assert probe.call_count == 3
    reconnect.assert_called_once()


def test_delivery_probe_surfaces_a_non_transient_failure(monkeypatch):
    """A probe error that is not a stale path reaches the caller untouched."""
    client = _client()
    client._delivery_recovery_timeout = 1.0
    monkeypatch.setattr(client, "_probe_delivery_once", MagicMock(
        side_effect=ValueError("probe is misconfigured")
    ))
    monkeypatch.setattr(client, "_schedule_reconnect", MagicMock())
    monkeypatch.setattr(client, "_wait_for_live_transport", MagicMock(
        return_value=True
    ))

    with pytest.raises(ValueError, match="misconfigured"):
        client.ensure_delivery_path(0.01)
    client._schedule_reconnect.assert_not_called()


def test_receipt_dispatchers_follow_a_replaced_emitter():
    """Receipt handlers are installed on the emitter in use, not the first one."""
    client = _client()
    first = EventEmitter()
    client.emitter = first
    client._ensure_query_receipt_dispatchers()
    assert first.listeners("thalovant.runtime.query.started")

    replacement = EventEmitter()
    client.emitter = replacement
    received = client._register_query_receipt(
        "thalovant.runtime.query.started", "query-1"
    )
    try:
        replacement.emit(
            "thalovant.runtime.query.started",
            Message("thalovant.runtime.query.started", {"query_id": "query-1"}),
        )
        assert received.is_set()
    finally:
        client._unregister_query_receipt(
            "thalovant.runtime.query.started", "query-1", received
        )
    # Installing again on the same emitter stays a no-op.
    client._ensure_query_receipt_dispatchers()
    assert len(replacement.listeners("thalovant.runtime.query.started")) == 1


def test_receipt_dispatcher_rollback_failure_is_logged(monkeypatch):
    """A listener left behind by a failed rollback is reported, not hidden."""
    client = _client()
    emitter = MagicMock()
    emitter.on.side_effect = [None, RuntimeError("emitter rejected listener")]
    emitter.remove_listener.side_effect = KeyError("already gone")
    client.emitter = emitter
    debug = MagicMock()
    monkeypatch.setattr(agent_module.LOG, "debug", debug)

    with pytest.raises(RuntimeError, match="rejected listener"):
        client._ensure_query_receipt_dispatchers()

    debug.assert_called_once()
    assert "roll back query receipt listener" in debug.call_args.args[0]
