import dataclasses
import logging
import math
import queue
import time
import uuid
from concurrent.futures import Future
from copy import deepcopy
from threading import Event, Lock, Thread, current_thread
from typing import Callable, Dict, Any, Iterator, Optional, Set

from ovos_bus_client import MessageBusClient
from ovos_bus_client.client.client import _maybe_encrypt
from ovos_bus_client.message import Message
from ovos_config import Configuration
from ovos_spec_tools.session import SESSION1_REGISTERED_FIELDS
from ovos_utils import json_dumps
from ovos_utils.fakebus import FakeBus
from ovos_utils.log import LOG
from pyee import EventEmitter
from websocket import (WebSocketConnectionClosedException, WebSocketException,
                       WebSocketTimeoutException)

from hivemind_bus_client.message import HiveMessage, HiveMessageType
from hivemind_plugin_manager.protocols import AgentProtocol

from hivemind_ovos_agent_plugin.policy import (AddBlacklistedIntent,
                                                AddBlacklistedSkill,
                                                OVOSAgentPolicy,
                                                RewriteUtterance,
                                                SetContextField,
                                                SetSessionField)
from hivemind_ovos_agent_plugin._metrics import (
    BUS_WRITE,
    BUS_WRITE_QUEUE,
    SKILL_HANDLER,
)
from hivemind_ovos_agent_plugin.version import __version__


__all__ = [
    "AddBlacklistedIntent",
    "AddBlacklistedSkill",
    "OVOSAgentPolicy",
    "OVOSAgentProtocol",
    "RewriteUtterance",
    "SetContextField",
    "SetSessionField",
    "__version__",
]


class _TransientWebsocketDisconnectFilter(logging.Filter):
    """Downgrade websocket-client's pre-callback rollout noise to INFO.

    ``websocket-client`` logs an unconditional ERROR ending in ``goodbye``
    after it invokes ``on_error``.  The runtime client owns the bounded
    reconnect and escalation policy, so known transport-level rollout
    failures must not be reported as independent application errors first.
    """

    _TRANSIENT_FRAGMENTS = (
        "connection to remote host was lost",
        "operation not permitted",
        "connection refused",
        "connection reset by peer",
        "broken pipe",
        "timed out",
    )

    def filter(self, record):
        """Retain every record while lowering only known transient messages."""
        message = record.getMessage().lower()
        if (record.levelno >= logging.ERROR
                and message.endswith("- goodbye")
                and any(item in message
                        for item in self._TRANSIENT_FRAGMENTS)):
            record.levelno = logging.INFO
            record.levelname = logging.getLevelName(logging.INFO)
        return True


def _install_websocket_disconnect_log_filter():
    """Install the process-wide websocket filter exactly once."""
    logger = logging.getLogger("websocket")
    if not any(
        isinstance(item, _TransientWebsocketDisconnectFilter)
        for item in logger.filters
    ):
        logger.addFilter(_TransientWebsocketDisconnectFilter())


class _RuntimeMessageBusClient(MessageBusClient):
    """Reconnect quietly while a managed OVOS runtime is being replaced.

    Kubernetes can reject a connection with ``EPERM`` while a Service has no
    ready endpoints.  That is an expected, bounded condition during a serial
    runtime rollout, not an application traceback.  Keep retrying at INFO and
    escalate once when the outage exceeds the configured recovery budget.
    """

    def __init__(self, *args, reconnect_error_after=120,
                 message_send_timeout=15, ping_interval=15,
                 ping_timeout=5, delivery_recovery_timeout=20,
                 bus_write_queue_size=256, **kwargs):
        """Initialize bounded reconnect state before creating the bus client."""
        _install_websocket_disconnect_log_filter()
        self._disconnect_started_at = None
        self._disconnect_escalated = False
        self._reconnect_state_lock = Lock()
        self._send_lock = Lock()
        self._bus_write_queue = queue.Queue(
            maxsize=self._positive_int(bus_write_queue_size, 256)
        )
        self._bus_writer_stop = Event()
        self._bus_writer_thread = None
        self._query_receipt_events = {}
        self._query_receipt_lock = Lock()
        self._query_receipt_dispatchers = {}
        self._query_receipt_dispatcher_setup_lock = Lock()
        self._reconnect_worker = None
        self._reconnect_error = None
        self._close_requested = False
        try:
            reconnect_error_after = float(reconnect_error_after)
        except (TypeError, ValueError):
            reconnect_error_after = 120.0
        self._reconnect_error_after = max(reconnect_error_after, 1.0)
        self._message_send_timeout = self._positive_float(
            message_send_timeout, 15.0
        )
        self._delivery_recovery_timeout = self._positive_float(
            delivery_recovery_timeout, 20.0
        )
        self._ping_interval = self._positive_float(ping_interval, 15.0)
        self._ping_timeout = self._positive_float(ping_timeout, 5.0)
        if self._ping_timeout >= self._ping_interval:
            self._ping_timeout = max(1.0, self._ping_interval / 2)
        super().__init__(*args, **kwargs)
        # Install the fixed receipt topology before the websocket receive
        # thread starts. Query workers must never mutate pyee registrations
        # while that thread is dispatching a runtime response.
        self._ensure_query_receipt_dispatchers()
        self._start_bus_writer()

    @staticmethod
    def _positive_float(value, default):
        """Return a finite positive float suitable for timeout settings."""
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return default
        return parsed if math.isfinite(parsed) and parsed > 0 else default

    @staticmethod
    def _positive_int(value, default):
        """Return a positive integer suitable for bounded queue settings."""
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return default
        return parsed if parsed > 0 else default

    @staticmethod
    def _error_from_args(args):
        """Return the websocket error from supported callback signatures."""
        return args[0] if len(args) == 1 else args[1]

    @staticmethod
    def _is_transient_disconnect(error):
        """Return whether an error should enter the bounded reconnect path."""
        return isinstance(error, (
            ConnectionError,
            PermissionError,
            TimeoutError,
            WebSocketConnectionClosedException,
            WebSocketTimeoutException,
        ))

    def on_open(self, *args):
        """Reset the outage budget after the upstream client reconnects."""
        self._disconnect_started_at = None
        self._disconnect_escalated = False
        self.retry = 5
        return super().on_open(*args)

    def close(self):
        """Mark an intentional shutdown so close callbacks cannot reconnect."""
        self._ensure_reconnect_state()
        with self._reconnect_state_lock:
            self._close_requested = True
        writer_stop = getattr(self, "_bus_writer_stop", None)
        if writer_stop is not None:
            writer_stop.set()
        result = super().close()
        writer = getattr(self, "_bus_writer_thread", None)
        if writer is not None and writer is not current_thread():
            writer.join(timeout=1.0)
        return result

    def _start_bus_writer(self):
        """Start the single ordered OVOS-bus writer exactly once."""
        writer = self._bus_writer_thread
        if writer is not None and writer.is_alive():
            return
        writer = Thread(
            target=self._run_bus_writer,
            name="ovos-runtime-bus-writer",
            daemon=True,
        )
        self._bus_writer_thread = writer
        writer.start()

    def _run_bus_writer(self):
        """Drain application frames in FIFO order outside caller threads."""
        while (not self._bus_writer_stop.is_set()
               or not self._bus_write_queue.empty()):
            try:
                enqueued_at, message, completion = self._bus_write_queue.get(
                    timeout=0.1
                )
            except queue.Empty:
                continue
            BUS_WRITE_QUEUE.observe_ms(
                (time.monotonic() - enqueued_at) * 1000
            )
            write_started = time.monotonic()
            try:
                self._send_synchronously(message, reconnect=False)
            except Exception as error:
                completion.set_exception(error)
                LOG.warning(
                    "OVOS bus writer rejected %s: %s",
                    getattr(message, "msg_type", type(message).__name__),
                    error,
                )
            else:
                completion.set_result(None)
            finally:
                BUS_WRITE.observe_ms(
                    (time.monotonic() - write_started) * 1000
                )
                self._bus_write_queue.task_done()

    def _ensure_reconnect_state(self):
        """Initialize reconnect state for normal and test-constructed clients."""
        if not hasattr(self, "_reconnect_state_lock"):
            self._reconnect_state_lock = Lock()
            self._reconnect_worker = None
            self._reconnect_error = None
            self._close_requested = False
        if not hasattr(self, "_send_lock"):
            self._send_lock = Lock()

    def _ensure_query_receipt_dispatchers(self):
        """Install immutable query-receipt handlers exactly once per bus."""
        if not hasattr(self, "_query_receipt_events"):
            self._query_receipt_events = {}
            self._query_receipt_lock = Lock()
            self._query_receipt_dispatchers = {}
            self._query_receipt_dispatcher_setup_lock = Lock()
        with self._query_receipt_dispatcher_setup_lock:
            if self._query_receipt_dispatchers:
                return
            def dispatch(event_type):
                def _handler(response):
                    self._dispatch_query_receipt(event_type, response)
                return _handler

            installed = []
            for response_type in (
                    "thalovant.runtime.query.prepared",
                    "thalovant.runtime.query.started"):
                callback = dispatch(response_type)
                try:
                    self.emitter.on(response_type, callback)
                except Exception:
                    for installed_type, installed_callback in reversed(
                            installed):
                        try:
                            self.emitter.remove_listener(
                                installed_type, installed_callback
                            )
                        except Exception:
                            pass
                    raise
                installed.append((response_type, callback))
            self._query_receipt_dispatchers = dict(installed)

    def _dispatch_query_receipt(self, response_type, response):
        """Wake only waiters for the exact query receipt."""
        if (not isinstance(response, Message)
                or not isinstance(response.data, dict)):
            return
        query_id = response.data.get("query_id")
        if not isinstance(query_id, str) or not query_id:
            return
        with self._query_receipt_lock:
            waiters = tuple(
                self._query_receipt_events.get(
                    (response_type, query_id), ()
                )
            )
        for waiter in waiters:
            waiter.set()

    def _register_query_receipt(self, response_type, query_id):
        """Register one query worker without changing emitter topology."""
        self._ensure_query_receipt_dispatchers()
        received = Event()
        with self._query_receipt_lock:
            self._query_receipt_events.setdefault(
                (response_type, query_id), set()
            ).add(received)
        return received

    def _unregister_query_receipt(self, response_type, query_id, received):
        """Remove one waiter from the agent-owned receipt registry."""
        with self._query_receipt_lock:
            waiters = self._query_receipt_events.get(
                (response_type, query_id)
            )
            if waiters is None:
                return
            waiters.discard(received)
            if not waiters:
                self._query_receipt_events.pop(
                    (response_type, query_id), None
                )

    def _close_stale_transport(self, transport):
        """Wake one stale websocket without blocking a query or supervisor.

        ``websocket-client`` may wait indefinitely in ``WebSocketApp.close``
        when another thread is stuck in a frame write.  Recovery only needs to
        retire that exact transport; the reconnect worker owns its replacement.
        Mark the old run loop stopped immediately and bound the best-effort
        close on a daemon thread.
        """
        if transport is None:
            return True
        try:
            transport.keep_running = False
        except Exception:
            pass

        completed = Event()

        def _close():
            try:
                transport.close()
            except Exception as exc:
                LOG.info("Could not close stale OVOS bus transport: %s", exc)
            finally:
                completed.set()

        closer = Thread(
            target=_close,
            name="ovos-runtime-transport-close",
            daemon=True,
        )
        closer.start()
        timeout = min(
            getattr(self, "_message_send_timeout", 15.0),
            1.0,
        )
        if completed.wait(timeout):
            return True
        LOG.info(
            "OVOS bus transport close exceeded %.1f seconds; continuing "
            "bounded recovery",
            timeout,
        )
        return False

    def _schedule_reconnect(self, error):
        """Start one durable reconnect worker for error and clean-close paths."""
        self._ensure_reconnect_state()
        was_connected = self.connected_event.is_set()
        self.connected_event.clear()
        stale_transport = None
        with self._reconnect_state_lock:
            if self._close_requested:
                return
            self._reconnect_error = error
            if (self._reconnect_worker is not None
                    and self._reconnect_worker.is_alive()):
                # After its first successful reconnect the supervisor is
                # blocked inside WebSocketApp.run_forever().  A send from a
                # query thread may be the first code to notice a half-open
                # socket, so close that stale transport to wake the existing
                # supervisor instead of leaving it unable to consume the new
                # reconnect request.
                if (was_connected
                        and self._reconnect_worker is not current_thread()):
                    stale_transport = self.client
            else:
                self._reconnect_worker = Thread(
                    target=self._run_reconnect_loop,
                    name="ovos-runtime-bus-reconnect",
                    daemon=True,
                )
                self._reconnect_worker.start()
        if stale_transport is not None:
            self._close_stale_transport(stale_transport)

    def _transport_is_open(self):
        """Return whether websocket-client still has a live transport."""
        sock = getattr(self.client, "sock", None)
        return bool(sock is not None and getattr(sock, "connected", False))

    def _wait_for_live_transport(self, deadline):
        """Bound reconnection waits so a query worker can never block forever."""
        if self.connected_event.is_set() and self._transport_is_open():
            return True
        self._schedule_reconnect(
            WebSocketConnectionClosedException(
                "OVOS message bus transport is not connected"
            )
        )
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            if self.connected_event.wait(min(remaining, 0.25)):
                if self._transport_is_open():
                    return True
                self.connected_event.clear()

    @staticmethod
    def _send_frame_with_timeout(transport, payload, timeout, message_type):
        """Write one websocket frame without trusting a blocking socket call."""
        completed = Event()
        errors = []

        def _write_frame():
            try:
                transport.send(payload)
            except Exception as exc:
                errors.append(exc)
            finally:
                completed.set()

        writer = Thread(
            target=_write_frame,
            name="ovos-runtime-frame-write",
            daemon=True,
        )
        writer.start()
        if not completed.wait(timeout):
            # Closing the exact websocket that owns the blocked write wakes its
            # daemon thread and lets the normal reconnect path replace it.
            websocket = getattr(transport, "sock", None)
            shutdown = getattr(websocket, "shutdown", None)
            if callable(shutdown):
                try:
                    shutdown()
                except Exception:
                    pass
            raise TimeoutError(
                "OVOS message bus frame write remained blocked while sending "
                f"{message_type}"
            )
        if errors:
            raise errors[0]

    def _send(self, message):
        """Queue one bus frame without blocking the HiveMind caller thread."""
        # Retain direct behavior for legacy subclasses and test instances that
        # bypass ``__init__``. Fully initialized production clients always own
        # the bounded writer queue.
        if not hasattr(self, "_bus_write_queue"):
            return self._send_synchronously(message)
        if not self.connected_event.is_set() or not self._transport_is_open():
            error = ConnectionError("OVOS messagebus is not connected")
            self._schedule_reconnect(error)
            raise error
        completion = Future()
        try:
            self._bus_write_queue.put_nowait(
                (time.monotonic(), message, completion)
            )
        except queue.Full as error:
            raise TimeoutError("OVOS messagebus write queue is full") from error
        return completion

    def _send_synchronously(self, message, *, reconnect=True):
        """Send once, reconnecting a stale runtime socket within a hard bound.

        ``ovos-bus-client`` waits without a timeout after its initial ten
        seconds and swallows ``WebSocketConnectionClosedException``.  In a
        HiveMind query pool that strands one worker per request indefinitely.
        Keep the wire representation identical while making availability and
        delivery failures explicit and retrying one frame after reconnection.
        """
        self._ensure_reconnect_state()
        if hasattr(message, "serialize"):
            payload = message.serialize()
            message_type = getattr(message, "msg_type", "message")
        else:
            payload = json_dumps(message.__dict__)
            message_type = type(message).__name__
        payload = _maybe_encrypt(payload)
        deadline = time.monotonic() + self._message_send_timeout
        last_error = None

        attempts = 2 if reconnect else 1
        for attempt in range(attempts):
            if reconnect and not self._wait_for_live_transport(deadline):
                raise TimeoutError(
                    "OVOS message bus did not reconnect within "
                    f"{self._message_send_timeout:.1f} seconds"
                ) from last_error
            if (not reconnect
                    and (not self.connected_event.is_set()
                         or not self._transport_is_open())):
                raise ConnectionError("OVOS messagebus is not connected")
            lock_acquired = False
            try:
                # websocket-client does not make the message-level ordering
                # contract visible here. Serialize all application frames so
                # concurrent HiveMind query workers cannot interleave sends.
                # A stuck websocket write must not leave every later query
                # blocked forever behind this lock. Bound lock acquisition,
                # replace the stale transport, and retry the exact frame.
                remaining = deadline - time.monotonic()
                # Preserve at least half of the end-to-end send budget for
                # closing the stale path and retrying on its replacement.
                lock_wait = min(1.0, max(remaining / 2, 0.0))
                if lock_wait <= 0 or not self._send_lock.acquire(
                        timeout=lock_wait):
                    raise TimeoutError(
                        "OVOS message bus send serialization remained busy "
                        f"while sending {message_type}"
                    )
                lock_acquired = True
                remaining = deadline - time.monotonic()
                frame_wait = min(1.0, max(remaining / 2, 0.0))
                if frame_wait <= 0:
                    raise TimeoutError(
                        "OVOS message bus send budget expired while sending "
                        f"{message_type}"
                    )
                transport = self.client
                self._send_frame_with_timeout(
                    transport,
                    payload,
                    frame_wait,
                    message_type,
                )
                return
            except Exception as exc:
                if not self._is_transient_disconnect(exc):
                    LOG.exception(
                        "Failed to emit OVOS message %s", message_type
                    )
                    raise
                last_error = exc
                self._schedule_reconnect(exc)
                if reconnect and attempt == 0:
                    continue
                raise ConnectionError(
                    f"OVOS message bus closed while sending {message_type}"
                ) from exc
            finally:
                if lock_acquired:
                    self._send_lock.release()

    def _probe_delivery_once(self, timeout):
        """Prove the OVOS intent service consumed and answered one probe."""
        probe_id = uuid.uuid4().hex
        probe_type = "thalovant.runtime.probe"
        response_type = "thalovant.runtime.probe.response"
        received = Event()

        def _on_probe(message):
            if (isinstance(message, Message)
                    and isinstance(message.data, dict)
                    and message.data.get("probe_id") == probe_id):
                received.set()

        self.emitter.on(response_type, _on_probe)
        try:
            self.emit(Message(probe_type, {"probe_id": probe_id}))
            return received.wait(timeout)
        finally:
            try:
                self.emitter.remove_listener(response_type, _on_probe)
            except (KeyError, ValueError):
                pass

    def emit_confirmed(self, message, acceptance_timeout=2,
                       recovery_timeout=None):
        """Deliver one query with a post-transform receipt and exact retry.

        Reserve ``query_id`` without a side effect, then require the runtime to
        confirm that the exact utterance passed its bounded transformer stage.
        If a frame or receipt is lost, reconnect once and repeat that
        idempotent stage.
        """
        context = message.context if isinstance(message.context, dict) else {}
        query_id = context.get("query_id")
        if not isinstance(query_id, str) or not query_id:
            raise ValueError("confirmed OVOS query requires a non-empty query_id")

        timeout = self._positive_float(acceptance_timeout, 2.0)
        prepare = Message(
            "thalovant.runtime.query.prepare", {"query_id": query_id}
        )
        stages = (
            (prepare, "thalovant.runtime.query.prepared", "reservation"),
            (message, "thalovant.runtime.query.started", "pipeline start"),
        )

        if recovery_timeout is None:
            recovery_timeout = getattr(
                self, "_delivery_recovery_timeout", 20.0
            )
        recovery_timeout = self._positive_float(recovery_timeout, 20.0)
        # Reservation and pipeline start are one delivery transaction. One
        # deadline prevents independent recovery windows from exceeding the
        # caller's complete query timeout.
        deadline = time.monotonic() + recovery_timeout
        for request, response_type, stage in stages:
            received = self._register_query_receipt(
                response_type, query_id
            )
            reconnected = False
            try:
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError(
                            "OVOS intent service did not confirm query "
                            f"{stage} for {query_id} within the bounded "
                            f"{recovery_timeout:.1f}-second delivery window"
                        )
                    received.clear()
                    self.emit(request)
                    remaining = deadline - time.monotonic()
                    if remaining > 0 and received.wait(min(timeout, remaining)):
                        break

                    last_error = TimeoutError(
                        "OVOS intent service did not confirm query "
                        f"{stage} for {query_id} within {timeout:.1f} seconds"
                    )
                    if not reconnected:
                        self._schedule_reconnect(last_error)
                        reconnected = True
                        LOG.info(
                            "OVOS intent-service query %s receipt was not "
                            "observed; reconnecting once and retrying the "
                            "exact frame within the shared %.1f-second "
                            "delivery window",
                            stage,
                            recovery_timeout,
                        )
                        transport_deadline = min(
                            deadline,
                            time.monotonic() + self._message_send_timeout,
                        )
                        self._wait_for_live_transport(transport_deadline)
                    if time.monotonic() < deadline:
                        continue
                    last_error = TimeoutError(
                        "OVOS intent service did not confirm query "
                        f"{stage} for {query_id} within the bounded "
                        f"{recovery_timeout:.1f}-second delivery window"
                    )
                    LOG.error("%s", last_error)
                    raise last_error
            finally:
                self._unregister_query_receipt(
                    response_type, query_id, received
                )

    def ensure_delivery_path(self, probe_timeout=2):
        """Reconnect a half-open bus before emitting a user utterance.

        A websocket can remain locally ``connected`` after a Kubernetes
        endpoint replacement while writes disappear into the retired path.
        Transport ping/pong, broker self-echo, and send exceptions cannot prove
        that the intent service consumes frames. Probe the application first,
        replace the path once on failure, and require a response from the fresh
        path before the user utterance.
        """
        timeout = self._positive_float(probe_timeout, 2.0)
        recovery_timeout = getattr(
            self, "_delivery_recovery_timeout", 20.0
        )
        deadline = time.monotonic() + recovery_timeout
        last_error = None
        reconnected = False
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            if self._probe_delivery_once(min(timeout, remaining)):
                return

            last_error = TimeoutError(
                "OVOS intent service did not answer the delivery probe within "
                f"{timeout:.1f} seconds"
            )
            if not reconnected:
                self._schedule_reconnect(last_error)
                reconnected = True
                LOG.info(
                    "OVOS message bus delivery path is stale; reconnecting "
                    "once and probing for up to %.1f seconds before the user "
                    "utterance",
                    recovery_timeout,
                )
                transport_deadline = min(
                    deadline,
                    time.monotonic() + self._message_send_timeout,
                )
                self._wait_for_live_transport(transport_deadline)
        last_error = TimeoutError(
            "OVOS intent service did not answer the delivery probe within the "
            f"bounded {recovery_timeout:.1f}-second recovery window"
        )
        LOG.error("%s", last_error)
        raise last_error

    def run_forever(self):
        """Run with ping/pong liveness so half-open sockets are bounded."""
        self.started_running = True
        return self.client.run_forever(
            ping_interval=self._ping_interval,
            ping_timeout=self._ping_timeout,
        )

    def _run_reconnect_loop(self):
        """Reconnect after every observed close without recursive run loops."""
        while True:
            with self._reconnect_state_lock:
                if self._close_requested:
                    self._reconnect_worker = None
                    return
                error = self._reconnect_error or WebSocketConnectionClosedException(
                    "OVOS message bus connection closed"
                )
                self._reconnect_error = None

            now = time.monotonic()
            if self._disconnect_started_at is None:
                self._disconnect_started_at = now
            elapsed = now - self._disconnect_started_at
            if (elapsed >= self._reconnect_error_after
                    and not self._disconnect_escalated):
                LOG.error(
                    "OVOS message bus remained unavailable for %.1f seconds: %r",
                    elapsed,
                    error,
                )
                self._disconnect_escalated = True
            else:
                LOG.info(
                    "OVOS message bus is temporarily unavailable; retrying in "
                    "%.1f seconds (%s)",
                    self.retry,
                    type(error).__name__,
                )

            stale_transport = self.client
            if getattr(stale_transport, "keep_running", False):
                self._close_stale_transport(stale_transport)

            time.sleep(self.retry)
            self.retry = min(self.retry * 2, 60)
            with self._reconnect_state_lock:
                if self._close_requested:
                    self._reconnect_worker = None
                    return
                # Ignore the close callback caused by closing the stale
                # transport above; only callbacks from the new run matter.
                self._reconnect_error = None
            try:
                self.emitter.emit("reconnecting")
                self.client = self.create_client()
                self.run_forever()
            except WebSocketException as exc:
                with self._reconnect_state_lock:
                    self._reconnect_error = exc

            with self._reconnect_state_lock:
                if self._close_requested:
                    self._reconnect_worker = None
                    return
                if self._reconnect_error is None:
                    # A mocked or externally stopped run loop returned without
                    # a close/error callback; do not spin indefinitely.
                    self._reconnect_worker = None
                    return

    def on_close(self, *args):
        """Reconnect when the runtime bus closes its websocket cleanly."""
        super().on_close(*args)
        self._schedule_reconnect(
            WebSocketConnectionClosedException(
                "OVOS message bus connection closed cleanly"
            )
        )

    def on_error(self, *args):
        """Reconnect transient disconnects and preserve other error handling."""
        error = self._error_from_args(args)
        if not self._is_transient_disconnect(error):
            return super().on_error(*args)
        self._schedule_reconnect(error)


@dataclasses.dataclass()
class OVOSAgentProtocol(AgentProtocol):
    """HiveMind agent protocol that bridges client messages to an OVOS bus."""
    bus: MessageBusClient = dataclasses.field(default_factory=FakeBus)
    config: Dict[str, Any] = dataclasses.field(default_factory=lambda: Configuration().get("websocket", {}))
    _owned_bus: Optional[MessageBusClient] = dataclasses.field(
        default=None, init=False, repr=False, compare=False
    )
    _active_query_scopes: Dict[str, Set[str]] = dataclasses.field(
        default_factory=dict, init=False, repr=False
    )
    _active_query_scopes_lock: Lock = dataclasses.field(
        default_factory=Lock, init=False, repr=False
    )
    _active_query_callbacks: Dict[str, Dict[str, Callable]] = dataclasses.field(
        default_factory=dict, init=False, repr=False
    )
    _active_query_scope_index: Dict[str, Set[str]] = dataclasses.field(
        default_factory=dict, init=False, repr=False
    )
    _query_dispatcher_buses: Set[int] = dataclasses.field(
        default_factory=set, init=False, repr=False
    )
    _query_dispatcher_handlers: Dict[int, list] = dataclasses.field(
        default_factory=dict, init=False, repr=False
    )
    _query_dispatcher_setup_lock: Lock = dataclasses.field(
        default_factory=Lock, init=False, repr=False
    )
    _skill_handler_starts: Dict[str, float] = dataclasses.field(
        default_factory=dict, init=False, repr=False
    )
    _skill_handler_starts_lock: Lock = dataclasses.field(
        default_factory=Lock, init=False, repr=False
    )

    def __post_init__(self):
        if not self.bus or isinstance(self.bus, FakeBus):
            ovos_bus_address = self.config.get("host") or "127.0.0.1"
            ovos_bus_port = self.config.get("port") or 8181
            timeout = self._connection_timeout()
            self.bus = _RuntimeMessageBusClient(
                host=ovos_bus_address,
                port=ovos_bus_port,
                emitter=EventEmitter(),
                reconnect_error_after=self.config.get(
                    "reconnect_error_after", 120
                ),
                message_send_timeout=self.config.get(
                    "message_send_timeout", 15
                ),
                delivery_recovery_timeout=self.config.get(
                    "delivery_recovery_timeout", 20
                ),
                bus_write_queue_size=self.config.get(
                    "bus_write_queue_size", 256
                ),
                ping_interval=self.config.get("ping_interval", 15),
                ping_timeout=self.config.get("ping_timeout", 5),
            )
            # Install one immutable set of runtime-bus handlers before the
            # websocket receive thread starts. Per-query ``on``/``remove``
            # mutations can deadlock pyee's non-reentrant emitter lock while
            # that receive thread is dispatching a message.
            self._ensure_query_dispatchers(self.bus)
            self.bus.run_in_thread()
            self._owned_bus = self.bus
            # Bound startup without taking the whole HiveMind node down. The
            # managed client keeps reconnecting in the background; get_bus()
            # reports it unavailable immediately until that succeeds.
            if not self.bus.connected_event.wait(timeout):
                LOG.error(
                    f"Could not connect to the OVOS messagebus at "
                    f"ws://{ovos_bus_address}:{ovos_bus_port} within {timeout}s. "
                    "Retrying in the background; clients will receive "
                    "backend_unavailable until it connects."
                )
        else:
            self._owned_bus = None
            self._ensure_query_dispatchers(self.bus)
        self.register_bus_handlers()

    def register_bus_handlers(self):
        LOG.debug("registering internal OVOS bus handlers")
        self.bus.on("hive.send.downstream", self.handle_send)
        self.bus.on("message", self.handle_internal_mycroft)  # catch all

    @staticmethod
    def _normalize_timeout(raw, default: float = 10.0) -> float:
        """Return a finite, non-negative timeout."""
        try:
            timeout = float(raw)
        except (TypeError, ValueError):
            return default
        return timeout if math.isfinite(timeout) and timeout >= 0 else default

    def _connection_timeout(self) -> float:
        """Return a finite, non-negative startup wait."""
        return self._normalize_timeout(self.config.get("connection_timeout", 10))

    def get_bus(self, client=None) -> FakeBus | MessageBusClient:
        """Return a usable bus without blocking Core's shared IOLoop thread."""
        bus = self.bus
        if bus is not self._owned_bus:
            return bus
        if (bus.connected_event.is_set()
                and (not isinstance(bus, _RuntimeMessageBusClient)
                     or bus._transport_is_open())):
            return bus
        raise ConnectionError("OVOS messagebus is not connected")

    def wait_for_bus(self, timeout: Optional[float] = None) -> bool:
        """Wait for the owned runtime bus from setup or maintenance code."""
        if self.bus is not self._owned_bus:
            return True
        if timeout is None:
            timeout = self._connection_timeout()
        else:
            timeout = self._normalize_timeout(timeout)
        return self.bus.connected_event.wait(timeout)

    def _send_to_client(self, peer: str, client, hmessage: HiveMessage) -> bool:
        """Send a HiveMessage without letting stale sockets break bus dispatch."""
        try:
            client.send(hmessage)
            return True
        except Exception as exc:
            LOG.warning(f"Could not send {hmessage.msg_type} to {peer}: {exc}")
            clients = getattr(getattr(self, "hm_protocol", None), "clients", None)
            if clients is not None and clients.get(peer) is client:
                clients.pop(peer, None)
            return False

    def natural_language_query(self, utterance: str,
                               lang: str) -> "Iterator[Optional[str]]":
        """Answer by injecting the utterance on the OVOS bus and streaming the
        ``speak`` replies until ``ovos.utterance.handled`` (or 10s inactivity),
        correlated by a fresh query-scoped session so they are not reverse-routed."""
        yield from self._stream_query(utterance, lang)

    def answer_query_message(self, message: Message,
                             client=None) -> "Iterator[Optional[Any]]":
        """Answer an admitted QUERY while preserving its trusted context.

        HiveMind policy plugins annotate the admitted message session with
        server-side fields such as skill and intent blacklists.  The legacy
        ``answer_query`` seam only carries text and language, so HiveMind-core
        calls this optional, richer seam when the agent provides it.
        """
        utterances = message.data.get("utterances") or []
        utterance = utterances[0] if utterances else ""
        lang = (message.data.get("lang") or message.context.get("lang")
                or "en-US")
        if not utterance:
            yield None
            return
        yield from self._stream_query(
            utterance,
            lang,
            context=message.context,
            bus=self.get_bus(client),
            preserve_messages=True,
        )

    def _ensure_query_correlation_state(self):
        """Create the active-query registry for normal and test instances."""
        if not hasattr(self, "_active_query_scopes"):
            self._active_query_scopes = {}
            self._active_query_scopes_lock = Lock()
        if not hasattr(self, "_active_query_callbacks"):
            self._active_query_callbacks = {}
        if not hasattr(self, "_active_query_scope_index"):
            self._active_query_scope_index = {}
        if not hasattr(self, "_query_dispatcher_buses"):
            self._query_dispatcher_buses = set()
            self._query_dispatcher_handlers = {}
            self._query_dispatcher_setup_lock = Lock()

    def _ensure_query_dispatchers(self, bus) -> None:
        """Install one permanent bus dispatcher set for every used bus.

        Runtime messages arrive on the websocket receive thread while query
        workers consume them. Mutating pyee registrations from those workers
        can leave its plain ``Lock`` permanently held under that contention.
        Keep the emitter topology immutable and route events through a small
        agent-owned registry instead.
        """
        self._ensure_query_correlation_state()
        bus_id = id(bus)
        with self._query_dispatcher_setup_lock:
            if bus_id in self._query_dispatcher_buses:
                return

            def dispatch(event):
                def _handler(message):
                    self._dispatch_active_query_event(event, message)
                return _handler

            speak = dispatch("speak")
            handler_done = dispatch("handler_done")
            registrations = [
                ("speak", speak),
                ("ovos.utterance.speak", speak),
                ("mycroft.skill.handler.start", dispatch("handler_start")),
                ("mycroft.skill.handler.complete", handler_done),
                ("mycroft.skill.handler.error", handler_done),
                ("ovos.utterance.handled", dispatch("done")),
            ]
            installed = []
            try:
                for event_name, callback in registrations:
                    bus.on(event_name, callback)
                    installed.append((event_name, callback))
            except Exception:
                for event_name, callback in reversed(installed):
                    try:
                        bus.remove(event_name, callback)
                    except Exception:
                        pass
                raise
            self._query_dispatcher_handlers[bus_id] = registrations
            self._query_dispatcher_buses.add(bus_id)

    def _dispatch_active_query_event(self, event: str, message) -> None:
        """Route one immutable bus subscription to matching query callbacks.

        A listener replica can have many queries in flight while every replica
        receives the same runtime-bus lifecycle events.  Broadcasting every
        event to every local query makes dispatch quadratic under load because
        each callback repeats correlation and scope scans.  Prefer explicit
        query identifiers, then the server-admitted client/site scope index.
        Ambiguous or unrelated events cannot satisfy the existing unique-scope
        correlation rule, so they intentionally invoke no query callback.
        """
        self._ensure_query_correlation_state()
        if isinstance(message, str):
            try:
                indexed_message = Message.deserialize(message)
            except Exception:
                indexed_message = None
        else:
            indexed_message = message

        with self._active_query_scopes_lock:
            active_ids = set(self._active_query_callbacks)
            identifiers = (
                self._message_query_ids(indexed_message)
                if indexed_message is not None else set()
            )
            candidates = identifiers.intersection(active_ids)
            if not candidates and indexed_message is not None:
                tokens = self._query_scope_tokens(
                    getattr(indexed_message, "context", None), response=True
                )
                client_tokens = {
                    token for token in tokens
                    if token.startswith(("peer:", "client:"))
                }
                candidates = {
                    query_id
                    for token in client_tokens
                    for query_id in self._active_query_scope_index.get(
                        token, set()
                    )
                }
                if not candidates:
                    site_tokens = {
                        token for token in tokens
                        if token.startswith("site:")
                    }
                    candidates = {
                        query_id
                        for token in site_tokens
                        for query_id in self._active_query_scope_index.get(
                            token, set()
                        )
                    }
                if len(candidates) != 1:
                    candidates = set()
            callbacks = [
                self._active_query_callbacks[query_id].get(event)
                for query_id in candidates
                if self._active_query_callbacks[query_id].get(event) is not None
            ]
        for callback in callbacks:
            try:
                callback(message)
            except Exception:
                LOG.exception("Active OVOS query %s dispatcher raised", event)

    @staticmethod
    def _query_scope_tokens(context: Optional[Dict[str, Any]], *,
                            response: bool = False) -> Set[str]:
        """Return non-secret, server-admitted routing tokens for a query.

        ``source`` identifies the admitted HiveMind client. OVOS replies often
        move that value to ``destination``, so response contexts inspect both
        sides while admitted request contexts register only their source.
        Site and client identifiers are retained independently to avoid a
        coincidental value in one namespace matching another.
        """
        if not isinstance(context, dict):
            return set()
        tokens = set()
        session = context.get("session")
        if isinstance(session, dict):
            for key in ("site_id", "siteId"):
                value = session.get(key)
                if isinstance(value, str) and value.strip() not in ("", "unknown"):
                    tokens.add(f"site:{value.strip()}")
            for key in ("client_id", "clientId"):
                value = session.get(key)
                if isinstance(value, str) and value.strip():
                    tokens.add(f"client:{value.strip()}")
        for key in ("site_id", "siteId"):
            value = context.get(key)
            if isinstance(value, str) and value.strip() not in ("", "unknown"):
                tokens.add(f"site:{value.strip()}")
        for key in ("client_id", "clientId"):
            value = context.get(key)
            if isinstance(value, str) and value.strip():
                tokens.add(f"client:{value.strip()}")
        peer_keys = ("source", "destination") if response else ("source",)
        for key in peer_keys:
            value = context.get(key)
            values = value if isinstance(value, list) else [value]
            for candidate in values:
                if isinstance(candidate, str) and candidate.strip():
                    tokens.add(f"peer:{candidate.strip()}")
        return tokens

    @staticmethod
    def _message_query_ids(message: Message) -> Set[str]:
        """Return explicit correlation identifiers carried by a bus message."""
        identifiers = set()
        for container in (getattr(message, "context", None),
                          getattr(message, "data", None)):
            if not isinstance(container, dict):
                continue
            value = container.get("query_id")
            if isinstance(value, str) and value:
                identifiers.add(value)
            session = container.get("session")
            if isinstance(session, dict):
                value = session.get("session_id")
                if isinstance(value, str) and value:
                    identifiers.add(value)
        return identifiers

    def _register_active_query(
            self, query_id: str, context: Optional[Dict[str, Any]],
            callbacks: Optional[Dict[str, Callable]] = None) -> None:
        self._ensure_query_correlation_state()
        tokens = self._query_scope_tokens(context)
        with self._active_query_scopes_lock:
            previous = self._active_query_scopes.get(query_id, set())
            for token in previous:
                query_ids = self._active_query_scope_index.get(token)
                if query_ids is not None:
                    query_ids.discard(query_id)
                    if not query_ids:
                        self._active_query_scope_index.pop(token, None)
            self._active_query_scopes[query_id] = tokens
            self._active_query_callbacks[query_id] = dict(callbacks or {})
            for token in tokens:
                self._active_query_scope_index.setdefault(token, set()).add(
                    query_id
                )

    def _unregister_active_query(self, query_id: str) -> None:
        self._ensure_query_correlation_state()
        with self._active_query_scopes_lock:
            tokens = self._active_query_scopes.pop(query_id, set())
            self._active_query_callbacks.pop(query_id, None)
            for token in tokens:
                query_ids = self._active_query_scope_index.get(token)
                if query_ids is not None:
                    query_ids.discard(query_id)
                    if not query_ids:
                        self._active_query_scope_index.pop(token, None)

    def _uniquely_matches_active_scope(self, message: Message,
                                       query_id: str) -> bool:
        """Match an uncorrelated reply only to one admitted active query."""
        context = getattr(message, "context", None)
        tokens = self._query_scope_tokens(context, response=True)
        if not tokens:
            return False
        self._ensure_query_correlation_state()
        with self._active_query_scopes_lock:
            client_tokens = {
                token for token in tokens
                if token.startswith(("peer:", "client:"))
            }
            candidates = {
                candidate_id
                for token in client_tokens
                for candidate_id in self._active_query_scope_index.get(
                    token, set()
                )
            }
            # A server-admitted client route is more specific than a site:
            # many identities may legitimately share one site during load.
            if not candidates:
                site_tokens = {
                    token for token in tokens if token.startswith("site:")
                }
                candidates = {
                    candidate_id
                    for token in site_tokens
                    for candidate_id in self._active_query_scope_index.get(
                        token, set()
                    )
                }
        return candidates == {query_id}

    def _stream_query(self, utterance: str, lang: str, *,
                      context: Optional[Dict[str, Any]] = None,
                      bus=None, preserve_messages: bool = False
                      ) -> "Iterator[Optional[Any]]":
        """Collect one OVOS answer using query-id or session correlation.

        OVOS skills expose their real handler lifecycle through
        ``mycroft.skill.handler.start`` and ``mycroft.skill.handler.complete``.
        Confirmed delivery uses the idempotent post-transform receipt as its
        application-liveness proof, and every delivery/reply stage shares the
        complete query deadline. Once a handler starts, keep the query open
        until that lifecycle ends
        instead of applying the short reply-settle fallback to an intermediate
        ``speak``.  The fallback remains bounded for common-query and legacy
        paths that do not emit handler lifecycle events.  The context-aware
        HiveMind seam also receives the original speak Message so core can
        retain safe skill provenance.
        """
        import queue
        import time
        import uuid
        qid = uuid.uuid4().hex
        q: "queue.Queue" = queue.Queue()
        query_bus = bus or self.bus
        config = getattr(self, "config", None)
        config = config if isinstance(config, dict) else {}

        def _positive_timeout(name, default):
            try:
                value = float(config.get(name, default))
            except (TypeError, ValueError):
                return default
            return value if value > 0 else default

        response_timeout = _positive_timeout("query_timeout", 10.0)
        handled_grace = _positive_timeout("query_handled_grace", 1.0)
        reply_grace = _positive_timeout("query_reply_grace", 1.0)
        delivery_probe_timeout = _positive_timeout(
            "delivery_probe_timeout", 2.0
        )
        query_accept_timeout = _positive_timeout(
            "query_accept_timeout", 2.0
        )
        seen_replies = set()
        used_scope_fallback = False

        def _message(value):
            if isinstance(value, str):
                try:
                    return Message.deserialize(value)
                except Exception:
                    return None
            return value

        def _matches_query(msg, *, mark_scope_fallback=False):
            nonlocal used_scope_fallback
            msg = _message(msg)
            if msg is None:
                return False
            identifiers = self._message_query_ids(msg)
            if qid in identifiers:
                return True
            self._ensure_query_correlation_state()
            with self._active_query_scopes_lock:
                active_ids = set(self._active_query_scopes)
            # An explicit foreign active query identifier is authoritative.
            # Never override it merely because two clients share a site.
            if identifiers.intersection(active_ids):
                return False
            if not self._uniquely_matches_active_scope(msg, qid):
                return False
            if mark_scope_fallback and not used_scope_fallback:
                LOG.info(
                    "Accepted OVOS reply through unique active query scope "
                    "after query correlation was omitted"
                )
                used_scope_fallback = True
            return True

        def _on_speak(msg):
            msg = _message(msg)
            if msg is None or not _matches_query(
                msg, mark_scope_fallback=True
            ):
                return
            data = msg.data if isinstance(msg.data, dict) else {}
            utterance = data.get("utterance", "")
            if utterance:
                context = msg.context if isinstance(msg.context, dict) else {}
                session = context.get("session")
                session_id = (session.get("session_id")
                              if isinstance(session, dict) else None)
                fingerprint = (utterance, context.get("skill_id"), session_id)
                if fingerprint in seen_replies:
                    return
                seen_replies.add(fingerprint)
                q.put(("speak", msg))

        def _on_handler_start(msg):
            if _matches_query(msg):
                q.put(("handler_start", time.monotonic()))

        def _on_handler_done(msg):
            if _matches_query(msg):
                q.put(("handler_done", time.monotonic()))

        def _on_done(msg):
            if _matches_query(msg):
                q.put(("done", None))

        query_context = deepcopy(context) if isinstance(context, dict) else {}
        session = query_context.get("session")
        if not isinstance(session, dict):
            session = {}
        else:
            session = deepcopy(session)
        session["session_id"] = qid
        session.setdefault("lang", lang)
        query_context["session"] = session
        query_context["query_id"] = qid

        self._ensure_query_dispatchers(query_bus)
        self._register_active_query(qid, context, {
            "speak": _on_speak,
            "handler_start": _on_handler_start,
            "handler_done": _on_handler_done,
            "done": _on_done,
        })
        try:
            response_deadline = time.monotonic() + response_timeout
            query_message = Message(
                "recognizer_loop:utterance",
                {"utterances": [utterance], "lang": lang},
                query_context,
            )
            emit_confirmed = getattr(query_bus, "emit_confirmed", None)
            if callable(emit_confirmed):
                # The post-transform receipt proves that the intent service
                # reached a runnable pipeline stage, so a separate application
                # probe only adds another independent recovery window. Reserve
                # at most half of the complete query timeout for delivery and
                # leave the remainder for the skill handler lifecycle.
                delivery_budget = min(
                    getattr(query_bus, "_delivery_recovery_timeout", 20.0),
                    response_timeout / 2,
                )
                if isinstance(query_bus, _RuntimeMessageBusClient):
                    emit_confirmed(
                        query_message,
                        min(query_accept_timeout, delivery_budget),
                        recovery_timeout=delivery_budget,
                    )
                else:
                    emit_confirmed(
                        query_message,
                        min(query_accept_timeout, delivery_budget),
                    )
            else:
                ensure_delivery_path = getattr(
                    query_bus, "ensure_delivery_path", None
                )
                if callable(ensure_delivery_path):
                    ensure_delivery_path(
                        min(delivery_probe_timeout, response_timeout / 2)
                    )
                if time.monotonic() >= response_deadline:
                    raise TimeoutError(
                        "OVOS query delivery exceeded the complete query "
                        "timeout"
                    )
                query_bus.emit(query_message)
            handled_deadline = None
            reply_deadline = None
            answered = False
            handler_active = False
            while True:
                deadline = response_deadline
                if handled_deadline is not None:
                    deadline = min(deadline, handled_deadline)
                if reply_deadline is not None:
                    deadline = min(deadline, reply_deadline)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    if not answered:
                        LOG.warning(
                            "OVOS query timed out before a correlated reply "
                            "was observed"
                        )
                    yield None
                    return
                try:
                    event, chunk = q.get(timeout=remaining)
                except queue.Empty:
                    if not answered:
                        LOG.warning(
                            "OVOS query timed out before a correlated reply "
                            "was observed"
                        )
                    yield None
                    return
                if event == "handler_start":
                    handler_active = True
                    handled_deadline = None
                    reply_deadline = None
                    continue
                if event == "handler_done":
                    yield None
                    return
                if event == "done":
                    if answered:
                        yield None
                        return
                    handled_deadline = time.monotonic() + handled_grace
                    continue
                answered = True
                # Explicit skill handlers can emit progress speech while their
                # real work is still running. Their lifecycle is authoritative;
                # the short settle fallback is only for paths without it.
                # A scope-fallback reply proves the answer belongs to this
                # query, but it also proves OVOS dropped the explicit query
                # correlation.  The matching handler-complete event may be
                # equally uncorrelated, so it cannot safely terminate this
                # collector.  Bound that degraded path with the normal reply
                # grace instead of retaining a listener worker until the full
                # query timeout.
                if not handler_active or used_scope_fallback:
                    reply_deadline = time.monotonic() + reply_grace
                if preserve_messages:
                    yield chunk
                else:
                    data = chunk.data if isinstance(chunk.data, dict) else {}
                    yield data.get("utterance", "")
        finally:
            self._unregister_active_query(qid)

    # mycroft handlers - from master -> slave
    def handle_send(self, message: Message):
        """ovos wants to send a HiveMessage.

        A device can be both a master and a slave; downstream messages are handled here.
        HiveMindSlaveInternalProtocol handles requests meant to go upstream.
        """
        payload = message.data.get("payload")
        peer = message.data.get("peer")
        msg_type = message.data["msg_type"]

        hmessage = HiveMessage(msg_type, payload=payload, target_peers=[peer])

        if msg_type in [HiveMessageType.PROPAGATE, HiveMessageType.BROADCAST]:
            for peer, client in list(self.clients.items()):
                self._send_to_client(peer, client, hmessage)
        elif msg_type == HiveMessageType.ESCALATE:
            # only slaves can escalate, ignore silently
            pass
        elif peer:
            client = self.clients.get(peer)
            if client is not None:
                self._send_to_client(peer, client, hmessage)
            else:
                LOG.error("That client is not connected")
                self.bus.emit(
                    message.forward(
                        "hive.client.send.error",
                        {"error": "That client is not connected", "peer": peer},
                    )
                )

    def handle_internal_mycroft(self, message: str):
        """Forward internal messages to clients if they are the target.

        Client isolation happens here: clients only get responses to their own messages.
        """
        message = Message.deserialize(message)
        self._observe_skill_lifecycle(message)
        if self._is_runtime_private_event(message.msg_type):
            return
        target_peers = message.context.get("destination") or []
        if not isinstance(target_peers, list):
            target_peers = [target_peers]

        if target_peers:
            unmatched = set(target_peers)
            session = message.context.get("session")
            if isinstance(session, dict):
                # OVOS-SESSION-1 §2.1 requires registered null fields to be
                # omitted. Preserve unknown extension fields verbatim (§2.4).
                message.context["session"] = {
                    key: value
                    for key, value in session.items()
                    if value is not None or key not in SESSION1_REGISTERED_FIELDS
                }
            for peer, client in list(self.clients.items()):
                if peer in target_peers:
                    unmatched.discard(peer)
                    LOG.debug(f"{message.msg_type} - destination: {peer}")
                    message.context["source"] = "hive"
                    msg = HiveMessage(
                        HiveMessageType.BUS,
                        source_peer=peer,
                        target_peers=target_peers,
                        payload=message,
                    )
                    self._send_to_client(peer, client, msg)
            for peer in unmatched:
                LOG.warning(
                    f"{message.msg_type} - destination peer not connected: "
                    f"{peer}"
                )

    def _observe_skill_lifecycle(self, message: Message) -> None:
        """Measure correlated OVOS handler execution without forwarding it.

        The runtime catch-all sees lifecycle events for both HiveMind QUERY and
        ordinary BUS utterance paths. Pairing by the OVOS query/session
        identifiers moves the metric to that common boundary while the
        existing private-event filter keeps lifecycle traffic off satellites.
        State is time-bounded and size-bounded so missing completion events
        cannot leak memory.
        """
        lifecycle = message.msg_type
        if lifecycle not in {
            "mycroft.skill.handler.start",
            "mycroft.skill.handler.complete",
            "mycroft.skill.handler.error",
        }:
            return
        identifiers = self._message_query_ids(message)
        if not identifiers:
            return
        # A few lightweight embedders instantiate this dataclass through
        # ``__new__`` to avoid opening a runtime bus; keep that supported.
        if not hasattr(self, "_skill_handler_starts"):
            self._skill_handler_starts = {}
        if not hasattr(self, "_skill_handler_starts_lock"):
            self._skill_handler_starts_lock = Lock()

        now = time.monotonic()
        completed_starts = []
        with self._skill_handler_starts_lock:
            stale = [
                identifier
                for identifier, started in self._skill_handler_starts.items()
                if now - started > 60.0
            ]
            for identifier in stale:
                self._skill_handler_starts.pop(identifier, None)

            if lifecycle == "mycroft.skill.handler.start":
                while len(self._skill_handler_starts) + len(identifiers) > 4096:
                    oldest = next(iter(self._skill_handler_starts), None)
                    if oldest is None:
                        break
                    self._skill_handler_starts.pop(oldest, None)
                for identifier in identifiers:
                    self._skill_handler_starts[identifier] = now
                return

            for identifier in identifiers:
                started = self._skill_handler_starts.pop(identifier, None)
                if started is not None:
                    completed_starts.append(started)

        if completed_starts:
            # query_id and session_id commonly carry the same lifecycle; emit
            # one sample using the newest matching start, never one per alias.
            SKILL_HANDLER.observe_ms(
                max(0.0, now - max(completed_starts)) * 1000
            )

    @staticmethod
    def _is_runtime_private_event(message_type: str) -> bool:
        """Return whether an OVOS control event is private to the runtime.

        Public client replies such as ``speak`` and
        ``ovos.utterance.handled`` intentionally remain routable. The narrow
        denylist only removes capability probes, fallback coordination,
        handler lifecycle, and Thalovant delivery receipts.
        """
        if message_type.startswith("mycroft.skill.handler."):
            return True
        if message_type.startswith("thalovant.runtime."):
            return True
        if message_type.startswith("ovos.skills.fallback."):
            return True
        return message_type.endswith((".fallback.ping", ".fallback.pong"))


# back-compat alias for the old class name shipped from ovos-bus-client
OVOSProtocol = OVOSAgentProtocol


__all__ = ["OVOSAgentProtocol", "OVOSProtocol", "__version__"]
