import dataclasses
import time
from copy import deepcopy
from threading import Lock, Thread
from typing import Dict, Any, Iterator, Optional

from ovos_bus_client import MessageBusClient
from ovos_bus_client.message import Message
from ovos_config import Configuration
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


class _RuntimeMessageBusClient(MessageBusClient):
    """Reconnect quietly while a managed OVOS runtime is being replaced.

    Kubernetes can reject a connection with ``EPERM`` while a Service has no
    ready endpoints.  That is an expected, bounded condition during a serial
    runtime rollout, not an application traceback.  Keep retrying at INFO and
    escalate once when the outage exceeds the configured recovery budget.
    """

    def __init__(self, *args, reconnect_error_after=120, **kwargs):
        self._disconnect_started_at = None
        self._disconnect_escalated = False
        self._reconnect_state_lock = Lock()
        self._reconnect_worker = None
        self._reconnect_error = None
        self._close_requested = False
        try:
            reconnect_error_after = float(reconnect_error_after)
        except (TypeError, ValueError):
            reconnect_error_after = 120.0
        self._reconnect_error_after = max(reconnect_error_after, 1.0)
        super().__init__(*args, **kwargs)

    @staticmethod
    def _error_from_args(args):
        return args[0] if len(args) == 1 else args[1]

    @staticmethod
    def _is_transient_disconnect(error):
        return isinstance(error, (
            ConnectionError,
            PermissionError,
            TimeoutError,
            WebSocketConnectionClosedException,
            WebSocketTimeoutException,
        ))

    def on_open(self, *args):
        self._disconnect_started_at = None
        self._disconnect_escalated = False
        return super().on_open(*args)

    def close(self):
        self._ensure_reconnect_state()
        with self._reconnect_state_lock:
            self._close_requested = True
        return super().close()

    def _ensure_reconnect_state(self):
        """Initialize reconnect state for normal and test-constructed clients."""
        if not hasattr(self, "_reconnect_state_lock"):
            self._reconnect_state_lock = Lock()
            self._reconnect_worker = None
            self._reconnect_error = None
            self._close_requested = False

    def _schedule_reconnect(self, error):
        """Start one durable reconnect worker for error and clean-close paths."""
        self._ensure_reconnect_state()
        self.connected_event.clear()
        with self._reconnect_state_lock:
            if self._close_requested:
                return
            self._reconnect_error = error
            if (self._reconnect_worker is not None
                    and self._reconnect_worker.is_alive()):
                return
            self._reconnect_worker = Thread(
                target=self._run_reconnect_loop,
                name="ovos-runtime-bus-reconnect",
                daemon=True,
            )
            self._reconnect_worker.start()

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

            try:
                if self.client.keep_running:
                    self.client.close()
            except Exception as exc:
                LOG.error(
                    "Exception closing websocket at %s: %s",
                    self.client.url,
                    exc,
                )

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
                    return

    def on_close(self, *args):
        super().on_close(*args)
        self._schedule_reconnect(
            WebSocketConnectionClosedException(
                "OVOS message bus connection closed cleanly"
            )
        )

    def on_error(self, *args):
        error = self._error_from_args(args)
        if not self._is_transient_disconnect(error):
            return super().on_error(*args)
        self._schedule_reconnect(error)


@dataclasses.dataclass()
class OVOSAgentProtocol(AgentProtocol):
    """HiveMind agent protocol that bridges client messages to an OVOS bus."""
    bus: MessageBusClient = dataclasses.field(default_factory=FakeBus)
    config: Dict[str, Any] = dataclasses.field(default_factory=lambda: Configuration().get("websocket", {}))

    def __post_init__(self):
        if not self.bus or isinstance(self.bus, FakeBus):
            ovos_bus_address = self.config.get("host") or "127.0.0.1"
            ovos_bus_port = self.config.get("port") or 8181
            timeout = self.config.get("connection_timeout", 10)
            self.bus = _RuntimeMessageBusClient(
                host=ovos_bus_address,
                port=ovos_bus_port,
                emitter=EventEmitter(),
                reconnect_error_after=self.config.get(
                    "reconnect_error_after", 120
                ),
            )
            self.bus.run_in_thread()
            # Fail fast instead of blocking forever: a bare ``connected_event.wait()``
            # hangs indefinitely when no OVOS messagebus is reachable, which silently
            # stalls whatever hosts this protocol (e.g. HiveMindService.run() never
            # binds its network listeners). Raise a clear, actionable error instead.
            if not self.bus.connected_event.wait(timeout):
                self.bus.close()
                raise ConnectionError(
                    f"Could not connect to the OVOS messagebus at "
                    f"ws://{ovos_bus_address}:{ovos_bus_port} within {timeout}s. "
                    f"Is the OVOS messagebus running? Start it (e.g. 'ovos-messagebus'), "
                    f"or set the agent protocol's host/port/connection_timeout in the config."
                )
        self.register_bus_handlers()

    def register_bus_handlers(self):
        LOG.debug("registering internal OVOS bus handlers")
        self.bus.on("hive.send.downstream", self.handle_send)
        self.bus.on("message", self.handle_internal_mycroft)  # catch all

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

    def _stream_query(self, utterance: str, lang: str, *,
                      context: Optional[Dict[str, Any]] = None,
                      bus=None, preserve_messages: bool = False
                      ) -> "Iterator[Optional[Any]]":
        """Collect one OVOS answer using query-id or session correlation.

        OVOS skills may emit ``ovos.utterance.handled`` immediately before an
        asynchronous ``speak``.  Treating ``handled`` as an unconditional end
        marker loses that valid answer, so an unanswered query gets a short,
        bounded grace period.  The context-aware HiveMind seam also receives
        the original speak Message so core can retain safe skill provenance.
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
        seen_replies = set()

        def _message(value):
            if isinstance(value, str):
                try:
                    return Message.deserialize(value)
                except Exception:
                    return None
            return value

        def _matches_query(msg):
            msg = _message(msg)
            if msg is None:
                return False
            msg_context = getattr(msg, "context", None)
            if isinstance(msg_context, dict):
                if msg_context.get("query_id") == qid:
                    return True
                session = msg_context.get("session")
                if (isinstance(session, dict)
                        and session.get("session_id") == qid):
                    return True
            data = getattr(msg, "data", None)
            if isinstance(data, dict):
                if data.get("query_id") == qid:
                    return True
                session = data.get("session")
                if (isinstance(session, dict)
                        and session.get("session_id") == qid):
                    return True
            return False

        def _on_speak(msg):
            msg = _message(msg)
            if msg is None or not _matches_query(msg):
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

        query_bus.on("speak", _on_speak)
        query_bus.on("ovos.utterance.speak", _on_speak)
        query_bus.on("ovos.utterance.handled", _on_done)
        try:
            query_bus.emit(Message(
                "recognizer_loop:utterance",
                {"utterances": [utterance], "lang": lang},
                query_context,
            ))
            response_deadline = time.monotonic() + response_timeout
            handled_deadline = None
            answered = False
            while True:
                deadline = response_deadline
                if handled_deadline is not None:
                    deadline = min(deadline, handled_deadline)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    yield None
                    return
                try:
                    event, chunk = q.get(timeout=remaining)
                except queue.Empty:
                    yield None
                    return
                if event == "done":
                    if answered:
                        yield None
                        return
                    handled_deadline = time.monotonic() + handled_grace
                    continue
                answered = True
                if preserve_messages:
                    yield chunk
                else:
                    data = chunk.data if isinstance(chunk.data, dict) else {}
                    yield data.get("utterance", "")
        finally:
            query_bus.remove("speak", _on_speak)
            query_bus.remove("ovos.utterance.speak", _on_speak)
            query_bus.remove("ovos.utterance.handled", _on_done)

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
        target_peers = message.context.get("destination") or []
        if not isinstance(target_peers, list):
            target_peers = [target_peers]

        if target_peers:
            for peer, client in list(self.clients.items()):
                if peer in target_peers:
                    LOG.debug(f"{message.msg_type} - destination: {peer}")
                    message.context["source"] = "hive"
                    msg = HiveMessage(
                        HiveMessageType.BUS,
                        source_peer=peer,
                        target_peers=target_peers,
                        payload=message,
                    )
                    self._send_to_client(peer, client, msg)


# back-compat alias for the old class name shipped from ovos-bus-client
OVOSProtocol = OVOSAgentProtocol


__all__ = ["OVOSAgentProtocol", "OVOSProtocol", "__version__"]
