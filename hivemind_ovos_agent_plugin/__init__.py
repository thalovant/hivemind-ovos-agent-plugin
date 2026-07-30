import dataclasses
from collections.abc import Iterator
from typing import Any

from hivemind_bus_client.message import HiveMessage, HiveMessageType
from hivemind_plugin_manager.protocols import AgentProtocol
from ovos_bus_client import MessageBusClient
from ovos_bus_client.message import Message
from ovos_config import Configuration
from ovos_spec_tools.session import SESSION1_REGISTERED_FIELDS
from ovos_utils.fakebus import FakeBus
from ovos_utils.log import LOG
from pyee import EventEmitter

from hivemind_ovos_agent_plugin.policy import (
    AddBlacklistedIntent,
    AddBlacklistedSkill,
    OVOSAgentPolicy,
    RewriteUtterance,
    SetContextField,
    SetSessionField,
)
from hivemind_ovos_agent_plugin.version import __version__


@dataclasses.dataclass()
class OVOSAgentProtocol(AgentProtocol):
    """HiveMind agent protocol that bridges client messages to an OVOS bus."""
    bus: MessageBusClient = dataclasses.field(default_factory=FakeBus)
    config: dict[str, Any] = dataclasses.field(default_factory=lambda: Configuration().get("websocket", {}))
    _owned_bus: MessageBusClient | None = dataclasses.field(
        default=None, init=False, repr=False, compare=False
    )

    def __post_init__(self):
        if not self.bus or isinstance(self.bus, FakeBus):
            ovos_bus_address = self.config.get("host") or "127.0.0.1"
            ovos_bus_port = self.config.get("port") or 8181
            timeout = self._connection_timeout()
            self.bus = MessageBusClient(
                host=ovos_bus_address,
                port=ovos_bus_port,
                emitter=EventEmitter(),
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
            self._owned_bus = self.bus
        self.register_bus_handlers()

    def register_bus_handlers(self):
        LOG.debug("registering internal OVOS bus handlers")
        self.bus.on("hive.send.downstream", self.handle_send)
        self.bus.on("message", self.handle_internal_mycroft)  # catch all

    def _connection_timeout(self) -> float:
        raw = self.config.get("connection_timeout", 10)
        try:
            return max(0.0, float(raw))
        except (TypeError, ValueError):
            return 10.0

    def get_bus(self, client=None) -> FakeBus | MessageBusClient:
        """Return the current bus once its own reconnect loop restores it.

        ``MessageBusClient`` owns websocket recovery. The agent neither sends
        the client's message nor creates a competing connection; it only bounds
        how long Core can wait for the plugin-created bus to become usable.
        Caller-supplied buses remain entirely caller-owned.
        """
        bus = self.bus
        if bus is not self._owned_bus:
            return bus
        timeout = self._connection_timeout()
        if bus.connected_event.wait(timeout):
            return bus
        raise ConnectionError(f"OVOS messagebus did not reconnect within {timeout}s")

    def natural_language_query(self, utterance: str,
                               lang: str) -> "Iterator[str | None]":
        """Answer by injecting the utterance on the OVOS bus and streaming the
        ``speak`` replies until ``ovos.utterance.handled`` (or 10s inactivity),
        correlated by a fresh query-scoped session so they are not reverse-routed."""
        import queue
        import uuid
        qid = uuid.uuid4().hex
        q: queue.Queue = queue.Queue()

        def _on_speak(msg):
            if isinstance(msg, str):
                try:
                    msg = Message.deserialize(msg)
                # Ignore malformed third-party bus payloads.
                except Exception:  # noqa: BLE001
                    return
            if not isinstance(msg, Message):
                return
            if msg.msg_type == "speak" and msg.context.get("query_id") == qid:
                q.put(msg.data.get("utterance", ""))

        def _on_done(msg):
            if isinstance(msg, str):
                try:
                    msg = Message.deserialize(msg)
                # Ignore malformed third-party bus payloads.
                except Exception:  # noqa: BLE001
                    return
            if not isinstance(msg, Message):
                return
            if msg.context.get("query_id") == qid:
                q.put(None)

        bus = self.get_bus()
        bus.on("speak", _on_speak)
        bus.on("ovos.utterance.handled", _on_done)
        try:
            bus.emit(Message(
                "recognizer_loop:utterance",
                {"utterances": [utterance], "lang": lang},
                {"query_id": qid, "session": {"session_id": qid}},
            ))
            while True:
                try:
                    chunk = q.get(timeout=10.0)
                except queue.Empty:
                    yield None
                    return
                if chunk is None:
                    yield None
                    return
                yield chunk
        finally:
            bus.remove("speak", _on_speak)
            bus.remove("ovos.utterance.handled", _on_done)

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
            for peer in self.clients:
                self.clients[peer].send(hmessage)
        elif msg_type == HiveMessageType.ESCALATE:
            # only slaves can escalate, ignore silently
            pass
        elif peer:
            if peer in self.clients:
                client = self.clients[peer]
                client.send(hmessage)
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
            session = message.context.get("session")
            if isinstance(session, dict):
                # OVOS-SESSION-1 §2.1 requires registered null fields to be
                # omitted. Enforce that producer rule at the OVOS-to-HiveMind
                # boundary while preserving unknown fields verbatim (§2.4).
                message.context["session"] = {
                    key: value
                    for key, value in session.items()
                    if value is not None or key not in SESSION1_REGISTERED_FIELDS
                }
            for peer, client in self.clients.items():
                if peer in target_peers:
                    LOG.debug(f"{message.msg_type} - destination: {peer}")
                    message.context["source"] = "hive"
                    msg = HiveMessage(
                        HiveMessageType.BUS,
                        source_peer=peer,
                        target_peers=target_peers,
                        payload=message,
                    )
                    client.send(msg)


# back-compat alias for the old class name shipped from ovos-bus-client
OVOSProtocol = OVOSAgentProtocol


__all__ = [
    "AddBlacklistedIntent",
    "AddBlacklistedSkill",
    "OVOSAgentPolicy",
    "OVOSAgentProtocol",
    "OVOSProtocol",
    "RewriteUtterance",
    "SetContextField",
    "SetSessionField",
    "__version__",
]
