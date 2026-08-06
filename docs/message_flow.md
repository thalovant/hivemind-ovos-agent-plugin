# Message Flow

This document traces messages through the system. The plugin only handles the
**downstream** half (OVOS → client) and a small slice of fan-out / direct dispatch.
Upstream (client → OVOS bus) is `hivemind-core`'s job.

## Upstream: HiveMind client → OVOS bus

Handled entirely by `hivemind-core`. The plugin is not involved.

```
client                hivemind-core                 OVOS bus
  |                        |                            |
  |---HiveMessage(BUS)---->|                            |
  |                        |  decrypt, verify, policy   |
  |                        |---Mycroft Message--------->|
  |                        |                            |--> skills, pipelines, ...
```

## Downstream A: skill replies / TTS / etc.

OVOS skills typically emit responses with `context["destination"]` set to the original
client peer. The plugin's catch-all `message` handler picks these up. Public replies
(`speak`, `ovos.utterance.speak`, and `ovos.utterance.handled`) and other explicitly
targeted application messages remain routable.

```
OVOS skill                  OVOSAgentProtocol             hivemind-core            client
    |                              |                           |                     |
    |---emit Message --------------|                           |                     |
    |    context.destination=peer  |                           |                     |
    |                              |--match peer in self.clients                     |
    |                              |--wrap as HiveMessage(BUS)----------------------->|
```

### Client isolation guarantee

`handle_internal_mycroft` filters by `context["destination"]`:

```python
target_peers = message.context.get("destination") or []
if target_peers:
    for peer, client in self.clients.items():
        if peer in target_peers:
            ...
            client.send(msg)
```

A message with no `destination` is dropped. A message whose `destination` does not
match any connected client is dropped. **A connected client never sees a message
addressed to a different client.** This is the single security-critical invariant this
plugin enforces; it is exercised by tests in `tests/test_isolation.py`.

OVOS runtime coordination is not a downstream application response. The bridge consumes
skill/intent handler lifecycle, fallback coordination, runtime receipt/probe, readiness,
and recognizer audio lifecycle events locally instead of encrypting and forwarding those
internal broadcasts to every satellite. Their fixed-cardinality metric categories remain
scrapeable, so dropping them at the public boundary does not hide runtime work.

## Downstream B: explicit `hive.send.downstream`

An OVOS component (or any code on the bus) can explicitly push a HiveMessage to a
peer:

```python
bus.emit(Message("hive.send.downstream", {
    "peer": "ws://1.2.3.4:5678",
    "msg_type": "speak",
    "payload": some_mycroft_message,
}))
```

`handle_send` routes by `msg_type`:

| `msg_type`                       | Behaviour                                                            |
|----------------------------------|----------------------------------------------------------------------|
| `HiveMessageType.PROPAGATE`      | Broadcast to **all** connected clients.                              |
| `HiveMessageType.BROADCAST`      | Broadcast to **all** connected clients.                              |
| `HiveMessageType.ESCALATE`       | Silently ignored. Only slaves can escalate; handled elsewhere.       |
| Anything else with `peer` set    | Sent to that peer if connected; emits `hive.client.send.error` if not.|
| Anything else with `peer` unset  | Silently dropped.                                                    |

### Error reporting

When a `peer` is requested but not connected, the plugin emits a Mycroft
`hive.client.send.error` message back onto the OVOS bus, carrying `{error, peer}`.
Callers can listen for this if they need to know whether dispatch succeeded.

## Out of scope

- **Binary payloads** (audio streaming, files) — owned by a separate
  `BinaryDataHandlerProtocol` plugin.
- **Hive routing types** (`HANDSHAKE`, `HELLO`) — these never reach this plugin; they
  are consumed inside `hivemind-core`.
- **Policy decisions** — `hivemind-core`'s policy chain runs before this plugin sees a
  message. By the time the OVOS bus emits anything, the message has already been
  authorized.
