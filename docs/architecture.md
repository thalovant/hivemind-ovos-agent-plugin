# Architecture

```
                                                +-----------------------+
                                                |   OVOS Skills /       |
                                                |   pipelines / TTS     |
                                                +----------^------------+
                                                           |
                                              (mycroft Message objects)
                                                           |
+------------------+    HiveMessage     +------------+   OVOS bus    +-----+
| HiveMind client  | <----------------> | hivemind-  | <-----------> | OVOS|
| (satellite, IoT) |   (encrypted ws)   | core       |  (websocket)  | bus |
+------------------+                    +-----^------+               +-----+
                                              |
                                              | AgentProtocol contract
                                              v
                                  +-------------------------+
                                  |  OVOSAgentProtocol      |
                                  |  (this package)         |
                                  +-------------------------+
```

## Responsibilities

This plugin is the **bridge** between `hivemind-core` and a running OVOS bus. It is
loaded by `hivemind-core` via the `hivemind.agent.protocol` entry point group and
fulfils the `AgentProtocol` contract from `hivemind-plugin-manager`.

It owns exactly three responsibilities:

1. **Downstream dispatch**: when an OVOS component emits `hive.send.downstream` on the
   OVOS bus, forward the payload to the correct HiveMind client (or fan out for
   `PROPAGATE`/`BROADCAST` types).
2. **Response routing with client isolation**: when a public or application-defined
   OVOS bus message has `context["destination"]` set to a connected HiveMind peer, wrap
   it as a `HiveMessageType.BUS` message and forward to that peer — and **only** that
   peer. Runtime-only fallback, handler, intent, readiness, receipt, and recognizer audio
   lifecycle events terminate at this bridge.
3. **Runtime selection**: when explicitly configured with independent runtime shards,
   return the one rendezvous-hashed bus for a client through the documented
   `AgentProtocol.get_bus(client)` contract, and use the same selection for the
   client-aware `AgentProtocol.answer_query(...)` path. HiveMind Core still owns
   ordinary BUS-message emission; the agent neither intercepts nor rewrites it.

It does **not** own:

- Decryption, handshake, authentication — `hivemind-core` does this.
- ACL enforcement / policy admission — orchestrated by `hivemind-core`'s policy
  chain (see [issue #85](https://github.com/JarbasHiveMind/HiveMind-core/issues/85)).
  This package contributes `OVOSAgentPolicy` (entry point `hivemind.policy /
  hivemind-ovos-agent-policy`) to that chain; see [`policy.md`](policy.md).
- Binary payload routing — handled by a separate `BinaryDataHandlerProtocol` plugin.
- Ordinary upstream BUS traffic (client → OVOS bus) — `hivemind-core` emits it
  directly on the bus returned by `get_bus(client)`. QUERY/CASCADE execution uses
  the AgentProtocol query contract and is therefore emitted by the selected agent
  backend.

## Why this lives in its own package

This module used to be `ovos_bus_client.hpm`, shipped as part of the `ovos-bus-client`
library. That created a dependency-direction smell:

- `ovos-bus-client` is a foundational lib used across the OVOS ecosystem.
- `hpm.py` imports `hivemind-core` and `hivemind-bus-client`.
- That means `ovos-bus-client`, at the bottom of the stack, knew about HiveMind, at the
  top of the stack.

Extracting it makes the layering explicit:

```
ovos-bus-client        <- foundational, OVOS only
hivemind-plugin-manager
hivemind-bus-client
hivemind-core
hivemind-ovos-agent-plugin   <- depends on all of the above; nothing depends on it
```

## Plugin lifecycle

1. `hivemind-core` reads its `agent_protocol` config block at startup.
2. `AgentProtocolFactory.create("hivemind-ovos-agent-plugin", config=...)` resolves the
   entry point to `OVOSAgentProtocol` and instantiates it with the given config.
3. `__post_init__` connects to the OVOS bus and registers the two bus handlers.
4. `hivemind-core` injects the instance as the `agent_protocol` field of its
   `HiveMindListenerProtocol`.
5. From that point on the plugin is purely event-driven: it reacts to OVOS bus
   messages and dispatches HiveMessages.

## Threading

The plugin runs each OVOS bus client on its own background thread. Each runtime
has one bounded FIFO writer, so HiveMind workers never contend inside
`websocket-client` or wait indefinitely behind a disconnected transport. A
disconnected selected shard and a full queue both fail immediately. Registered
receive handlers still execute on their corresponding bus thread.

## Runtime sharding

Runtime sharding is opt-in and requires an explicit list of unique, independently
isolated messagebus endpoints. A repeated connection to one broadcast bus is not a
shard: every receiver would observe the same `speak` frame and could produce duplicate
audio. The plugin therefore rejects both repeated endpoint tuples and the ambiguous
legacy `pool_size > 1` shape.

For a valid shard set, rendezvous hashing maps the admitted HiveMind peer to exactly one
runtime. The mapping is deterministic across listener processes and stable while shard
membership is unchanged. If that runtime is unavailable, the request fails immediately;
it is not replayed on another runtime because the first runtime may already have accepted
it. Runtime replies are accepted only by the bus that owns their destination peer, and a
bounded short-lived guard suppresses exact repeated correlated replies.

This mode distributes independent skill requests. It does not turn one HiveMind server
into a shared active-active relay: client registries, HiveMapper routes, and query/cascade
collectors remain process-local as documented by HiveMind Core.

### Listener ownership and ingress

One established WebSocket is owned by one listener process for its lifetime. Horizontal
listener replicas are safe only when the deployment also satisfies one of these routing
models:

- every listener has the same `runtime_shards` set, so the peer identity selects the same
  runtime after reconnecting through a different listener; or
- ingress consistently routes that peer to a listener which owns the selected runtime.

A random or round-robin reconnect across listeners with different shard subsets is not
sticky ownership and can move the peer to a different runtime. The plugin cannot repair
that at the application layer because ingress chooses a listener before the authenticated
HiveMind peer is available. Do not increase the partition count until the deployment has
defined and tested reconnect-stable ownership.

The originating listener remains the authoritative public reply route: it accepts a
runtime reply only from the bus selected for the destination peer, then looks up the live
peer in its process-local client registry. The short-lived deduplication guard is a safety
net for broker retries or endpoint mistakes, not a replacement for bus ownership.

The `self.clients` mapping is owned by `HiveMindListenerProtocol`; fan-out reads
a stable item snapshot because connect and disconnect callbacks may mutate that
mapping concurrently.
