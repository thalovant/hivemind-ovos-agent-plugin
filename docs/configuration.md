# Configuration

The plugin is configured by the `hivemind-core` `agent_protocol` block.

```json
{
  "agent_protocol": {
    "hivemind-ovos-agent-plugin": {
      "host": "127.0.0.1",
      "port": 8181
    }
  }
}
```

## Keys

| Key    | Type   | Default        | Description                                         |
|--------|--------|----------------|-----------------------------------------------------|
| `host` | string | `127.0.0.1`    | Hostname or IP of the OVOS messagebus.              |
| `port` | int    | `8181`         | TCP port of the OVOS messagebus.                    |
| `message_send_timeout` | number | `15` | Maximum seconds a send may wait for runtime-bus reconnection. |
| `bus_write_queue_size` | integer | `256` | Maximum ordered OVOS-bus writes waiting behind the single writer; overload fails immediately. |
| `ping_interval` | number | `15` | WebSocket ping interval used to detect half-open runtime connections. |
| `ping_timeout` | number | `5` | Seconds to wait for a runtime-bus pong; must be below `ping_interval`. |
| `delivery_probe_timeout` | number | `2` | Maximum seconds for each application-level runtime probe or receipt attempt. |
| `delivery_recovery_timeout` | number | `20` | Maximum shared window for exact, idempotent query reservation and post-transform pipeline start while the OVOS core consumer reconnects. The effective query-delivery budget is also capped at half of `query_timeout`. |
| `runtime_shards` | array | unset | Named, unique OVOS messagebus endpoints used for deterministic client-to-runtime routing. Maximum 64. |
| `reply_dedupe_seconds` | number | `5` | Short window for suppressing an exact repeated correlated public reply in sharded mode. |
| `reply_dedupe_max_entries` | integer | `8192` | Bounded correlated-reply fingerprints retained across shards. Maximum 65536. |

`query_timeout` bounds delivery and the complete skill-handler lifecycle.
Confirmed queries use their post-transform pipeline receipt as the
application-level liveness proof instead of paying for a separate probe
recovery window.
Intermediate
speech does not close a query while an OVOS handler is still active; legacy
and common-query paths without lifecycle events retain the bounded
`query_reply_grace` settle fallback.

If no `host`/`port` are supplied, the plugin falls back to the
`websocket` section of the global OVOS `Configuration()`, which is also the standard
location for OVOS bus client settings. This means an OVOS install that already has
`mycroft.conf` configured will work without any extra config in `hivemind-core`.

## Independent runtime shards

Use `runtime_shards` only when every hostname reaches a different OVOS runtime and
messagebus. Hostnames must be explicit and unique; a shared ClusterIP service is not an
independent endpoint.

```json
{
  "agent_protocol": {
    "hivemind-ovos-agent-plugin": {
      "runtime_shards": [
        {"id": "runtime-0", "host": "runtime-0.runtime-headless"},
        {"id": "runtime-1", "host": "runtime-1.runtime-headless"}
      ],
      "port": 8181
    }
  }
}
```

The shard ID and endpoint tuple must each be unique. Client routing uses the stable peer
identity and rendezvous hashing. An unavailable selected shard produces
`backend_unavailable`; the agent does not retry the request on another shard. The old
`pool_size > 1` configuration is rejected because it cannot prove that separate
connections terminate on separate broadcast buses.

All listener replicas should receive the same shard list. If listeners intentionally own
different shard subsets, ingress must consistently route an authenticated peer back to
the listener that owns its shard; ordinary round-robin reconnects do not provide this
guarantee. See [Listener ownership and ingress](architecture.md#listener-ownership-and-ingress).

## Reusing an existing bus connection

If you instantiate `OVOSAgentProtocol` programmatically and pass a non-default `bus`
argument, the plugin will skip its own bus-client setup and use the one you supply.
This is useful for tests and for OVOS deployments that already manage their own bus
client lifecycle.

```python
from ovos_bus_client import MessageBusClient
from hivemind_ovos_agent_plugin import OVOSAgentProtocol

bus = MessageBusClient(host="ovos.lan", port=8181)
bus.run_in_thread()
bus.connected_event.wait()

agent = OVOSAgentProtocol(bus=bus)
```

`hivemind-core` does not currently expose a way to inject a custom bus instance; that
path is for advanced/embedded use only.

## OVOS config interaction

When the plugin falls back to `Configuration().get("websocket", {})` it reads the
same keys OVOS itself reads. There is no separate "hivemind" section in `mycroft.conf`.
