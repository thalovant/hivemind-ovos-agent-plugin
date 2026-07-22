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
| `ping_interval` | number | `15` | WebSocket ping interval used to detect half-open runtime connections. |
| `ping_timeout` | number | `5` | Seconds to wait for a runtime-bus pong; must be below `ping_interval`. |
| `delivery_probe_timeout` | number | `2` | Maximum seconds for each application-level runtime probe or receipt attempt. |
| `delivery_recovery_timeout` | number | `20` | Total bounded window for exact, idempotent probe and query-receipt retries while the OVOS core consumer reconnects. |

`query_timeout` bounds the complete skill-handler lifecycle. Intermediate
speech does not close a query while an OVOS handler is still active; legacy
and common-query paths without lifecycle events retain the bounded
`query_reply_grace` settle fallback.

If no `host`/`port` are supplied, the plugin falls back to the
`websocket` section of the global OVOS `Configuration()`, which is also the standard
location for OVOS bus client settings. This means an OVOS install that already has
`mycroft.conf` configured will work without any extra config in `hivemind-core`.

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
