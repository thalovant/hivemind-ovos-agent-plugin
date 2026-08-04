"""The auto-connect branch must degrade, not hang, when the OVOS bus is down."""
import socket
import time

import pytest

from hivemind_ovos_agent_plugin import OVOSAgentProtocol


def _closed_port() -> int:
    """Return a port with nothing listening on it."""
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def test_starts_degraded_when_messagebus_unreachable():
    port = _closed_port()
    start = time.monotonic()
    agent = OVOSAgentProtocol(config={"host": "127.0.0.1", "port": port,
                                      "connection_timeout": 1})
    elapsed = time.monotonic() - start
    try:
        assert elapsed < 10, f"took {elapsed:.1f}s — should stop near the 1s timeout"
        with pytest.raises(ConnectionError, match="not connected"):
            agent.get_bus()

        agent.bus.connected_event.set()
        assert agent.get_bus() is agent.bus
    finally:
        agent.bus.close()
