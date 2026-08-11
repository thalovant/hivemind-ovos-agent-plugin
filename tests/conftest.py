from unittest.mock import MagicMock

import pytest
from ovos_utils.fakebus import FakeBus
from hivemind_plugin_manager.protocols import ClientCallbacks

from hivemind_ovos_agent_plugin import OVOSAgentProtocol


@pytest.fixture
def fake_bus():
    return FakeBus()


@pytest.fixture
def make_client():
    """Factory for mock HiveMindClientConnection objects."""

    def _make(peer: str):
        client = MagicMock()
        client.peer = peer
        client.send = MagicMock()
        return client

    return _make


@pytest.fixture
def agent(fake_bus):
    """OVOSAgentProtocol wired to a FakeBus, without triggering the auto-connect
    branch in __post_init__ (which would try to talk to a real OVOS messagebus).
    """
    plugin = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
    plugin.bus = fake_bus
    plugin.config = {}
    plugin._owned_bus = None
    plugin.hm_protocol = MagicMock()
    plugin.hm_protocol.clients = {}
    plugin.callbacks = ClientCallbacks()
    plugin.register_bus_handlers()
    return plugin
