"""Verify the plugin registers handlers and exposes a nonblocking bus."""

import time
from unittest.mock import MagicMock

import pytest

from hivemind_ovos_agent_plugin import OVOSAgentProtocol


class TestBusRegistration:
    def test_registers_hive_send_downstream(self, agent, fake_bus):
        # FakeBus stores listeners on its internal emitter
        listeners = fake_bus.ee.listeners("hive.send.downstream")
        assert (
            any(
                getattr(listener, "__func__", None) is OVOSAgentProtocol.handle_send
                for listener in listeners
            )
            or any(
                getattr(listener, "__name__", "") == "handle_send"
                for listener in listeners
            )
        )

    def test_registers_catch_all_message_listener(self, agent, fake_bus):
        listeners = fake_bus.ee.listeners("message")
        assert (
            any(
                getattr(listener, "__name__", "") == "handle_internal_mycroft"
                for listener in listeners
            )
            or any(
                getattr(listener, "__func__", None)
                is OVOSAgentProtocol.handle_internal_mycroft
                for listener in listeners
            )
        )

    def test_bus_field_is_kept(self, agent, fake_bus):
        """The bus the plugin operates on is the one we wired in."""
        assert agent.bus is fake_bus

    def test_get_bus_returns_external_bus_without_waiting(self, agent, fake_bus):
        assert agent.get_bus() is fake_bus

    def test_get_bus_returns_connected_owned_bus(self, agent):
        bus = MagicMock()
        bus.connected_event.is_set.return_value = True
        agent.bus = agent._owned_bus = bus

        assert agent.get_bus() is bus
        bus.connected_event.wait.assert_not_called()

    def test_get_bus_raises_immediately_when_owned_bus_is_disconnected(self,
                                                                       agent):
        bus = MagicMock()
        bus.connected_event.is_set.return_value = False
        agent.bus = agent._owned_bus = bus

        started = time.monotonic()
        with pytest.raises(ConnectionError, match="not connected"):
            agent.get_bus()

        assert time.monotonic() - started < 0.1
        bus.connected_event.wait.assert_not_called()

    def test_wait_for_bus_is_explicit_and_bounded(self, agent):
        bus = MagicMock()
        bus.connected_event.wait.return_value = True
        agent.bus = agent._owned_bus = bus

        assert agent.wait_for_bus(2.5) is True
        bus.connected_event.wait.assert_called_once_with(2.5)

    def test_wait_for_bus_uses_configured_default_timeout(self, agent):
        bus = MagicMock()
        bus.connected_event.wait.return_value = True
        agent.bus = agent._owned_bus = bus
        agent.config = {"connection_timeout": 3}

        assert agent.wait_for_bus() is True
        bus.connected_event.wait.assert_called_once_with(3.0)

    @pytest.mark.parametrize("timeout", ["invalid", float("inf"), -1])
    def test_wait_for_bus_normalizes_invalid_explicit_timeout(self, agent,
                                                              timeout):
        bus = MagicMock()
        bus.connected_event.wait.return_value = True
        agent.bus = agent._owned_bus = bus
        agent.config = {"connection_timeout": 10}

        assert agent.wait_for_bus(timeout) is True
        bus.connected_event.wait.assert_called_once_with(10.0)

    @pytest.mark.parametrize("raw", [None, "invalid", float("inf"), -1])
    def test_invalid_connection_timeout_uses_safe_default(self, agent, raw):
        agent.config = {"connection_timeout": raw}

        assert agent._connection_timeout() == 10.0
