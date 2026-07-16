"""Verify the plugin registers the expected handlers on the OVOS bus."""

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
