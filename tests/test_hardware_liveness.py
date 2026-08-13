import importlib.util
from pathlib import Path
import unittest
from unittest.mock import Mock, call

from utils.hardware_liveness_monitor import HardwareLivenessMonitor


MODULE_PATH = Path(__file__).parents[1] / "handlers" / "mqtt_message_handler.py"
SPEC = importlib.util.spec_from_file_location("mqtt_message_handler_under_test", MODULE_PATH)
assert SPEC and SPEC.loader
MQTT_MESSAGE_HANDLER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MQTT_MESSAGE_HANDLER)
MQTTMessageHandler = MQTT_MESSAGE_HANDLER.MQTTMessageHandler
is_hardware_report = MQTT_MESSAGE_HANDLER.is_hardware_report


TOPIC = "empresas/Sunida/Principal/BOTONERA/Pulsador1"


class FakeRedis:
    def __init__(self):
        self.values = {}
        self.expirations = []

    def set(self, key, value, nx=False, ex=None):
        if nx and key in self.values:
            return False
        self.values[key] = value
        return True

    def expire(self, key, ttl):
        self.expirations.append((key, ttl))
        return key in self.values


def build_handler(redis_client=None):
    handler = object.__new__(MQTTMessageHandler)
    handler.logger = Mock()
    handler._alive_excluded_types = {"PANTALLA"}
    handler._alive_redis = redis_client
    handler._alive_ttl = 30
    handler._activate = Mock()
    return handler


def test_outbound_commands_do_not_count_as_hardware_reports():
    assert not is_hardware_report(TOPIC, {"tipo_alarma": "NORMAL"})
    assert not is_hardware_report(
        TOPIC,
        {"tipo_alarma": "NORMAL", "action": "deactivate"},
    )
    assert not is_hardware_report(TOPIC, {"action": "generic"})


def test_device_alarm_and_status_are_hardware_reports():
    assert is_hardware_report(
        TOPIC,
        {
            "tipo_mensaje": "alarma",
            "id_origen": "Pulsador1",
            "tipo_alarma": "Incendio",
        },
    )
    assert is_hardware_report(
        TOPIC,
        {"tipo_mensaje": "status", "id_dispositivo": "Pulsador1", "estado": "Activo"},
    )
    assert is_hardware_report(TOPIC, {"tipo_mensaje": "heartbeat"})


def test_retained_or_mismatched_reports_do_not_refresh_liveness():
    report = {"tipo_mensaje": "heartbeat"}
    assert not is_hardware_report(TOPIC, report, retained=True)
    assert not is_hardware_report(
        TOPIC,
        {"tipo_mensaje": "status", "id_dispositivo": "OtroEquipo"},
    )


def test_first_report_activates_and_following_reports_only_renew_ttl():
    redis_client = FakeRedis()
    handler = build_handler(redis_client)
    report = {"tipo_mensaje": "heartbeat"}

    handler._handle_liveness(TOPIC, report)
    handler._handle_liveness(TOPIC, report)

    handler._activate.assert_called_once_with("Sunida", "Pulsador1")
    assert len(redis_client.expirations) == 1
    assert redis_client.expirations[0][1] == 30


def test_outbound_command_never_creates_alive_key():
    redis_client = FakeRedis()
    handler = build_handler(redis_client)

    handler._handle_liveness(TOPIC, {"tipo_alarma": "NORMAL"})

    assert redis_client.values == {}
    handler._activate.assert_not_called()


def test_expiration_is_ignored_if_device_key_already_reappeared():
    backend = Mock()
    monitor = HardwareLivenessMonitor(None, backend, logger=Mock())
    monitor._client = Mock()
    monitor._client.exists.return_value = True

    monitor._mark_inactive("Sunida", "Pulsador1")

    backend.send_physical_status.assert_not_called()


def test_reappearance_during_inactive_update_restores_active_last():
    backend = Mock()
    backend.send_physical_status.return_value = True
    monitor = HardwareLivenessMonitor(None, backend, logger=Mock())
    monitor._client = Mock()
    monitor._client.exists.side_effect = [False, True]

    monitor._mark_inactive("Sunida", "Pulsador1")

    assert backend.send_physical_status.call_args_list == [
        call("Sunida", "Pulsador1", {"estado": "Inactivo"}),
        call("Sunida", "Pulsador1", {"estado": "Activo"}),
    ]


class HardwareLivenessTests(unittest.TestCase):
    test_outbound_commands_do_not_count_as_hardware_reports = staticmethod(
        test_outbound_commands_do_not_count_as_hardware_reports
    )
    test_device_alarm_and_status_are_hardware_reports = staticmethod(
        test_device_alarm_and_status_are_hardware_reports
    )
    test_retained_or_mismatched_reports_do_not_refresh_liveness = staticmethod(
        test_retained_or_mismatched_reports_do_not_refresh_liveness
    )
    test_first_report_activates_and_following_reports_only_renew_ttl = staticmethod(
        test_first_report_activates_and_following_reports_only_renew_ttl
    )
    test_outbound_command_never_creates_alive_key = staticmethod(
        test_outbound_command_never_creates_alive_key
    )
    test_expiration_is_ignored_if_device_key_already_reappeared = staticmethod(
        test_expiration_is_ignored_if_device_key_already_reappeared
    )
    test_reappearance_during_inactive_update_restores_active_last = staticmethod(
        test_reappearance_during_inactive_update_restores_active_last
    )
