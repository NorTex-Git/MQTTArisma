"""
Jerarquía de tipos de mensaje para el fanout de alertas.
Cada subclase define: quién puede recibirlo y cómo enviarlo.
"""
from __future__ import annotations
from typing import Dict, TYPE_CHECKING

if TYPE_CHECKING:
    from models.alert_user import AlertUser


class AlertMessageType:
    """Tipo base de mensaje de alerta."""

    def can_receive(self, user: "AlertUser") -> bool:
        raise NotImplementedError

    def send(self, user: "AlertUser", svc, alert_data: Dict, logger=None) -> None:
        raise NotImplementedError


class TemplateMessage(AlertMessageType):
    """Plantilla 'crear_alerta' — para todos excepto el creador."""

    def can_receive(self, user: "AlertUser") -> bool:
        return not user.is_creator

    def send(self, user: "AlertUser", svc, alert_data: Dict, logger=None) -> None:
        alert_name = alert_data.get("nombre_alerta") or alert_data.get("tipo_alerta") or "Alerta"
        activacion = alert_data.get("activacion_alerta") or {}
        creador = activacion.get("nombre") or alert_data.get("empresa_nombre", "un miembro autorizado")

        recipient = {
            "phone": user.phone,
            "template_name": "crear_alerta",
            "language": "es_CO",
            "components": [
                {
                    "type": "body",
                    "parameters": [
                        {"type": "text", "text": user.nombre or "Usuario"},
                        {"type": "text", "text": alert_name},
                        {"type": "text", "text": creador},
                    ],
                }
            ],
        }
        try:
            svc.send_bulk_template(recipients=[recipient], use_queue=True)
        except Exception as ex:
            if logger:
                logger.error(f"TemplateMessage.send error {user.phone}: {ex}")


class MapMessage(AlertMessageType):
    """Mapa de ubicación — solo para el creador (o manager si se extiende)."""

    def can_receive(self, user: "AlertUser") -> bool:
        return user.is_creator

    def send(self, user: "AlertUser", svc, alert_data: Dict, logger=None) -> None:
        ubicacion = alert_data.get("ubicacion") or {}
        if not isinstance(ubicacion, dict):
            return
        url = ubicacion.get("url_maps")
        if not url:
            return
        try:
            svc.send_bulk_location_button_message(
                recipients=[{"phone": user.phone, "nombre": user.nombre}],
                url_maps=url,
                footer_text="Equipo RESCUE",
                use_queue=True,
            )
        except Exception as ex:
            if logger:
                logger.error(f"MapMessage.send error {user.phone}: {ex}")


class ManagerLastNotifiedPatch(AlertMessageType):
    """
    No envía mensaje WhatsApp: PATCH-only de last_notified_alert en cache.
    Exclusivo para managers que NO son creators. NO crea entry si no existe
    — el manager entra a la conversación solo cuando él elige (CAMBIAR_ALERTA).
    """

    def can_receive(self, user: "AlertUser") -> bool:
        return user.is_manager and not user.is_creator

    def send(self, user: "AlertUser", svc, alert_data: Dict, logger=None) -> None:
        if not user.cache_exists(svc):
            return
        try:
            svc.update_number_cache(
                phone=user.phone,
                data={
                    "last_notified_alert": {
                        "alert_id": user.alert_id,
                        "sede": alert_data.get("sede", ""),
                    }
                },
            )
        except Exception as ex:
            if logger:
                logger.error(f"ManagerLastNotifiedPatch.send error {user.phone}: {ex}")


# Orden importa: cada usuario recibe los mensajes que le corresponden
BROADCAST_MESSAGE_TYPES = [
    TemplateMessage(),
    MapMessage(),
    ManagerLastNotifiedPatch(),  # PATCH-only de last_notified_alert
]
