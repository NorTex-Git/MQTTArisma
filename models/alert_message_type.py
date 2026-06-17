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
    No envía mensaje WhatsApp: setea last_notified_alert en cache.
    Aplica a TODOS los no-creators (regulares + managers). Es la única fuente
    confiable de "última alerta notificada al usuario" — info_alert.alert_id
    queda atado al foco activo del usuario, que NO se cambia automáticamente
    al llegar nueva alerta (el usuario decide tocando "Estoy disponible").

    Si cache existe → PATCH last_notified_alert.
    Si NO existe → CREA entry con metadata mínima + last_notified_alert. Necesario
    para que el próximo mensaje (tap "Ver detalles") sea procesado por
    _process_save_number en lugar de _process_new_number_sync.
    """

    def can_receive(self, user: "AlertUser") -> bool:
        return not user.is_creator

    def send(self, user: "AlertUser", svc, alert_data: Dict, logger=None) -> None:
        last_notified = {
            "alert_id": user.alert_id,
            "sede": alert_data.get("sede", ""),
        }

        empresa_id = alert_data.get("empresa_id")
        if not empresa_id:
            empresa_data = alert_data.get("empresa")
            if isinstance(empresa_data, dict):
                empresa_id = empresa_data.get("id") or empresa_data.get("_id")
        empresa_nombre = alert_data.get("empresa_nombre", "")

        try:
            if user.cache_exists(svc):
                svc.update_number_cache(
                    phone=user.phone,
                    data={"last_notified_alert": last_notified},
                )
                return

            # Crear entry mínima para que próximo mensaje vaya por _process_save_number
            new_data = {
                "id": user.usuario_id,
                "empresa": empresa_nombre,
                "last_notified_alert": last_notified,
            }
            if user.home_sede:
                new_data["sede"] = user.home_sede
            if user.rol:
                new_data["rol"] = {
                    "nombre": user.rol.get("nombre") or user.rol.get("name", ""),
                    "is_creator": bool(user.rol.get("is_creator")),
                    "is_alert_manager": bool(user.rol.get("is_alert_manager")),
                }
            if empresa_id:
                new_data["empresa_id"] = empresa_id

            svc.add_number_to_cache(
                phone=user.phone,
                name=user.nombre or "",
                data=new_data,
                empresa_id=empresa_id,
            )
        except Exception as ex:
            if logger:
                logger.error(f"ManagerLastNotifiedPatch.send error {user.phone}: {ex}")


# Orden importa: cada usuario recibe los mensajes que le corresponden
BROADCAST_MESSAGE_TYPES = [
    TemplateMessage(),           # plantilla crear_alerta → todos excepto creador
    MapMessage(),                # mapa ubicación → creador
    ManagerLastNotifiedPatch(),  # PATCH-only de last_notified_alert → managers
]
