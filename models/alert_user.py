"""
Jerarquía de tipos de usuario para el sistema de alertas RESCUE.
Cada subclase encapsula el comportamiento de cache y mensajería
específico de cada rol, eliminando cadenas if/else dispersas en los handlers.
"""
from __future__ import annotations
from typing import Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from services.whatsapp_service import WhatsAppService


class AlertUser:
    """Tipo base de usuario en el contexto de una alerta."""

    def __init__(
        self,
        phone: str,
        nombre: str,
        rol: Dict,
        alert_id: str,
        cache_entry: Optional[Dict] = None,
        usuario_id: str = "",
    ):
        self.phone = phone
        self.nombre = nombre
        self.rol = rol
        self.alert_id = str(alert_id) if alert_id else ""
        self._cache = cache_entry or {}
        self.usuario_id = usuario_id

    # --- Identidad ---
    @property
    def is_creator(self) -> bool:
        return False

    @property
    def is_manager(self) -> bool:
        return False

    # --- Cache helpers ---
    def is_focused_on(self, alert_id: str) -> bool:
        return (self._cache.get("info_alert") or {}).get("alert_id") == str(alert_id)

    def last_notified_id(self) -> Optional[str]:
        return (self._cache.get("last_notified_alert") or {}).get("alert_id")

    def cache_exists(self, cache_svc) -> bool:
        """True si ya existe entry en cache para este teléfono."""
        try:
            entry = cache_svc.get_number_from_cache(phone=self.phone)
            return entry is not None
        except Exception:
            return False

    # --- Ciclo de vida del cache ---
    def on_alert_broadcast(self, cache_svc) -> None:
        """Qué hace este tipo de usuario cuando se broadcast una nueva alerta."""
        pass

    def on_alert_deactivated(self, cache_svc) -> None:
        """Limpieza de cache cuando la alerta se cierra."""
        try:
            patch = {
                "info_alert": "__DELETE__",
                "alert_active": "__DELETE__",
                "disponible": "__DELETE__",
                "embarcado": "__DELETE__",
            }
            cache_svc.update_number_cache(phone=self.phone, data=patch)
        except Exception:
            pass

    def on_focus_switch(self, new_alert_id: str, cache_svc) -> None:
        """Cuando el usuario cambia foco a otra alerta (solo managers)."""
        pass


class RegularUser(AlertUser):
    """Miembro de la sede, sin roles especiales."""

    @property
    def is_creator(self) -> bool:
        return False

    @property
    def is_manager(self) -> bool:
        return False

    def on_alert_broadcast(self, cache_svc) -> None:
        # Cache masivo ya lo crea el handler antes del fanout de mensajes
        pass


class CreatorUser(AlertUser):
    """Usuario que creó la alerta. Ya queda disponible al crearla."""

    @property
    def is_creator(self) -> bool:
        return True

    @property
    def is_manager(self) -> bool:
        return False

    def on_alert_broadcast(self, cache_svc) -> None:
        # Ya tiene alert_active desde que creó la alerta. No modificar cache.
        pass


class ManagerUser(AlertUser):
    """Monitor cross-sede. Solo entra a conversaciones por elección explícita."""

    @property
    def is_creator(self) -> bool:
        return False

    @property
    def is_manager(self) -> bool:
        return True

    def on_alert_broadcast(self, cache_svc) -> None:
        # PATCH-only: registrar última alerta notificada sin tocar info_alert.
        # NO crear entry si no existe — el manager entra solo cuando él elige.
        if not self.cache_exists(cache_svc):
            return
        try:
            patch = {
                "last_notified_alert": {
                    "alert_id": self.alert_id,
                    "sede": self.rol.get("sede", ""),
                }
            }
            cache_svc.update_number_cache(phone=self.phone, data=patch)
        except Exception:
            pass

    def on_alert_deactivated(self, cache_svc) -> None:
        # Solo limpiar si tenía foco en esta alerta específica
        if self.is_focused_on(self.alert_id):
            try:
                patch = {
                    "info_alert": "__DELETE__",
                    "alert_active": "__DELETE__",
                    "disponible": "__DELETE__",
                    "embarcado": "__DELETE__",
                }
                cache_svc.update_number_cache(phone=self.phone, data=patch)
            except Exception:
                pass

    def on_focus_switch(self, new_alert_id: str, cache_svc) -> None:
        try:
            cache_svc.update_number_cache(
                phone=self.phone,
                data={"info_alert": {"alert_id": str(new_alert_id)}},
            )
        except Exception:
            pass


class DualRoleUser(AlertUser):
    """Usuario con is_creator=True Y is_alert_manager=True."""

    @property
    def is_creator(self) -> bool:
        return True

    @property
    def is_manager(self) -> bool:
        return True

    def on_alert_broadcast(self, cache_svc) -> None:
        # Como CreatorUser: ya tiene cache activo, no tocar.
        pass

    def on_alert_deactivated(self, cache_svc) -> None:
        # Como ManagerUser: limpiar solo si tenía foco
        if self.is_focused_on(self.alert_id):
            try:
                patch = {
                    "info_alert": "__DELETE__",
                    "alert_active": "__DELETE__",
                    "disponible": "__DELETE__",
                    "embarcado": "__DELETE__",
                }
                cache_svc.update_number_cache(phone=self.phone, data=patch)
            except Exception:
                pass

    def on_focus_switch(self, new_alert_id: str, cache_svc) -> None:
        try:
            cache_svc.update_number_cache(
                phone=self.phone,
                data={"info_alert": {"alert_id": str(new_alert_id)}},
            )
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def make_alert_user(
    usuario_dict: Dict,
    creator_phone: str,
    alert_id: str,
    cache_svc=None,
) -> AlertUser:
    """
    Construye el AlertUser correcto a partir del dict de usuario.

    Args:
        usuario_dict: dict con keys 'numero', 'nombre', 'rol', 'usuario_id'.
        creator_phone: teléfono del creador (resultado de _resolve_creator_phone).
                       Si es '' o None → ningún usuario es creator.
        alert_id: ID de la alerta en contexto.
        cache_svc: WhatsAppService (para leer snapshot del cache).
    """
    phone = (usuario_dict.get("numero") or "").strip()
    nombre = usuario_dict.get("nombre", "")
    rol = usuario_dict.get("rol") or {}
    if not isinstance(rol, dict):
        rol = {}
    usuario_id = str(usuario_dict.get("usuario_id") or usuario_dict.get("id") or "")

    is_creator_flag = bool(phone and creator_phone and phone == creator_phone)
    is_manager_flag = bool(rol.get("is_alert_manager"))

    cache_entry: Dict = {}
    if cache_svc and phone:
        try:
            raw = cache_svc.get_number_from_cache(phone=phone)
            if isinstance(raw, dict):
                cache_entry = raw.get("data") or {}
        except Exception:
            pass

    cls_map = {
        (True,  True ): DualRoleUser,
        (True,  False): CreatorUser,
        (False, True ): ManagerUser,
        (False, False): RegularUser,
    }
    cls = cls_map[(is_creator_flag, is_manager_flag)]
    return cls(
        phone=phone,
        nombre=nombre,
        rol=rol,
        alert_id=alert_id,
        cache_entry=cache_entry,
        usuario_id=usuario_id,
    )


def make_whatsapp_user(cached_info: Dict, alert_id: str = "") -> AlertUser:
    """
    Construye AlertUser desde el cache existente de WhatsApp
    (usado en websocket_message_handler donde el cache ya está cargado).

    Args:
        cached_info: dict con keys 'phone', 'name', 'data'.
        alert_id: ID de la alerta activa si se conoce.
    """
    phone = cached_info.get("phone", "")
    nombre = cached_info.get("name", "")
    data = cached_info.get("data") or {}
    rol = data.get("rol") or {}
    if not isinstance(rol, dict):
        rol = {}
    usuario_id = str(data.get("id") or "")

    if not alert_id:
        alert_id = (data.get("info_alert") or {}).get("alert_id", "")

    is_creator_flag = bool(rol.get("is_creator"))
    is_manager_flag = bool(rol.get("is_alert_manager"))

    cls_map = {
        (True,  True ): DualRoleUser,
        (True,  False): CreatorUser,
        (False, True ): ManagerUser,
        (False, False): RegularUser,
    }
    cls = cls_map[(is_creator_flag, is_manager_flag)]
    return cls(
        phone=phone,
        nombre=nombre,
        rol=rol,
        alert_id=alert_id,
        cache_entry=data,
        usuario_id=usuario_id,
    )
