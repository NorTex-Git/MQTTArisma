"""
Jerarquía de tipos de usuario para el sistema de alertas RESCUE.
Cada subclase encapsula el comportamiento de cache y mensajería
específico de cada rol, eliminando cadenas if/else dispersas en los handlers.
"""
from __future__ import annotations
from typing import Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from services.whatsapp_service import WhatsAppService


def resolve_creator_phone(activacion_alerta: Dict, usuarios: List[Dict]) -> str:
    """Resuelve el teléfono del creador buscando por usuario_id en la lista de sede.
    activacion_alerta = {'id': usuario_id, 'nombre': ..., 'tipo_activacion': ...}.
    Si tipo='hardware' o id ausente, devuelve ''."""
    if not isinstance(activacion_alerta, dict):
        return ""
    tipo = (activacion_alerta.get("tipo_activacion") or "").lower()
    if tipo == "hardware":
        return ""
    creator_id = activacion_alerta.get("id")
    if not creator_id:
        return ""
    creator_id_str = str(creator_id)
    for usuario in usuarios or []:
        uid = usuario.get("usuario_id") or usuario.get("id")
        if uid and str(uid) == creator_id_str:
            return usuario.get("numero", "")
    return ""


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
        cache_exists_flag: Optional[bool] = None,
        home_sede: str = "",
    ):
        self.phone = phone
        self.nombre = nombre
        self.rol = rol
        self.alert_id = str(alert_id) if alert_id else ""
        self._cache = cache_entry or {}
        self.usuario_id = usuario_id
        # Flag persistido: True si make_alert_user encontró cache real.
        # Si None, fallback a heurística (cache_entry truthy).
        self._cache_exists_flag = (
            cache_exists_flag if cache_exists_flag is not None else bool(cache_entry)
        )
        # home_sede: sede a la que pertenece el usuario (su sede de origen).
        # Cae a cache si está, sino al param explícito.
        self.home_sede = (
            (self._cache.get("sede") or "").strip()
            or (home_sede or "").strip()
        )

    # --- Identidad ---
    @property
    def is_creator(self) -> bool:
        return False

    @property
    def is_manager(self) -> bool:
        return False

    # --- Sede membership ---
    @staticmethod
    def _norm_phone(p: str) -> str:
        return (p or "").strip().lstrip("+")

    def is_in_sede_for_alert(self, alert_data: Dict) -> bool:
        """True si el usuario pertenece a la sede de la alerta.
        Comparación principal: home_sede (de cache) == alert.sede.
        Fallback: teléfono aparece en numeros_telefonicos (cubre casos donde
        cache.sede no estaba poblada todavía)."""
        alert_sede = (alert_data.get("sede") or "").strip()
        if alert_sede and self.home_sede and alert_sede == self.home_sede:
            return True
        norm = self._norm_phone(self.phone)
        return any(
            self._norm_phone(u.get("numero", "")) == norm
            for u in (alert_data.get("numeros_telefonicos") or [])
        )

    # --- Cache helpers ---
    def is_focused_on(self, alert_id: str) -> bool:
        return (self._cache.get("info_alert") or {}).get("alert_id") == str(alert_id)

    def last_notified_id(self) -> Optional[str]:
        return (self._cache.get("last_notified_alert") or {}).get("alert_id")

    def cache_exists(self, cache_svc=None) -> bool:
        """True si ya existe entry en cache para este teléfono.
        Usa flag persistida (set en make_alert_user) para evitar fetches redundantes.
        cache_svc se mantiene para compat de firma."""
        return self._cache_exists_flag

    # --- Permisos por rol (acción → ¿puede ejecutarla?) ---
    # Reemplazan if/else dispersos como `if not is_creator: permission_denied`.
    # Cada subclase override solo lo que su rol cambia.
    def can_apagar(self) -> bool:
        """Apagar la alarma activa."""
        return False

    def can_embarcado(self, alert_data: Dict) -> bool:
        """Marcarse embarcado en una alerta. Solo si pertenece a esa sede."""
        return self.is_in_sede_for_alert(alert_data)

    def can_marcar_disponible(self, alert_data: Dict) -> bool:
        """Marcarse disponible. Solo si pertenece a la sede de la alerta."""
        return self.is_in_sede_for_alert(alert_data)

    def can_ver_ubicacion(self) -> bool:
        """Recibir ubicación de la alarma."""
        return True

    def can_cambiar_alerta(self) -> bool:
        """Abrir picker de cambio de foco entre alertas activas."""
        return False

    def can_switch_alert(self) -> bool:
        """Confirmar switch hacia una alerta específica."""
        return False

    def can_activar_alarma(self) -> bool:
        """Activar nueva alarma (creator/dual)."""
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

    def can_apagar(self) -> bool:
        return True

    def can_activar_alarma(self) -> bool:
        return True

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

    def can_cambiar_alerta(self) -> bool:
        return True

    def can_switch_alert(self) -> bool:
        return True

    def on_alert_broadcast(self, cache_svc) -> None:
        # PATCH de last_notified_alert lo hace ManagerLastNotifiedPatch.send
        # durante el fanout. Aquí no se toca el cache.
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

    def can_apagar(self) -> bool:
        return True

    def can_activar_alarma(self) -> bool:
        return True

    def can_cambiar_alerta(self) -> bool:
        return True

    def can_switch_alert(self) -> bool:
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

    # Sede del usuario: puede venir como string o dict
    sede_raw = usuario_dict.get("sede", "")
    if isinstance(sede_raw, dict):
        home_sede = (sede_raw.get("nombre") or sede_raw.get("name") or "").strip()
    else:
        home_sede = (sede_raw or "").strip()

    is_creator_flag = bool(phone and creator_phone and phone == creator_phone)
    is_manager_flag = bool(rol.get("is_alert_manager"))

    cache_entry: Dict = {}
    cache_exists_flag = False
    if cache_svc and phone:
        try:
            raw = cache_svc.get_number_from_cache(phone=phone)
            if isinstance(raw, dict):
                cache_entry = raw.get("data") or {}
                cache_exists_flag = True
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
        cache_exists_flag=cache_exists_flag,
        home_sede=home_sede,
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
        cache_exists_flag=True,
    )
