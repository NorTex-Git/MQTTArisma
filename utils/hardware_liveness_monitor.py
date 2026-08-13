"""Monitor de vida del hardware por expiración de claves Redis.

Detección de inactividad SIN polling. MQTTArisma refresca una clave
`hw:alive:<empresa><SEP><hardware>` en cada latido, con un TTL corto
(HARDWARE_STATUS_TTL_SECONDS). Cuando la clave expira —porque no llegó ningún
latido en la ventana— Redis emite un evento keyspace `expired`; aquí se captura
y se marca el hardware como Inactivo en el backend de inmediato, que a su vez lo
notifica al front por el canal realtime (Redis Pub/Sub) que ya existe.

Reemplaza la latencia del barrido (hasta ~600s) por una detección ≈ TTL.
El barrido del backend se mantiene solo como red de seguridad.
"""
from __future__ import annotations

import logging
import threading

# Prefijo y separador de las claves de vida. El separador es RS (ASCII 0x1e), que no
# aparece en nombres de empresa/hardware, para poder reconstruir ambos desde el nombre
# de la clave (el evento `expired` solo entrega el nombre, no el valor).
KEY_PREFIX = "hw:alive:"
KEY_SEP = "\x1e"


def build_alive_key(empresa: str, hardware: str) -> str:
    return f"{KEY_PREFIX}{empresa}{KEY_SEP}{hardware}"


class HardwareLivenessMonitor:
    """Escucha expiraciones de `hw:alive:*` y marca el hardware Inactivo."""

    def __init__(self, redis_config, backend_client, logger: logging.Logger | None = None):
        self._redis_config = redis_config
        self._backend_client = backend_client
        self._logger = logger or logging.getLogger(__name__)
        self._client = None
        self._pubsub = None
        self._thread = None
        self._running = False

    def start(self) -> bool:
        try:
            import redis
        except Exception as exc:  # pragma: no cover
            self._logger.error("Redis no disponible; monitor de vida deshabilitado: %s", exc)
            return False

        try:
            self._client = redis.Redis(
                host=self._redis_config.host,
                port=self._redis_config.port,
                db=self._redis_config.db,
                password=self._redis_config.password,
                socket_connect_timeout=5,
            )
            self._client.ping()
        except Exception as exc:
            self._logger.error("No se pudo conectar a Redis para el monitor de vida: %s", exc)
            return False

        # Habilitar notificaciones de expiración (flag 'E' + 'x'). Best-effort: si el
        # servidor no permite CONFIG SET, hay que activarlo en la config de Redis.
        try:
            self._client.config_set("notify-keyspace-events", "Ex")
        except Exception as exc:
            self._logger.warning(
                "No se pudo activar notify-keyspace-events (%s); la detección instantánea "
                "requiere 'Ex' en la config de Redis", exc
            )

        channel = f"__keyevent@{self._redis_config.db}__:expired"
        self._pubsub = self._client.pubsub(ignore_subscribe_messages=True)
        self._pubsub.subscribe(channel)
        self._running = True
        self._thread = threading.Thread(target=self._loop, args=(channel,), daemon=True)
        self._thread.start()
        self._logger.info("🔔 Monitor de vida por expiración Redis activo (canal=%s)", channel)
        return True

    def _loop(self, channel: str) -> None:
        try:
            for message in self._pubsub.listen():
                if not self._running:
                    break
                if message.get("type") != "message":
                    continue
                key = message.get("data")
                if isinstance(key, bytes):
                    key = key.decode("utf-8", errors="replace")
                if not isinstance(key, str) or not key.startswith(KEY_PREFIX):
                    continue
                rest = key[len(KEY_PREFIX):]
                if KEY_SEP not in rest:
                    continue
                empresa, hardware = rest.split(KEY_SEP, 1)
                # El PUT se hace en otro hilo para no bloquear la escucha de eventos.
                threading.Thread(
                    target=self._mark_inactive, args=(empresa, hardware), daemon=True
                ).start()
        except Exception as exc:
            if self._running:
                self._logger.error("Bucle del monitor de vida terminó con error: %s", exc)

    def _mark_inactive(self, empresa: str, hardware: str) -> None:
        try:
            ok = self._backend_client.send_physical_status(
                empresa, hardware, {"estado": "Inactivo"}
            )
            if ok:
                self._logger.info(
                    "💤 Inactivo (sin latido) empresa=%s hardware=%s", empresa, hardware
                )
            else:
                self._logger.warning(
                    "No se pudo marcar inactivo empresa=%s hardware=%s", empresa, hardware
                )
        except Exception as exc:
            self._logger.error("Error marcando inactivo: %s", exc)

    def stop(self) -> None:
        self._running = False
        try:
            if self._pubsub is not None:
                self._pubsub.close()
        except Exception:
            pass
