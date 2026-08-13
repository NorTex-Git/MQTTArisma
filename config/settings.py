"""
Configuración centralizada para la aplicación MQTT
"""
import os
import uuid
from dataclasses import dataclass, field
from typing import Optional
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()


def _env(name: str, default: str) -> str:
    """os.getenv tratando la cadena vacía como ausente.

    docker-compose interpola ${VAR} a "" cuando la variable no existe en el
    archivo de entorno, y un "" aquí rompe paho (transport) o int() (puertos).
    """
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return value.strip()


def _env_int(name: str, default: int) -> int:
    try:
        return int(_env(name, str(default)))
    except ValueError:
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    return _env(name, "true" if default else "false").lower() == "true"


@dataclass
class MQTTConfig:
    """Configuración para conexión MQTT"""
    broker: str = _env("MQTT_BROKER", "161.35.239.177")
    port: int = _env_int("MQTT_PORT", 17090)
    topic: str = _env("MQTT_TOPIC", "empresas")
    username: str = _env("MQTT_USERNAME", "tocancipa")
    password: str = _env("MQTT_PASSWORD", "B0mb3r0s")
    client_id: str = _env("MQTT_CLIENT_ID", "TST123")
    keep_alive: int = _env_int("MQTT_KEEP_ALIVE", 60)
    transport: str = _env("MQTT_TRANSPORT", "tcp")       # "tcp" o "websockets"
    ws_path: str = _env("MQTT_WS_PATH", "/mqtt")         # path WebSocket del broker
    tls: bool = _env_bool("MQTT_TLS", False)             # wss:// (Cloudflare = true)
    
    @classmethod
    def with_random_client_id(cls) -> 'MQTTConfig':
        """Crear configuración con ID de cliente aleatorio"""
        config = cls()
        config.client_id = f"tst-{uuid.uuid4()}"
        return config

    def summary(self) -> dict:
        """Configuración efectiva sin credenciales (para logs y /internal/status)"""
        return {
            "broker": self.broker,
            "port": self.port,
            "topic": self.topic,
            "username": self.username,
            "client_id": self.client_id,
            "transport": self.transport,
            "ws_path": self.ws_path,
            "tls": self.tls,
            "keep_alive": self.keep_alive,
        }


@dataclass
class BackendConfig:
    """Configuración para conexión al backend"""
    base_url: str = _env("BACKEND_URL", "http://rescue-backend:5002")
    api_key: Optional[str] = os.getenv("BACKEND_API_KEY") or None
    internal_token_header: str = _env("BACKEND_INTERNAL_TOKEN_HEADER", "X-Internal-Token")
    timeout: int = _env_int("BACKEND_TIMEOUT", 30)
    retry_attempts: int = _env_int("BACKEND_RETRY_ATTEMPTS", 3)
    retry_delay: int = _env_int("BACKEND_RETRY_DELAY", 5)
    enabled: bool = True  # Habilitado porque está corriendo


@dataclass
class WhatsAppConfig:
    """Configuración para API de WhatsApp"""
    api_url: str = _env("WHATSAPP_API_URL", "http://localhost:5050")
    timeout: int = _env_int("WHATSAPP_API_TIMEOUT", 30)
    enabled: bool = True


@dataclass
class WebSocketConfig:
    """Configuración para servidor WebSocket - INDEPENDIENTE de MQTT"""
    host: str = _env("WEBSOCKET_HOST", "0.0.0.0")
    port: int = _env_int("WEBSOCKET_PORT", 8080)
    ping_interval: int = _env_int("WEBSOCKET_PING_INTERVAL", 30)
    ping_timeout: int = _env_int("WEBSOCKET_PING_TIMEOUT", 10)
    enabled: bool = True
    # NOTA: WebSocket NO tiene dependencias MQTT


@dataclass
class RedisConfig:
    """Configuración para Redis"""
    host: str = _env("REDIS_HOST", "localhost")
    port: int = _env_int("REDIS_PORT", 6379)
    db: int = _env_int("REDIS_DB", 0)
    password: Optional[str] = os.getenv("REDIS_PASSWORD") or None

    # Configuración de colas WhatsApp
    whatsapp_queue_name: str = _env("WHATSAPP_QUEUE_NAME", "whatsapp_messages")
    whatsapp_workers: int = _env_int("WHATSAPP_WORKERS", 3)
    whatsapp_queue_ttl: int = _env_int("WHATSAPP_QUEUE_TTL", 3600)


@dataclass
class AppConfig:
    """Configuración general de la aplicación"""
    mqtt: MQTTConfig = field(default_factory=MQTTConfig)
    backend: BackendConfig = field(default_factory=BackendConfig)
    whatsapp: WhatsAppConfig = field(default_factory=WhatsAppConfig)
    websocket: WebSocketConfig = field(default_factory=WebSocketConfig)
    redis: RedisConfig = field(default_factory=RedisConfig)
    log_level: str = _env("LOG_LEVEL", "INFO")
    message_interval: int = _env_int("MESSAGE_INTERVAL", 20)
    # Throttle de vida del hardware: cada dispositivo late cada ~2s, pero el backend
    # solo necesita saber que sigue vivo. Se refresca physical-status como máximo una
    # vez por dispositivo cada N segundos. El umbral de inactividad del backend es de
    # 600s (HARDWARE_STATUS_STALE_SECONDS), así que 45s da margen de sobra.
    # Ventana de vida del hardware (TTL de la clave Redis `hw:alive:*`). Se refresca en
    # cada latido; si no llega ninguno en esta ventana, la clave expira y el monitor de
    # expiración marca el hardware Inactivo al instante (sin esperar al barrido de 600s).
    # El hardware late cada ~2s, así que 30s aguanta ~15 latidos perdidos sin parpadear.
    hardware_status_ttl_seconds: int = _env_int("HARDWARE_STATUS_TTL_SECONDS", 30)
    # Tipos de dispositivo que NO llevan estado de vida (no son hardware físico
    # monitorizable). Las PANTALLAS, por ejemplo, solo muestran; no reportan vida.
    # Lista separada por comas; se compara en mayúsculas contra el <TIPO> del topic.
    hardware_status_excluded_types: str = _env("HARDWARE_STATUS_EXCLUDED_TYPES", "PANTALLA")
