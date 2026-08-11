"""
Utilidades para configurar logging
"""
import logging
import logging.handlers
import sys
from datetime import datetime

ACTION_LOGGER_PREFIXES = (
    "clients.backend_client",
    "services.whatsapp_service",
)

DEFAULT_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# Loggers de terceros que solo aportan ruido en INFO
NOISY_LOGGERS = ("websockets", "urllib3", "paho", "asyncio", "aiohttp.access")

# Tamaño máximo del archivo de log antes de rotar (logs/ es bind-mount en Docker)
LOG_MAX_BYTES = 10 * 1024 * 1024
LOG_BACKUP_COUNT = 5

# Evita que una segunda llamada duplique handlers en el root
_root_configured = False


class ActionFilter(logging.Filter):
    """Permitir solo logs de acciones y errores."""

    def __init__(self, allowed_prefixes=None):
        super().__init__()
        self.allowed_prefixes = tuple(allowed_prefixes or ())

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno >= logging.ERROR:
            return True
        if record.levelno >= logging.INFO:
            return any(record.name.startswith(prefix) for prefix in self.allowed_prefixes)
        return False


def _build_handlers(level: int, log_file: str = None, format_string: str = None) -> list:
    """Construir los handlers (consola + archivo rotativo) usados por los loggers."""
    formatter = logging.Formatter(format_string or DEFAULT_FORMAT)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    handlers = [console_handler]

    if log_file:
        file_handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)

    return handlers


def setup_root_logging(level: str = "INFO", log_file: str = None,
                       format_string: str = None) -> logging.Logger:
    """
    Configurar el logger RAÍZ para que los logs de handlers/clients
    (logging.getLogger(__name__)) también lleguen a consola y archivo.

    Sin esto solo se persisten los logs del logger del servicio y los errores
    de publicación MQTT se pierden. Idempotente: llamarla dos veces no duplica
    líneas.

    Debe invocarse ANTES de construir clientes/handlers, porque el log de
    conexión del publisher ocurre en su constructor.
    """
    global _root_configured

    requested_level = getattr(logging, level.upper(), logging.INFO)
    root_logger = logging.getLogger()
    root_logger.setLevel(requested_level)

    if not _root_configured:
        for handler in _build_handlers(requested_level, log_file, format_string):
            root_logger.addHandler(handler)
        _root_configured = True

    # Silenciar el ruido de terceros
    for noisy in NOISY_LOGGERS:
        logging.getLogger(noisy).setLevel(logging.WARNING)

    return root_logger


def setup_logger(name: str = "mqtt_app", level: str = "INFO",
                log_file: str = None, format_string: str = None) -> logging.Logger:
    """
    Configurar logger para la aplicación
    
    Args:
        name: Nombre del logger
        level: Nivel de logging (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Archivo de log opcional
        format_string: Formato personalizado de log
    
    Returns:
        Logger configurado
    """
    requested_level = getattr(logging, level.upper(), logging.INFO)
    logger = logging.getLogger(name)
    logger.setLevel(requested_level)
    logger.propagate = False
    
    # Limpiar handlers existentes
    logger.handlers.clear()

    # Handlers propios (consola + archivo rotativo), sin filtro — muestran todo
    for handler in _build_handlers(requested_level, log_file, format_string):
        logger.addHandler(handler)

    return logger


def get_timestamped_filename(base_name: str, extension: str = "log") -> str:
    """
    Generar nombre de archivo con timestamp
    
    Args:
        base_name: Nombre base del archivo
        extension: Extensión del archivo
    
    Returns:
        Nombre de archivo con timestamp
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{base_name}_{timestamp}.{extension}"
