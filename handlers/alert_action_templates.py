"""Plantillas de los mensajes que representan las acciones del usuario en la
conversación de la alerta (las que ve la empresa en el panel).

No son las plantillas/menús de WhatsApp: son el texto "respuesta del usuario" que
se registra en la conversación cuando el usuario ejecuta una acción (embarcarse,
apagar la alarma, marcar disponibilidad, pedir ubicación). Redacción en tercera
persona con el nombre del usuario, sin emojis. Centralizado aquí para poder
ajustar el copy en un solo lugar.
"""


def _nombre(nombre) -> str:
    nombre = (nombre or "").strip()
    if not nombre:
        return "El usuario"
    # Primer nombre para que quede natural ("Nicolas va en camino").
    return nombre.split()[0]


def embarcado_message(nombre) -> str:
    return f"{_nombre(nombre)} va en camino a la emergencia."


def apagar_message(nombre) -> str:
    return f"{_nombre(nombre)} apagó la alarma."


def disponible_message(nombre) -> str:
    return f"{_nombre(nombre)} confirmó disponibilidad."


def ubicacion_message(nombre) -> str:
    return f"{_nombre(nombre)} solicitó la ubicación de la alarma."


# Mapa acción -> constructor, para registrar de forma uniforme desde los handlers.
ACTION_MESSAGE_BUILDERS = {
    "EMBARCADO": embarcado_message,
    "APAGAR": apagar_message,
    "DISPONIBLE": disponible_message,
    "UBICACION": ubicacion_message,
}


def action_message(action: str, nombre) -> str:
    """Devuelve el texto de conversación para una acción, o '' si no aplica."""
    builder = ACTION_MESSAGE_BUILDERS.get((action or "").upper())
    return builder(nombre) if builder else ""
