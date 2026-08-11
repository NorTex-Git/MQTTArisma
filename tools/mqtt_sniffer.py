#!/usr/bin/env python3
"""
Sniffer MQTT de diagnóstico: imprime TODO lo que se publica bajo el topic base.

Uso:
    python tools/mqtt_sniffer.py            # escucha empresas/#
    python tools/mqtt_sniffer.py "empresas/MiEmpresa/#"

Sirve para saber quién publica y quién no:
  1. Deja el sniffer corriendo.
  2. Dispara la botonera  -> deben verse los mensajes a SEMAFORO/PANTALLA.
  3. Crea alerta desde web -> si NO aparece nada, el fanout de websocket_service no publica.
  4. Crea alerta por WhatsApp / apaga la alarma -> idem.

Solo lee. No publica nada.
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import paho.mqtt.client as mqtt  # noqa: E402
from config.settings import MQTTConfig  # noqa: E402


def main() -> int:
    config = MQTTConfig()
    topic_filter = sys.argv[1] if len(sys.argv) > 1 else f"{config.topic}/#"

    print(f"broker={config.broker}:{config.port} transport={config.transport} tls={config.tls}")
    print(f"filtro={topic_filter}")

    client = mqtt.Client(f"sniffer-{int(time.time())}", transport=config.transport)
    client.username_pw_set(config.username, config.password)
    if config.transport == "websockets":
        client.ws_set_options(path=config.ws_path)
        if config.tls:
            import ssl

            client.tls_set(cert_reqs=ssl.CERT_REQUIRED)

    def on_connect(_client, _userdata, _flags, rc):
        if rc != 0:
            print(f"❌ conexión rechazada rc={rc}")
            return
        print("✅ conectado, esperando mensajes (Ctrl+C para salir)\n")
        _client.subscribe(topic_filter, qos=0)

    def on_message(_client, _userdata, msg):
        payload = msg.payload.decode(errors="replace")
        try:
            payload = json.dumps(json.loads(payload), ensure_ascii=False)[:400]
        except json.JSONDecodeError:
            payload = payload[:400]
        print(f"{time.strftime('%H:%M:%S')} | {msg.topic}\n    {payload}\n")

    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(config.broker, config.port, config.keep_alive)
    except Exception as exc:
        print(f"❌ no se pudo conectar: {exc}")
        return 1

    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("\nfin")
    finally:
        client.disconnect()
    return 0


if __name__ == "__main__":
    sys.exit(main())
