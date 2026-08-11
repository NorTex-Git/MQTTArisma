"""
Mini cliente MQTT para publicación que reutiliza el MQTTClient existente
"""

import logging
import threading
import time
from typing import Dict, Any, Optional
from clients.mqtt_client import MQTTClient
from config.settings import MQTTConfig

# Espera máxima a que el broker confirme la conexión inicial
CONNECT_TIMEOUT_SECONDS = 5
# Intervalo mínimo entre intentos de reconexión manual
RECONNECT_THROTTLE_SECONDS = 10


class MQTTPublisherLite:
    """
    Mini cliente MQTT para publicación que reutiliza el MQTTClient existente
    sin dañar la lógica actual ni los callbacks
    """

    def __init__(self, config: MQTTConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)

        # Crear un MQTTClient pero configurado solo para publicar
        self.mqtt_client = MQTTClient(config)

        # Configurar callbacks minimalistas (sin suscripciones automáticas)
        self._setup_publisher_callbacks()

        self.is_connected = False
        self.publish_count = 0
        self.error_count = 0
        self._loop_started = False
        self._last_reconnect_attempt = 0.0
        self._reconnect_lock = threading.Lock()
    
    def _setup_publisher_callbacks(self):
        """Configurar callbacks minimalistas solo para publicación"""
        
        def minimal_connect_callback(client, userdata, flags, rc):
            """Callback de conexión sin suscripciones automáticas"""
            if rc == 0:
                self.is_connected = True
                self.logger.info("📤 MQTT Publisher conectado al broker")
            else:
                self.is_connected = False
                self.logger.error(f"❌ Error conectando MQTT Publisher: {rc}")
        
        def minimal_disconnect_callback(client, userdata, rc):
            """Callback de desconexión"""
            self.is_connected = False
            self.logger.info("📡 MQTT Publisher desconectado")
        
        # Asignar callbacks minimalistas
        self.mqtt_client.set_connect_callback(minimal_connect_callback)
        self.mqtt_client.set_disconnect_callback(minimal_disconnect_callback)
        
        # NO configurar callback de mensajes para evitar logs innecesarios
        # self.mqtt_client.set_message_callback(None)
    
    def connect(self) -> bool:
        """
        Programar la conexión al broker y arrancar el loop de red.

        No bloquea el arranque del servicio: si el broker no responde, paho
        sigue reintentando solo (reconnect_delay_set) y `ensure_connected`
        cubre el resto. El publisher NUNCA debe descartarse por este retorno.
        """
        try:
            self.mqtt_client.connect_async()
            self._start_loop_once()

            start_time = time.time()
            while not self.is_connected and (time.time() - start_time) < CONNECT_TIMEOUT_SECONDS:
                time.sleep(0.1)

            if self.is_connected:
                self.logger.info(
                    "✅ MQTT Publisher conectado y listo (%s:%s, client_id=%s)",
                    self.config.broker, self.config.port, self.config.client_id
                )
                return True

            self.logger.error(
                "❌ MQTT Publisher aún sin conectar a %s:%s (client_id=%s) — "
                "se reintentará en segundo plano",
                self.config.broker, self.config.port, self.config.client_id
            )
            return False

        except Exception as e:
            self.logger.error(f"❌ Error conectando al broker: {e}")
            return False

    def _start_loop_once(self) -> None:
        """Arrancar el loop de red de paho una sola vez"""
        if not self._loop_started:
            self.mqtt_client.start_loop()
            self._loop_started = True

    def ensure_connected(self) -> bool:
        """
        Garantizar conexión antes de publicar.

        Si paho aún no reconectó, fuerza un intento como máximo cada
        RECONNECT_THROTTLE_SECONDS. Thread-safe: publican tanto los workers de
        Redis como el hilo HTTP interno.
        """
        if self.is_connected:
            return True

        with self._reconnect_lock:
            if self.is_connected:
                return True

            now = time.time()
            if now - self._last_reconnect_attempt < RECONNECT_THROTTLE_SECONDS:
                return False
            self._last_reconnect_attempt = now

            self.logger.warning(
                "⚠️ MQTT Publisher desconectado (%s:%s) — reintentando conexión",
                self.config.broker, self.config.port
            )
            try:
                self.mqtt_client.connect_async()
                self._start_loop_once()
            except Exception as e:
                self.logger.error(f"❌ Error reintentando conexión MQTT: {e}")
                return False

        start_time = time.time()
        while not self.is_connected and (time.time() - start_time) < CONNECT_TIMEOUT_SECONDS:
            time.sleep(0.1)

        if self.is_connected:
            self.logger.info("✅ MQTT Publisher reconectado")
        return self.is_connected
    
    def disconnect(self):
        """
        Desconectar del broker
        Reutiliza el método disconnect del MQTTClient existente
        """
        try:
            self.mqtt_client.stop_loop()
            self.mqtt_client.disconnect()
            self.is_connected = False
            self._loop_started = False
            self.logger.info("✅ MQTT Publisher desconectado")
        except Exception as e:
            self.logger.error(f"❌ Error desconectando: {e}")
    
    def publish(self, topic: str, message: str, qos: int = 0) -> bool:
        """
        Publicar mensaje de texto
        Reutiliza el método publish del MQTTClient existente
        """
        if not self.ensure_connected():
            self.logger.error(
                "❌ MQTT Publisher no conectado a %s:%s — mensaje NO enviado a %s",
                self.config.broker, self.config.port, topic
            )
            self.error_count += 1
            return False

        try:
            success = self.mqtt_client.publish(topic, message, qos)
            if success:
                self.publish_count += 1
                self.logger.info(f"📤 Mensaje publicado en {topic}")
            else:
                self.error_count += 1
                self.logger.error(f"❌ Error publicando mensaje en {topic}")
            
            return success
            
        except Exception as e:
            self.error_count += 1
            self.logger.error(f"❌ Excepción publicando mensaje: {e}")
            return False
    
    def publish_json(self, topic: str, data: Dict[str, Any], qos: int = 0) -> bool:
        """
        Publicar datos JSON
        Reutiliza el método publish_json del MQTTClient existente
        """
        if not self.ensure_connected():
            self.logger.error(
                "❌ MQTT Publisher no conectado a %s:%s — JSON NO enviado a %s",
                self.config.broker, self.config.port, topic
            )
            self.error_count += 1
            return False

        try:
            success = self.mqtt_client.publish_json(topic, data, qos)
            if success:
                self.publish_count += 1
                self.logger.info(f"📤 JSON publicado en {topic}")
            else:
                self.error_count += 1
                self.logger.error(f"❌ Error publicando JSON en {topic}")
            
            return success
            
        except Exception as e:
            self.error_count += 1
            self.logger.error(f"❌ Excepción publicando JSON: {e}")
            return False
    
    def get_status(self) -> Dict[str, Any]:
        """Obtener estado del publisher"""
        return {
            "connected": self.is_connected,
            "broker": self.config.broker,
            "port": self.config.port,
            "transport": self.config.transport,
            "tls": self.config.tls,
            "topic": self.config.topic,
            "client_id": self.config.client_id,
            "messages_published": self.publish_count,
            "errors": self.error_count,
            "success_rate": round(
                (self.publish_count / max(self.publish_count + self.error_count, 1)) * 100, 2
            )
        }
    
    def get_underlying_client(self) -> MQTTClient:
        """
        Obtener el cliente MQTT subyacente para acceso avanzado
        (solo si es necesario)
        """
        return self.mqtt_client
