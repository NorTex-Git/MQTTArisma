"""
Cliente MQTT escalable y reutilizable
"""
import json
import logging
import time
from typing import Callable, Optional, Any, Dict
import paho.mqtt.client as mqtt
from config.settings import MQTTConfig


class MQTTClient:
    """Cliente MQTT escalable con callbacks personalizables"""
    
    def __init__(self, config: MQTTConfig):
        self.config = config
        self.client = mqtt.Client(config.client_id, transport=config.transport)
        self.is_connected = False
        self.logger = logging.getLogger(__name__)

        # Callbacks personalizables
        self.on_message_callback: Optional[Callable] = None
        self.on_connect_callback: Optional[Callable] = None
        self.on_disconnect_callback: Optional[Callable] = None

        # Control de logs para evitar spam
        self.first_connection = True
        self.connection_count = 0

        # Configurar cliente
        self._setup_client()
    
    def _setup_client(self):
        """Configurar el cliente MQTT"""
        self.client.username_pw_set(self.config.username, self.config.password)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect
        self.client.reconnect_delay_set(min_delay=1, max_delay=120)

        if self.config.transport == "websockets":
            self.client.ws_set_options(path=self.config.ws_path)
            if self.config.tls:
                import ssl
                self.client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
                self.logger.info(f"🔒 MQTT sobre WSS (TLS habilitado) — path: {self.config.ws_path}")
    
    def _should_display_message(self, topic: str) -> bool:
        """Determinar si un mensaje debe mostrarse (solo BOTONERA válidos)"""
        # SOLO procesar si el topic contiene "BOTONERA"
        if "BOTONERA" in topic:
            topic_parts = topic.split("/")
            
            # Buscar la posición de BOTONERA
            botonera_index = -1
            for i, part in enumerate(topic_parts):
                if "BOTONERA" in part:
                    botonera_index = i
                    break
            
            # Verificar que después de BOTONERA hay exactamente 1 parte más
            if botonera_index != -1 and botonera_index + 1 < len(topic_parts):
                # Obtener partes después de BOTONERA
                parts_after_botonera = topic_parts[botonera_index + 1:]
                
                # Filtrar partes vacías (por barras al final como "/")
                parts_after_botonera = [part for part in parts_after_botonera if part.strip()]
                
                # Solo mostrar si hay exactamente 1 parte después de BOTONERA
                return len(parts_after_botonera) == 1
        
        return False
    
    def _on_connect(self, client, userdata, flags, rc):
        """Callback interno para conexión"""
        if rc == 0:
            self.is_connected = True
            self.connection_count += 1
            if self.connection_count > 1:
                self.logger.info("Reconexión exitosa al broker MQTT")
            
            # Solo mostrar log la primera vez o cada 10 reconexiones
            if self.first_connection or self.connection_count % 10 == 0:
                self.logger.info(f"✅ Conectado al broker MQTT (conexión #{self.connection_count})")
                self.first_connection = False
            
            # Ejecutar callback personalizado si existe
            if self.on_connect_callback:
                self.on_connect_callback(client, userdata, flags, rc)
        else:
            self.is_connected = False
            self.logger.error(f"Fallo al conectar, código de retorno {rc}")
    
    def _on_message(self, client, userdata, msg):
        """Callback interno para mensajes recibidos - SOLO muestra BOTONERA válidos"""
        try:
            topic = msg.topic
            payload = msg.payload.decode()
            
            # FILTRO PREVIO: Solo mostrar si es BOTONERA válido
            should_display = self._should_display_message(topic)
            
            if should_display:
                # Intentar parsear JSON solo para BOTONERA válidos
                try:
                    json_data = json.loads(payload)
                except json.JSONDecodeError:
                    json_data = None

                if self.logger.isEnabledFor(logging.INFO):
                    self.logger.info("=" * 100)
                    self.logger.info("🎯 MENSAJE MQTT RECIBIDO 🎯")
                    self.logger.info("📡 TOPIC: %s", topic)
                    self.logger.info("📦 PAYLOAD: %s", payload)
                    self.logger.info("📊 QoS: %s", msg.qos)
                    self.logger.info("🔄 Retain: %s", msg.retain)
                    if json_data is not None:
                        self.logger.info("✅ JSON VÁLIDO: %s", json.dumps(json_data, indent=2))
                    else:
                        self.logger.info("⚠️ NO ES JSON VÁLIDO")
                    self.logger.info("=" * 100)
            else:
                # Para mensajes no-BOTONERA, parsear JSON sin mostrar
                try:
                    json_data = json.loads(payload)
                except json.JSONDecodeError:
                    json_data = None
            
            # Ejecutar callback personalizado si existe (siempre)
            if self.on_message_callback:
                self.on_message_callback(topic, payload, json_data, bool(msg.retain))
                
        except Exception as e:
            self.logger.error(f"💥 ERROR PROCESANDO MENSAJE: {e}")
    
    def _on_disconnect(self, client, userdata, rc):
        """Callback interno para desconexión"""
        self.is_connected = False
        # Solo mostrar log de desconexión si es un error (rc != 0)
        if rc != 0:
            self.logger.warning(f"Desconexión inesperada del broker MQTT: {rc}, reintentando...")
        
        if self.on_disconnect_callback:
            self.on_disconnect_callback(client, userdata, rc)
    
    def connect(self) -> bool:
        """Conectar al broker MQTT"""
        try:
            self.client.connect(
                self.config.broker,
                self.config.port,
                self.config.keep_alive
            )
            return True
        except Exception as e:
            self.logger.error(f"Error conectando al broker: {e}")
            return False

    def connect_async(self) -> bool:
        """
        Conectar al broker sin bloquear ni fallar si el broker no responde.

        paho resuelve la conexión dentro del loop de red y, con
        reconnect_delay_set(1, 120), reintenta indefinidamente por su cuenta.
        Usar esto en publishers para que un broker caído al arranque no deje
        el servicio inutilizable de forma permanente.
        """
        try:
            self.client.connect_async(
                self.config.broker,
                self.config.port,
                self.config.keep_alive
            )
            return True
        except Exception as e:
            self.logger.error(f"Error programando conexión al broker: {e}")
            return False
    
    def start_loop(self):
        """Iniciar el loop de MQTT (non-blocking)"""
        self.client.loop_start()
    
    def stop_loop(self):
        """Detener el loop de MQTT"""
        self.client.loop_stop()
    
    def disconnect(self):
        """Desconectar del broker"""
        self.client.disconnect()
        self.is_connected = False
    
    def publish(self, topic: str, message: str, qos: int = 0) -> bool:
        """Publicar mensaje en un tema"""
        if not self.is_connected:
            self.logger.warning("No conectado al broker. No se puede publicar.")
            return False
        
        try:
            result = self.client.publish(topic, message, qos)
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                self.logger.info(f"Mensaje publicado en {topic}: {message}")
                return True
            else:
                self.logger.error(f"Error publicando mensaje: {result.rc}")
                return False
        except Exception as e:
            self.logger.error(f"Excepción al publicar: {e}")
            return False
    
    def publish_json(self, topic: str, data: Dict[str, Any], qos: int = 0) -> bool:
        """Publicar datos JSON en un tema"""
        try:
            json_message = json.dumps(data)
            return self.publish(topic, json_message, qos)
        except Exception as e:
            self.logger.error(f"Error serializando JSON: {e}")
            return False
    
    def subscribe(self, topic: str, qos: int = 0):
        """Suscribirse a un tema adicional"""
        if self.is_connected:
            self.client.subscribe(topic, qos)
            self.logger.info(f"Suscrito a {topic}")
    
    def unsubscribe(self, topic: str):
        """Desuscribirse de un tema"""
        if self.is_connected:
            self.client.unsubscribe(topic)
            self.logger.info(f"Desuscrito de {topic}")
    
    def set_message_callback(self, callback: Callable):
        """Establecer callback para mensajes recibidos"""
        self.on_message_callback = callback
    
    def set_connect_callback(self, callback: Callable):
        """Establecer callback para conexión"""
        self.on_connect_callback = callback
    
    def set_disconnect_callback(self, callback: Callable):
        """Establecer callback para desconexión"""
        self.on_disconnect_callback = callback
