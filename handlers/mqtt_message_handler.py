"""
Manejador de mensajes MQTT puro - SIN WHATSAPP
Solo procesa mensajes MQTT de BOTONERA y comunica con el backend
"""
import json
import logging
import threading
import time
from typing import Dict, Any, Optional, List

from utils.alert_normalizer import (
    AlertNormalizationError,
    build_tv_topic,
    normalize_alert_to_tv,
)
from utils.hardware_liveness_monitor import build_alive_key


_DEDUP_WINDOW_SECONDS = 2
_HARDWARE_REPORT_TYPES = {"heartbeat", "status", "estado", "alarma", "alarmas"}
_HARDWARE_ID_FIELDS = ("id_dispositivo", "id_origen", "device_id", "hardware_id")


def is_hardware_report(topic: str, data: Optional[Dict], retained: bool = False) -> bool:
    """Return True only for telemetry that can be attributed to the device."""
    if retained or not isinstance(data, dict):
        return False

    # Topic: empresas/<empresa>/<sede>/<TIPO>/<hardware>[/<ip>]. Los reportes de status
    # del firmware agregan la IP como 6º segmento, así que se aceptan 5 o 6 partes.
    parts = [part for part in topic.split("/") if part.strip()]
    if len(parts) not in (5, 6) or parts[0] != "empresas":
        return False

    message_type = str(data.get("tipo_mensaje") or "").strip().lower()
    if message_type not in _HARDWARE_REPORT_TYPES:
        return False

    hardware = parts[4].strip()
    identity = next(
        (str(data.get(field)).strip() for field in _HARDWARE_ID_FIELDS if data.get(field)),
        "",
    )
    if identity and identity.casefold() != hardware.casefold():
        return False

    # An alarm without device identity could be a command echoed by the broker.
    if message_type in {"alarma", "alarmas"} and not identity:
        return False

    return True


class MQTTMessageHandler:
    """Manejador de mensajes MQTT puro - SIN dependencias de WhatsApp"""

    def __init__(self, backend_client, mqtt_publisher=None, whatsapp_service=None, config=None):
        self.backend_client = backend_client
        self.mqtt_publisher = mqtt_publisher
        self.whatsapp_service = whatsapp_service  # Solo para ENVIAR cuando el backend lo requiera
        self.config = config
        self.logger = logging.getLogger(__name__)

        # Estadísticas
        self.processed_messages = 0
        self.error_count = 0

        # Usar settings en lugar de .env directo
        self.pattern_topic = config.mqtt.topic if config else "empresas"

        # Dedup: evita procesar el mismo hardware más de 1 vez por ventana de tiempo
        self._last_processed: Dict[str, float] = {}
        self._dedup_lock = threading.Lock()

        # Redis es la autoridad de vida: el primer reporte (o el regreso tras expirar)
        # marca Activo; los siguientes solo renuevan el TTL. Al expirar, el monitor
        # marca Inactivo.
        self._alive_ttl = getattr(config, "hardware_status_ttl_seconds", 30) if config else 30
        # Tipos que NO reportan vida (p. ej. PANTALLA: no es hardware, solo muestra).
        excluded_raw = getattr(config, "hardware_status_excluded_types", "PANTALLA") if config else "PANTALLA"
        self._alive_excluded_types = {
            item.strip().upper() for item in (excluded_raw or "").split(",") if item.strip()
        }
        self._alive_redis = None
        try:
            import redis as _redis
            redis_config = getattr(config, "redis", None) if config else None
            if redis_config is not None:
                self._alive_redis = _redis.Redis(
                    host=redis_config.host,
                    port=redis_config.port,
                    db=redis_config.db,
                    password=redis_config.password,
                    socket_timeout=2,
                    socket_connect_timeout=2,
                )
                self._alive_redis.ping()
                self.logger.info("✅ Vida por Redis activa (ttl=%ss)", self._alive_ttl)
        except Exception as exc:
            self._alive_redis = None
            self.logger.warning("⚠️ Redis no disponible para vida del hardware (%s)", exc)

        self.logger.info("🎯 MQTT Message Handler - SOLO procesamiento MQTT")
        self.logger.info("❌ SIN procesamiento de mensajes WhatsApp entrantes")

    def process_mqtt_message(
        self,
        topic: str,
        payload: str,
        json_data: Optional[Dict] = None,
        retained: bool = False,
    ) -> bool:
        """FILTRO ABSOLUTO PARA BOTONERA - Solo procesar topics que terminen después del hardware"""

        if json_data is None and isinstance(payload, str):
            try:
                json_data = json.loads(payload)
            except json.JSONDecodeError:
                json_data = None

        # Only device-originated telemetry renews liveness. Commands published by this
        # service return through the wildcard too, but must not count as heartbeats.
        self._handle_liveness(topic, json_data, retained=retained)

        # SOLO PROCESAR SI EL TOPIC CONTIENE "BOTONERA" Y TIENE MENSAJES APROPIADOS
        if "BOTONERA" in topic:
            topic_parts = topic.split("/")
            
            # Buscar la posición de BOTONERA
            botonera_index = -1
            for i, part in enumerate(topic_parts):
                if "BOTONERA" in part:
                    botonera_index = i
                    break
            
            # Verificar que después de BOTONERA hay exactamente 1 parte más (el nombre del hardware)
            if botonera_index != -1 and botonera_index + 1 < len(topic_parts):
                # Obtener partes después de BOTONERA
                parts_after_botonera = topic_parts[botonera_index + 1:]
                
                # Filtrar partes vacías (por barras al final como "/")
                parts_after_botonera = [part for part in parts_after_botonera if part.strip()]
                
                # Solo procesar si hay exactamente 1 parte después de BOTONERA
                if len(parts_after_botonera) == 1:
                    hardware_name = parts_after_botonera[0]
                    
                    self.logger.info(f"🚨 BOTONERA: {hardware_name} - {topic}")
                    
                    # Verificar que el payload no sea de tipo desactivación
                    if payload and isinstance(payload, str):
                        try:
                            json_data = json.loads(payload)
                            if json_data.get("tipo_alarma") == "NORMAL":
                                self.logger.info(f"⚠️ Ignorando mensaje de tipo NORMAL para {hardware_name}")
                                return True
                        except json.JSONDecodeError:
                            self.logger.error(f"❌ JSON inválido en payload: {payload}")
                            return False
                        except Exception as e:
                            self.logger.error(f"❌ Error al procesar payload: {e}")
                            return False
                    
                    # Procesar JSON de BOTONERA (cualquier estructura)
                    try:
                        #json_data = json.loads(payload)
                        
                        # ENVIAR DATOS AL BACKEND (usando hilo)
                        self._send_botonera_to_backend(hardware_name, json_data, topic, payload)
                        
                        self.processed_messages += 1
                        return True
                        
                    except json.JSONDecodeError as e:
                        self.logger.error(f"❌ JSON inválido: {e}")
                        self.error_count += 1
                        return False
                    except Exception as e:
                        self.logger.error(f"❌ Error procesando: {e}")
                        self.error_count += 1
                        return False
                else:
                    # Ignorar si hay más de 1 parte después de BOTONERA
                    return True
            else:
                # No hay partes después de BOTONERA, ignorar
                return True
        
        # IGNORAR CUALQUIER TOPIC QUE NO CONTENGA "BOTONERA"
        else:
            # No mostrar ni procesar otros topics
            return True

    def _handle_liveness(
        self,
        topic: str,
        data: Optional[Dict],
        retained: bool = False,
    ) -> None:
        """Renueva la vida únicamente a partir de un reporte atribuible al hardware.

        Topic esperado: empresas/<empresa>/<sede>/<TIPO>/<hardware>. El backend resuelve
        el hardware por empresa_nombre + hardware_nombre, así que se toman del topic.
        """
        if not is_hardware_report(topic, data, retained=retained):
            self.logger.debug("Mensaje MQTT ignorado para liveness topic=%s", topic)
            return
        parts = [part for part in topic.split("/") if part.strip()]
        empresa = parts[1]
        tipo = parts[3]
        # parts[4] es el hardware; un posible parts[5] es la IP (reportes de status).
        hardware = parts[4]
        # Las pantallas (y demás tipos excluidos) no son hardware con vida: no se refrescan.
        if tipo.upper() in self._alive_excluded_types:
            return

        if self._alive_redis is not None:
            key = build_alive_key(empresa, hardware)
            try:
                # SET NX EX: `added` es True solo si la clave no existía → primer latido o
                # regreso tras expirar. Ese es el único momento en que hay que marcar Activo.
                added = self._alive_redis.set(key, "1", nx=True, ex=self._alive_ttl)
                if added:
                    self._activate(empresa, hardware)
                else:
                    # Sigue vivo: refresca la ventana sin escribir al backend.
                    self._alive_redis.expire(key, self._alive_ttl)
                return
            except Exception as exc:
                self.logger.warning("⚠️ Redis vida falló (%s)", exc)

        self.logger.error(
            "Liveness ignorado: Redis no esta disponible empresa=%s hardware=%s",
            empresa,
            hardware,
        )

    def _activate(self, empresa: str, hardware: str) -> None:
        """Marca el hardware Activo en el backend (en otro hilo, fuera del hilo MQTT)."""
        threading.Thread(
            target=self._refresh_liveness_thread, args=(empresa, hardware), daemon=True
        ).start()

    def _refresh_liveness_thread(self, empresa: str, hardware: str) -> None:
        """Envía el refresco de vida al backend (fuera del hilo MQTT)."""
        try:
            ok = self.backend_client.send_physical_status(
                empresa, hardware, {"estado": "Activo"}
            )
            if ok:
                self.logger.info("💚 Vida refrescada empresa=%s hardware=%s", empresa, hardware)
            else:
                self.logger.warning(
                    "💔 No se pudo refrescar vida empresa=%s hardware=%s", empresa, hardware
                )
        except Exception as exc:
            self.logger.error("❌ Error refrescando vida: %s", exc)

    def _is_duplicate(self, topic: str) -> bool:
        """Retorna True si ya procesamos este topic dentro de la ventana de dedup."""
        now = time.time()
        with self._dedup_lock:
            last = self._last_processed.get(topic, 0)
            if now - last < _DEDUP_WINDOW_SECONDS:
                return True
            self._last_processed[topic] = now
            return False

    def _send_botonera_to_backend(self, hardware_name: str, data: Dict, topic: str, payload: str) -> None:
        """Enviar mensaje de BOTONERA al backend usando un nuevo hilo"""
        if self._is_duplicate(topic):
            self.logger.info("⏭️ Dedup: ignorando %s (ya procesado en los últimos %ss)", topic, _DEDUP_WINDOW_SECONDS)
            return
        thread = threading.Thread(target=self._send_alarm_thread, args=(hardware_name, data, topic, payload), daemon=True)
        thread.start()

    def _send_alarm_thread(self, hardware_name: str, data: Dict, topic: str, payload: str):
        """Enviar mensaje de BOTONERA al backend"""
        try:
            # Extraer datos reales del topic: empresas/nombre_empresa/sede_empresa/BOTONERA/nombre_hardware
            topic_parts = topic.split("/")
            
            # Validar estructura del topic
            if len(topic_parts) < 5 or topic_parts[0] != "empresas":
                self.logger.error(f"❌ Formato de topic inválido")
                return False
                
            # Extraer partes del topic
            empresa = topic_parts[1]
            sede = topic_parts[2] 
            tipo_hardware = topic_parts[3]  # BOTONERA
            nombre_hardware = topic_parts[4]
            
            # Crear estructura de datos con SOLO los campos requeridos
            mqtt_data = {
                "empresa": empresa,
                "sede": sede,
                "tipo_hardware": tipo_hardware,
                "nombre_hardware": nombre_hardware,
                "data": data
            }
            
            # Autenticar y enviar alarma
            token = self.backend_client.authenticate_hardware(mqtt_data)
            
            if not token:
                self.logger.error(
                    "❌ Autenticación fallida empresa=%s sede=%s hardware=%s",
                    mqtt_data.get('empresa'), mqtt_data.get('sede'), mqtt_data.get('nombre_hardware')
                )
                return False
            
            response = self.backend_client.send_alarm_data(mqtt_data, token)
            #print(json.dumps(response,indent=4))
            if response:
                self._handle_alarm_notifications(response, mqtt_data)
                return True
            else:
                self.logger.error(f"❌ Error enviando alarma")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Error: {e}")
            return False

    def _handle_alarm_notifications(self, response: Dict, mqtt_data: Dict) -> None:
        """Procesar respuesta del backend: solo fanout MQTT a otros hardware.
        WhatsApp (usuarios + managers) lo maneja process_empresa_activation via HTTP callback del backend."""
        try:
            if self.logger.isEnabledFor(logging.INFO):
                self.logger.info(
                    "📥 Respuesta cruda del backend: %s",
                    json.dumps(response, ensure_ascii=False, default=str)
                )
                self.logger.info(
                    "🛰️ Payload original desde MQTT: %s",
                    json.dumps(mqtt_data, ensure_ascii=False, default=str)
                )

            alert_data = self._resolve_alert_data(response, mqtt_data)

            if not alert_data:
                self.logger.warning("⚠️ Respuesta del backend sin datos de alerta")
                return

            # Solo fanout MQTT a otros hardware (SEMAFORO, PANTALLA, etc.)
            # WhatsApp lo delega el backend via trigger_fanout → process_empresa_activation
            self._send_mqtt_message(alert_data=alert_data, mqtt_data=mqtt_data)
            self.logger.info("✅ Fanout MQTT completado")

        except Exception as e:
            self.logger.error(f"❌ Error manejando notificaciones: {e}")
    
    def _intermediate_to_mqtt(self, alert, topics) -> None:
        """Enviar alertas a MQTT - IGUAL al WebSocket handler"""
        try:
            # Enviar alerta a mqtt usando el mismo método que WebSocket
            for topic in topics:
                full_topic = self.pattern_topic + "/" + topic
                message_hardware = self._select_data_hardware(alert=alert, topic=full_topic)
                self.send_mqtt_message(topic=full_topic, message_data=message_hardware)

        except Exception as ex:
            self.logger.error(f"❌ Error en el intermediario a enviar mensajes al mqtt: {ex}")

    def _resolve_alert_data(self, backend_response: Dict, mqtt_data: Dict) -> Dict:
        """Unificar los datos de alerta aunque el backend no entregue la clave `alert`"""
        alert_data = backend_response.get("alert")
        if alert_data:
            return alert_data

        derived: Dict[str, Any] = {}
        keys_to_copy = [
            "topics_otros_hardware",
            "numeros_telefonicos",
            "activacion_alerta",
            "ubicacion",
            "elementos_necesarios",
            "instrucciones",
            "prioridad",
            "tipo_alerta",
            "nombre_alerta",
            "empresa_nombre",
            "empresa",
            "sede",
            "image_alert",
            "imagen_base64",
            "alert_id",
            "_id",
            "descripcion",
            "fecha_creacion",
        ]

        for key in keys_to_copy:
            value = backend_response.get(key)
            if value is not None:
                derived[key] = value

        derived.setdefault("topics_otros_hardware", [])
        derived.setdefault("numeros_telefonicos", [])
        derived.setdefault("activacion_alerta", {})
        derived.setdefault("ubicacion", {})
        derived.setdefault("elementos_necesarios", [])
        derived.setdefault("instrucciones", [])

        if "tipo_alerta" not in derived and derived.get("nombre_alerta"):
            derived["tipo_alerta"] = derived["nombre_alerta"]
        if "nombre_alerta" not in derived and derived.get("tipo_alerta"):
            derived["nombre_alerta"] = derived["tipo_alerta"]

        if "empresa" not in derived and mqtt_data.get("empresa"):
            derived["empresa"] = mqtt_data["empresa"]
        if "empresa_nombre" not in derived:
            empresa_nombre = derived.get("empresa") or mqtt_data.get("empresa")
            if empresa_nombre:
                derived["empresa_nombre"] = empresa_nombre
        if "sede" not in derived and mqtt_data.get("sede"):
            derived["sede"] = mqtt_data["sede"]

        data_payload = backend_response.get("data") or mqtt_data.get("data")
        if data_payload:
            derived["data"] = data_payload
        else:
            derived.setdefault("data", {})

        has_meaningful_data = any([
            derived.get("topics_otros_hardware"),
            derived.get("numeros_telefonicos"),
            derived.get("tipo_alerta"),
            derived.get("nombre_alerta"),
        ])

        return derived if has_meaningful_data else {}

    def _send_mqtt_message(self, alert_data: Dict, mqtt_data: Dict) -> None:
        """Enviar mensajes MQTT a otros hardware - IGUAL al WebSocket handler"""
        try:
            if not alert_data:
                self.logger.warning("⚠️ No hay datos de alerta para enviar a otros hardware")
                return

            alert_payload = dict(alert_data)

            if self.logger.isEnabledFor(logging.INFO):
                self.logger.info(
                    "📨 alert_data utilizado para fanout MQTT: %s",
                    json.dumps(alert_payload, ensure_ascii=False, default=str)
                )
                self.logger.info(
                    "🔁 mqtt_data de origen: %s",
                    json.dumps(mqtt_data, ensure_ascii=False, default=str)
                )

            if not alert_payload.get("data"):
                alert_payload["data"] = mqtt_data.get("data", {})

            topics = alert_payload.get("topics_otros_hardware", [])
            if not topics:
                self.logger.info("ℹ️ No hay topics adicionales de hardware para activar")
                return

            # Enviar mensajes usando el mismo método que WebSocket
            self._intermediate_to_mqtt(alert=alert_payload, topics=topics)

        except Exception as e:
            self.logger.error(f"❌ Error enviando mensajes MQTT: {e}")

    def _select_data_hardware(self, topic, alert: Dict) -> Dict:
        """Seleccionar datos específicos según el tipo de hardware - IGUAL al WebSocket handler"""
      #  print(json.dumps(alert,indent=4))
        data_alert = alert.get("data", {}) or {}
        alarm_color = data_alert.get("tipo_alarma") or alert.get("nombre_alerta") or ""
        
        if "SEMAFORO" in topic:
            message_data = {
                "tipo_alarma": alarm_color,
            }
        elif "PANTALLA" in topic:
            if str(alarm_color).upper() == "NORMAL":
                return {
                    "tipo_alarma": "NORMAL",
                    "prioridad": alert.get("prioridad", "").upper()
                }
            try:
                message_data = normalize_alert_to_tv(alert)
            except AlertNormalizationError as exc:
                self.logger.error(f"❌ Error normalizando alerta para PANTALLA: {exc}")
                message_data = {"alert": alert}
            except Exception as exc:
                self.logger.error(f"❌ Error inesperado normalizando alerta para PANTALLA: {exc}")
                message_data = {"alert": alert}
        else:
            message_data = {
                "action": "generic",
                "message": "notificación genérica",
            }
            
        return message_data

    def _resolve_tv_topic_parts(self, alert_data: Dict, mqtt_data: Dict) -> tuple[str, str, str]:
        empresa = (
            alert_data.get("empresa")
            or alert_data.get("empresa_nombre")
            or mqtt_data.get("empresa")
            or "desconocida"
        )
        sede = alert_data.get("sede") or mqtt_data.get("sede") or "desconocida"
        pantalla = (
            alert_data.get("pantalla")
            or alert_data.get("nombre_pantalla")
            or "principal"
        )
        return str(empresa), str(sede), str(pantalla)

    def _publish_tv_alert(self, alert_data: Dict, mqtt_data: Dict) -> None:
        if not self.mqtt_publisher:
            self.logger.warning("⚠️ No hay cliente MQTT publisher disponible para TV")
            return

        try:
            normalized = normalize_alert_to_tv(alert_data)
        except AlertNormalizationError as exc:
            self.logger.error(f"❌ Error normalizando alerta para TV: {exc}")
            return
        except Exception as exc:
            self.logger.error(f"❌ Error inesperado normalizando alerta para TV: {exc}")
            return

        empresa, sede, pantalla = self._resolve_tv_topic_parts(alert_data, mqtt_data)
        topic = build_tv_topic(empresa=empresa, sede=sede, pantalla=pantalla)
        self.send_mqtt_message(topic=topic, message_data=normalized)
  
    def _send_location_personalized_message(self, numeros_data: list, hardware_location: Dict) -> bool:
        """Enviar mensaje de ubicación por WhatsApp usando CTA 'Abrir en Maps'"""

        if not self.whatsapp_service:
            self.logger.warning("⚠️ WhatsApp service no disponible")
            return False

        try:
            url_maps = hardware_location.get("url_maps")
            if not url_maps:
                self.logger.warning("⚠️ No hay URL de ubicación disponible")
                return False

            success = self.whatsapp_service.send_bulk_location_button_message(
                recipients=numeros_data,
                url_maps=url_maps,
                footer_text="Equipo RESCUE",
                use_queue=True
            )

            if not success:
                self.logger.error("❌ Error enviando mensaje de ubicación con CTA")

            return success

        except Exception as e:
            self.logger.error(f"❌ Error enviando mensaje de ubicación: {e}")
            return False

    def _log_template_sends(self, alert_id, recipients: List[Dict], summary_body: str) -> None:
        """Fire-and-forget: registra envíos de plantilla en backend (is_template=True)"""
        if not self.backend_client or not alert_id:
            return
        try:
            import threading

            alert_id_str = str(alert_id) if alert_id else None

            def _fire():
                try:
                    for r in recipients:
                        phone = r.get("phone")
                        if not phone:
                            continue
                        payload = {
                            "phone": phone,
                            "direction": "out",
                            "type": "template",
                            "body": summary_body,
                            "payload": r,
                            "is_template": True
                        }
                        self.backend_client.log_alert_message(alert_id=alert_id_str, payload=payload)
                except Exception:
                    pass

            threading.Thread(target=_fire, daemon=True).start()
        except Exception as exc:
            self.logger.debug(f"_log_template_sends ignorado: {exc}")


    def send_mqtt_message(self, topic: str, message_data: Dict, qos: int = 0) -> bool:
        """Enviar mensajes MQTT a un topic específico"""
        try:
            # Usar mqtt_publisher si está disponible
            if self.mqtt_publisher:
                success = self.mqtt_publisher.publish_json(topic, message_data, qos)
                
                if success:
                    self.logger.info(f"✅ Mensaje MQTT enviado a topic: {topic}")
                    return True
                else:
                    self.logger.error(f"❌ Error enviando mensaje MQTT a topic: {topic}")
                    return False
            else:
                self.logger.warning(f"⚠️ No hay cliente MQTT publisher disponible")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Error enviando mensaje MQTT: {e}")
            return False

    def _extract_phone_number(self, data: Dict[str, Any]) -> str:
        """Obtener y normalizar un número telefónico desde un payload genérico"""
        if not isinstance(data, dict):
            return ""

        numero = (
            data.get("numero")
            or data.get("telefono")
            or data.get("phone")
        )

        if not isinstance(numero, str):
            return ""

        normalized = numero.strip()
        if normalized.startswith("+"):
            normalized = normalized[1:]

        return normalized

    def _normalize_usuarios_list(self, usuarios: List[Dict]) -> List[Dict]:
        """Asegurar que cada usuario tenga un número válido y normalizado"""
        normalized_users: List[Dict[str, Any]] = []

        if not isinstance(usuarios, list):
            return normalized_users

        for usuario in usuarios:
            phone = self._extract_phone_number(usuario)
            if not phone:
                continue

            usuario_copy = dict(usuario)
            usuario_copy["numero"] = phone
            normalized_users.append(usuario_copy)

        return normalized_users


    def get_statistics(self) -> Dict[str, Any]:
        """Obtener estadísticas del manejador MQTT"""
        return {
            "processed_messages": self.processed_messages,
            "error_count": self.error_count,
            "error_rate": round(self.error_count / max(self.processed_messages, 1) * 100, 2),
            "hardware_types_count": 1  # Solo BOTONERA
        }
