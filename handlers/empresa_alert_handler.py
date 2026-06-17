"""
Handler específico para alertas desactivadas por empresa
Solo maneja WhatsApp Service y MQTT Publisher
"""
import logging
from typing import Dict, Any, Optional, List

from utils.alert_normalizer import (
    AlertNormalizationError,
    build_tv_topic,
    normalize_alert_to_tv,
)
from clients.mqtt_publisher_lite import MQTTPublisherLite
from config.settings import MQTTConfig
from models.alert_user import make_alert_user, resolve_creator_phone
from models.alert_message_type import BROADCAST_MESSAGE_TYPES


class EmpresaAlertHandler:
    """Handler específico para alertas desactivadas por empresa"""
    
    def __init__(self, whatsapp_service=None, config=None, enable_mqtt_publisher=True, backend_client=None):
        self.whatsapp_service = whatsapp_service
        self.config = config
        self.backend_client = backend_client
        self.logger = logging.getLogger(__name__)

        # Estadísticas específicas
        self.processed_count = 0
        self.error_count = 0
        
        # Configurar pattern topic igual que en websocket handler
        self.pattern_topic = config.mqtt.topic if config else "empresas"
        
        # MQTT Publisher para envío a dispositivos
        self.mqtt_publisher = None
        if enable_mqtt_publisher and config:
            try:
                publisher_config = MQTTConfig(
                    broker=config.mqtt.broker,
                    port=config.mqtt.port,
                    topic=config.mqtt.topic,
                    username=config.mqtt.username,
                    password=config.mqtt.password,
                    client_id=f"{config.mqtt.client_id}_empresa_handler",
                    keep_alive=config.mqtt.keep_alive
                )
                self.mqtt_publisher = MQTTPublisherLite(publisher_config)
                if self.mqtt_publisher.connect():
                    self.logger.info("✅ MQTT Publisher conectado desde Empresa Handler")
                else:
                    self.logger.warning("⚠️ Error conectando MQTT Publisher en Empresa Handler")
                    self.mqtt_publisher = None
            except Exception as e:
                self.logger.error(f"❌ Error iniciando MQTT Publisher en Empresa Handler: {e}")
                self.mqtt_publisher = None
        
        self.logger.info("🏢 Empresa Alert Handler iniciado")
        self.logger.info("📱 WhatsApp Service + MQTT Publisher habilitados")

    def process_empresa_alert(self, message_data: Dict) -> bool:
        """Procesar mensaje de alerta de empresa (activación o desactivación)"""
        try:
            message_type = message_data.get("type", "")
            
            if message_type == "alert_deactivated_by_empresa":
                return self.process_empresa_deactivation(message_data)
            elif message_type == "alert_created_by_empresa":
                return self.process_empresa_activation(message_data)
            else:
                self.logger.warning(f"⚠️ Tipo de mensaje de empresa no reconocido: {message_type}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Error procesando mensaje de empresa: {e}")
            self.error_count += 1
            return False
    
    def process_empresa_activation(self, message_data: Dict) -> bool:
        """Procesar activación de alerta por empresa (similar a MQTT handler)"""
        try:
            self.logger.info("🏢 Procesando activación de alerta por empresa")
            
            # Validar estructura del mensaje
            if not self._validate_empresa_activation_message(message_data):
                self.logger.error("❌ Estructura de mensaje inválida para activación de empresa")
                return False
            
            # Extraer datos principales de la estructura del backend
            alert_data = message_data.get("alert", {})
            alert_managers_raw = message_data.get("alert_managers", []) or []
            alert_id = alert_data.get("_id", "N/A")
            alert_name = alert_data.get("nombre_alerta") or alert_data.get("tipo_alerta", "Alerta")
            empresa_nombre = alert_data.get("empresa_nombre", "La Empresa")
            sede = alert_data.get("sede", "")
            usuarios = alert_data.get("numeros_telefonicos", [])
            usuarios_normalizados = self._normalize_usuarios_list(usuarios)
            managers_normalizados = self._normalize_usuarios_list(alert_managers_raw)
            topics_hardware = alert_data.get("topics_otros_hardware", [])
            descripcion = alert_data.get("descripcion", "")

            self.logger.info(f"📋 Datos de activación extraídos:")
            self.logger.info(f"   🚨 Alert ID: {alert_id}")
            self.logger.info(f"   📛 Tipo: {alert_name}")
            self.logger.info(f"   📝 Descripción: {descripcion}")
            self.logger.info(f"   👥 Usuarios sede: {len(usuarios_normalizados)}")
            self.logger.info(f"   🛡️ Managers empresa: {len(managers_normalizados)}")
            self.logger.info(f"   📡 Hardware: {len(topics_hardware)}")
            self.logger.info(f"   🏢 Empresa: {empresa_nombre}")
            self.logger.info(f"   🏛️ Sede: {sede}")

            # 1. Fanout de notificaciones vía modelo OOP (plantilla / mapa / patch managers)
            template_success = True
            manager_notify_success = True
            activacion_alerta = alert_data.get("activacion_alerta", {})

            # Resolver teléfono del creador. activacion_alerta solo tiene id+nombre,
            # buscamos en numeros_telefonicos por usuario_id para encontrar su teléfono.
            telefono_creador = resolve_creator_phone(activacion_alerta, usuarios_normalizados)

            # Union sede + managers sin duplicados
            all_by_phone: Dict[str, Dict] = {}
            for u in usuarios_normalizados:
                num = u.get("numero")
                if num:
                    all_by_phone[num] = u
            for m in managers_normalizados:
                num = m.get("numero")
                if num and num not in all_by_phone:
                    all_by_phone[num] = m

            alert_id_str = str(alert_data.get("_id", ""))

            # Construir objetos de usuario con OOP
            alert_users = [
                make_alert_user(u, telefono_creador, alert_id_str, self.whatsapp_service)
                for u in all_by_phone.values()
                if u.get("numero")
            ]

            # Fanout de mensajes polimórfico
            for user in alert_users:
                for msg_type in BROADCAST_MESSAGE_TYPES:
                    if msg_type.can_receive(user):
                        msg_type.send(user, self.whatsapp_service, alert_data, self.logger)
                # Ciclo de vida del cache
                user.on_alert_broadcast(self.whatsapp_service)

            # Backend log de plantillas (no creators) — preserva trazabilidad is_template
            template_logged = [
                {"phone": u.phone, "template_name": "crear_alerta"}
                for u in alert_users if not u.is_creator and u.phone
            ]
            if template_logged:
                self._log_template_sends(
                    alert_id=alert_id_str,
                    template_name="crear_alerta",
                    recipients=template_logged,
                    summary_body=f"Plantilla crear_alerta enviada (alerta {alert_name})"
                )

            # 2. Crear cache masivo para usuarios de la sede.
            # Incluye managers que también son miembros de sede (dual-role o managers
            # propios de esta sede): su foco ES esta alerta. Solo managers PURE cross-sede
            # quedan fuera, manejados por ManagerLastNotifiedPatch en el fanout.
            cache_success = True
            if usuarios_normalizados:
                cache_success = self._create_bulk_cache_empresa(
                    alert_data=alert_data,
                    usuarios=usuarios_normalizados
                )
            else:
                self.logger.info("ℹ️ No hay usuarios de sede para crear cache")

            # 2b. Metadata managers ya manejada por ManagerLastNotifiedPatch en el fanout
            # (crea entry si no existe, patch si existe). No hay block separado redundante.

            # 3. Enviar comandos MQTT a dispositivos hardware
            mqtt_success = True
            if topics_hardware:
                mqtt_success = self._send_mqtt_activation_commands(
                    topics_hardware=topics_hardware,
                    alert_data=alert_data
                )
            else:
                self.logger.info("ℹ️ No hay hardware para activar por MQTT")

            # Actualizar estadísticas
            if template_success and cache_success and mqtt_success and manager_notify_success:
                self.processed_count += 1
                self.logger.info("✅ Activación por empresa procesada exitosamente")
                return True
            else:
                self.error_count += 1
                self.logger.warning("⚠️ Activación parcialmente exitosa")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Error procesando activación por empresa: {e}")
            self.error_count += 1
            return False
    
    def process_empresa_deactivation(self, message_data: Dict) -> bool:
        """Procesar desactivación de alerta por empresa"""
        try:
            self.logger.info("🏢 Procesando desactivación de alerta por empresa")
            
            # Validar estructura del mensaje
            if not self._validate_empresa_message(message_data):
                self.logger.error("❌ Estructura de mensaje inválida para empresa")
                return False
            
            # Extraer datos principales de la nueva estructura
            alert = message_data.get("alert", {})
            timestamp = message_data.get("timestamp", "")
            
            alert_id = alert.get("id", "N/A")
            alert_name = alert.get("nombre", "Alerta")
            empresa = alert.get("empresa", "La Empresa")
            sede = alert.get("sede", "")
            prioridad = alert.get("prioridad", "media")
            usuarios = alert.get("usuarios", [])
            hardware_vinculado = alert.get("hardware_vinculado", [])
            desactivado_por = alert.get("desactivado_por", {})
            
            self.logger.info(f"📋 Datos extraídos:")
            self.logger.info(f"   🚨 Alert ID: {alert_id}")
            self.logger.info(f"   📛 Nombre: {alert_name}")
            self.logger.info(f"   👥 Usuarios: {len(usuarios)}")
            self.logger.info(f"   📡 Hardware: {len(hardware_vinculado)}")
            self.logger.info(f"   🏢 Empresa: {empresa}")
            self.logger.info(f"   🏛️ Sede: {sede}")

            # 1. Limpiar caché de usuarios afectados (usa alert_id + lista como fuentes)
            cache_success = self._clean_users_cache_after_deactivation(usuarios, alert_id=str(alert_id))

            # 1b. Limpiar foco de managers que tenían esta alerta activa.
            # Sin esto, managers siguen mandando mensajes a una alarma cerrada (info_alert quedaba colgado).
            self._clean_focused_managers_after_deactivation(
                alert_id=str(alert_id),
                nombre_alerta=alert_name,
                sede=sede
            )

            # 2. Enviar notificación WhatsApp a usuarios
            whatsapp_success = self._send_empresa_deactivation_notification(
                usuarios=usuarios,
                alert_info={
                    "id": alert_id,
                    "nombre": alert_name,
                    "empresa": empresa,
                    "sede": sede,
                    "timestamp": timestamp,
                    "desactivado_por": desactivado_por
                }
            )
            
            # 3. Enviar comandos MQTT a dispositivos hardware
            mqtt_success = self._send_mqtt_deactivation_commands(
                hardware_list=hardware_vinculado,
                prioridad=prioridad
            )
            
            # Actualizar estadísticas
            if cache_success and whatsapp_success and mqtt_success:
                self.processed_count += 1
                self.logger.info("✅ Desactivación por empresa procesada exitosamente")
                return True
            else:
                self.error_count += 1
                self.logger.warning("⚠️ Desactivación parcialmente exitosa")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Error procesando desactivación por empresa: {e}")
            self.error_count += 1
            return False

    def _validate_empresa_activation_message(self, message_data: Dict) -> bool:
        """Validar estructura del mensaje de activación de empresa"""
        # Verificar campos principales
        if "type" not in message_data or message_data["type"] != "alert_created_by_empresa":
            self.logger.error("❌ Tipo de mensaje incorrecto para activación")
            return False
            
        if "alert" not in message_data:
            self.logger.error("❌ Campo 'alert' faltante en mensaje de activación")
            return False
        
        alert = message_data["alert"]
        
        # Validar campos requeridos en alert para activación
        required_alert_fields = ["_id", "tipo_alerta"]
        for field in required_alert_fields:
            if field not in alert:
                self.logger.error(f"❌ Campo requerido faltante en alert para activación: {field}")
                return False
        
        return True
    
    def _validate_empresa_message(self, message_data: Dict) -> bool:
        """Validar estructura del mensaje de empresa"""
        # Verificar campos principales
        if "type" not in message_data or message_data["type"] != "alert_deactivated_by_empresa":
            self.logger.error("❌ Tipo de mensaje incorrecto")
            return False
            
        if "alert" not in message_data:
            self.logger.error("❌ Campo 'alert' faltante")
            return False
        
        alert = message_data["alert"]
        
        # Validar campos requeridos en alert
        required_alert_fields = ["id", "usuarios", "hardware_vinculado"]
        for field in required_alert_fields:
            if field not in alert:
                self.logger.error(f"❌ Campo requerido faltante en alert: {field}")
                return False
        
        # Validar que usuarios sea una lista
        if not isinstance(alert["usuarios"], list):
            self.logger.error("❌ alert.usuarios debe ser una lista")
            return False
            
        # Validar que hardware_vinculado sea una lista
        if not isinstance(alert["hardware_vinculado"], list):
            self.logger.error("❌ alert.hardware_vinculado debe ser una lista")
            return False
        
        return True

    def _clean_users_cache_after_deactivation(self, usuarios: List[Dict], alert_id: str = "") -> bool:
        """Limpiar caché de TODOS los teléfonos con foco en esta alerta.

        Fuente principal: find_numbers_by_alert(alert_id) — directo del backend WhatsApp,
        cubre regulares + managers + dual-role + cualquiera con info_alert.alert_id == este.
        Fallback: usuarios pasados (compat / casos donde alert_id no esté).
        """
        if not self.whatsapp_service:
            self.logger.warning("⚠️ WhatsApp service no disponible para limpieza de caché")
            return False

        try:
            data_to_delete = {
                "info_alert": "__DELETE__",
                "alert_active": "__DELETE__",
                "disponible": "__DELETE__",
                "embarcado": "__DELETE__",
            }

            phones_set: set = set()

            # Fuente 1: query directo por alert_id
            if alert_id:
                try:
                    focused = self.whatsapp_service.find_numbers_by_alert(
                        alert_id=str(alert_id), manager_only=False
                    ) or []
                    for entry in focused:
                        phone = entry.get("phone") or entry.get("numero") or entry.get("telefono")
                        if phone:
                            phones_set.add(str(phone).strip().lstrip("+"))
                except Exception as ex:
                    self.logger.warning(f"⚠️ find_numbers_by_alert falló para {alert_id}: {ex}")

            # Fuente 2: lista pasada (defensive)
            for usuario in usuarios or []:
                if not isinstance(usuario, dict):
                    continue
                telefono = usuario.get("telefono") or usuario.get("numero") or usuario.get("phone")
                if not telefono:
                    continue
                phones_set.add(str(telefono).strip().lstrip("+"))

            if not phones_set:
                self.logger.warning(f"⚠️ Sin teléfonos para limpiar cache de alerta {alert_id}")
                return False

            # Individual update por cada phone: bulk_update no honra __DELETE__
            # (lo setea como string literal, conversación queda "activa" en el cache).
            self.logger.info(f"🧹 Limpiando cache de alerta {alert_id} en {len(phones_set)} teléfonos")
            for phone in phones_set:
                try:
                    self.whatsapp_service.update_number_cache(phone=phone, data=data_to_delete)
                except Exception as ex:
                    self.logger.warning(f"⚠️ No se pudo limpiar cache {phone}: {ex}")
            return True

        except Exception as e:
            self.logger.error(f"❌ Error limpiando caché de usuarios: {e}")
            return False

    def _send_empresa_deactivation_notification(self, usuarios: List[Dict], alert_info: Dict) -> bool:
        """Enviar notificación de desactivación por empresa via WhatsApp"""
        if not self.whatsapp_service:
            self.logger.warning("⚠️ WhatsApp service no disponible")
            return False
            
        try:
            alert_id = alert_info.get("id", "N/A")
            alert_name = alert_info.get("nombre", "Alerta")
            empresa = alert_info.get("empresa", "La Empresa")
            sede = alert_info.get("sede", "")
            timestamp = alert_info.get("timestamp", "")
            desactivado_por = alert_info.get("desactivado_por", {})
            
            # Formatear fecha si existe
            fecha_formato = "Ahora"
            if timestamp:
                try:
                    from datetime import datetime
                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    fecha_formato = dt.strftime("%d/%m/%Y %H:%M")
                except:
                    fecha_formato = timestamp
            
            recipients = []
            
            for usuario in usuarios:
                nombre = usuario.get("nombre", "Usuario")
                telefono = usuario.get("telefono", "")
                
                # Limpiar formato del teléfono si es necesario
                if telefono.startswith("+"):
                    telefono = telefono[1:]  # Remover el +
                
                if not telefono:
                    self.logger.warning(f"⚠️ Usuario {nombre} no tiene teléfono válido")
                    continue
                    
                # Mensaje personalizado para cada usuario
                notification_message = f"¡Hola {nombre.split()[0].upper()}!\n\n"
                notification_message += f"ALERTA DESACTIVADA POR {empresa.upper()}\n\n"
                notification_message += f"Detalles:\n"
                notification_message += f"Alerta: {alert_name}\n"
                
                if sede:
                    notification_message += f"Sede: {sede}\n"
                    
                notification_message += f"Momento: {fecha_formato}\n"
                notification_message += f"Desactivada por: {desactivado_por.get('nombre', empresa)}\n\n"
                notification_message += f"El sistema ha vuelto a estado normal\n"
                notification_message += f"SISTEMA RESCUE"
                
                recipients.append({
                    "phone": telefono,
                    "message": notification_message
                })
            
            if not recipients:
                self.logger.warning("⚠️ No hay destinatarios válidos para WhatsApp")
                return False
            
            # Enviar usando el cliente de WhatsApp para envío masivo
            response = self.whatsapp_service.send_bulk_individual(
                recipients=recipients,
                use_queue=True
            )
            
            if response:
                self.logger.info(f"✅ Notificación de empresa enviada a {len(recipients)} usuarios")
                return True
            else:
                self.logger.error("❌ Error enviando notificación masiva de empresa")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Error enviando notificación WhatsApp de empresa: {e}")
            return False

    def _send_mqtt_deactivation_commands(self, hardware_list: List[Dict], prioridad: str) -> bool:
        """Enviar comandos de desactivación MQTT a dispositivos hardware"""
        if not self.mqtt_publisher:
            self.logger.warning("⚠️ MQTT Publisher no disponible")
            return False
            
        if not hardware_list:
            self.logger.info("ℹ️ No hay hardware para procesar")
            return True
            
        try:
            success_count = 0
            
            self.logger.info(f"🔄 Enviando comandos de desactivación MQTT a {len(hardware_list)} dispositivos")
            
            for hardware in hardware_list:
                topic = hardware.get("topic", "")
                hardware_name = hardware.get("nombre", "Hardware desconocido")
                hardware_id = hardware.get("id_origen", "N/A")
                
                if not topic:
                    self.logger.warning(f"⚠️ Hardware {hardware_name} no tiene topic definido")
                    continue
                
                # Usar el topic directamente (ya viene con la estructura completa)
                # Solo agregar el pattern_topic si no lo tiene
                if not topic.startswith(self.pattern_topic):
                    full_topic = f"{self.pattern_topic}/{topic}"
                else:
                    full_topic = topic
                
                # Crear mensaje de desactivación según el tipo de dispositivo
                deactivation_message = self._create_deactivation_message(
                    topic=topic, 
                    prioridad=prioridad
                )
                
                # No agregar información adicional - solo el mensaje básico
                
                # Enviar mensaje MQTT
                success = self._send_mqtt_message(message_data=deactivation_message, topic=full_topic)
                
                if success:
                    success_count += 1
                    self.logger.info(f"✅ Hardware desactivado: {hardware_name} ({hardware_id})")
                else:
                    self.logger.error(f"❌ Error desactivando hardware: {hardware_name} - Topic: {full_topic}")
            
            self.logger.info(f"📊 MQTT: {success_count}/{len(hardware_list)} dispositivos desactivados")
            return success_count > 0
            
        except Exception as e:
            self.logger.error(f"❌ Error enviando comandos MQTT de empresa: {e}")
            return False

    def _create_deactivation_message(self, topic: str, prioridad: str) -> Dict:
        """Crear mensaje de desactivación específico según el tipo de dispositivo"""
        if "SEMAFORO" in topic:
            # Para semáforos: solo tipo_alarma NORMAL
            return {
                "tipo_alarma": "NORMAL"
            }
        elif "PANTALLA" in topic:
            # Para televisores: tipo_alarma NORMAL + prioridad
            return {
                "tipo_alarma": "NORMAL",
                "prioridad": prioridad.upper()
            }
        else:
            # Para dispositivos genéricos: solo tipo_alarma NORMAL
            return {
                "tipo_alarma": "NORMAL"
            }

    def _send_empresa_activation_notification(self, usuarios: List[Dict], alert_data: Dict) -> bool:
        """Enviar notificación de activación por empresa via WhatsApp (similar a MQTT handler)"""
        if not self.whatsapp_service:
            self.logger.warning("⚠️ WhatsApp service no disponible")
            return False
            
        try:
            # Extraer datos de la alerta para el mensaje
            alert_name = alert_data.get("nombre_alerta", alert_data.get("tipo_alerta", "Alerta"))
            empresa_nombre = alert_data.get("empresa_nombre", "La Empresa")
            descripcion = alert_data.get("descripcion", "")
            image_alert = alert_data.get("image_alert", "")
            activacion_alerta = alert_data.get("activacion_alerta", {})
            creador_nombre = activacion_alerta.get("nombre", empresa_nombre)
            
            self.logger.info(f"📱 Enviando notificación de activación:")
            self.logger.info(f"   🆔 ID de alerta: {alert_data.get('_id', 'N/A')}")
            self.logger.info(f"   📡 Topics generados: {len(alert_data.get('topics_otros_hardware', []))} topics")
            
            recipients = []
            footer = f"Creada por {creador_nombre}\nEquipo RESCUE"
            
            for usuario in usuarios:
                nombre = usuario.get("nombre", "Usuario")
                telefono = usuario.get("numero", "")  # ✅ CAMBIO: usar 'numero' en lugar de 'telefono'
                
                # Limpiar formato del teléfono si es necesario
                if telefono.startswith("+"):
                    telefono = telefono[1:]  # Remover el +
                
                if not telefono:
                    self.logger.warning(f"⚠️ Usuario {nombre} no tiene teléfono válido")
                    continue
                    
                # Mensaje personalizado para cada usuario
                body_text = f"¡Hola {nombre.split()[0].upper()}!.\nAlerta de {alert_name} en {empresa_nombre}."
                if descripcion:
                    body_text += f"\n{descripcion}"
                
                recipients.append({
                    "phone": telefono,
                    "body_text": body_text
                })
            
            if not recipients:
                self.logger.warning("⚠️ No hay destinatarios válidos para WhatsApp")
                return False
            
            # Botones de acción
            buttons = [
                {
                    "id": "Activar_User",
                    "title": "Estoy disponible"
                }
            ]
            
            # Enviar mensaje usando bulk_button_message
            if image_alert:
                # Con imagen
                response = self.whatsapp_service.send_bulk_button_message(
                    header_type="image",
                    header_content=image_alert,
                    buttons=buttons,
                    footer_text=footer,
                    recipients=recipients,
                    use_queue=True
                )
            else:
                # Sin imagen, solo texto
                response = self.whatsapp_service.send_bulk_button_message(
                    header_type="text",
                    header_content=f"🚨 ALERTA {alert_name.upper()}",
                    buttons=buttons,
                    footer_text=footer,
                    recipients=recipients,
                    use_queue=True
                )
            
            if response:
                self.logger.info(f"✅ Notificación de activación enviada a {len(recipients)} usuarios")
                return True
            else:
                self.logger.error("❌ Error enviando notificación de activación masiva")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Error enviando notificación de activación: {e}")
            return False
    
    def _send_location_message_empresa(self, usuarios: List[Dict], location: Dict) -> bool:
        """Enviar ubicación por WhatsApp usando CTA 'Abrir en Maps'"""

        if not self.whatsapp_service:
            self.logger.warning("⚠️ WhatsApp service no disponible")
            return False

        try:
            url_maps = location.get("url_maps") if isinstance(location, dict) else None

            if not url_maps:
                self.logger.warning("⚠️ No hay URL de ubicación disponible")
                return False

            success = self.whatsapp_service.send_bulk_location_button_message(
                recipients=usuarios,
                url_maps=url_maps,
                footer_text="Equipo RESCUE",
                use_queue=True
            )

            if success:
                self.logger.info(f"✅ Mensaje de ubicación enviado a {len(usuarios)} usuarios")
                return True

            self.logger.error("❌ Error enviando mensaje de ubicación con CTA")
            return False

        except Exception as e:
            self.logger.error(f"❌ Error enviando mensaje de ubicación: {e}")
            return False

    def _extract_phone_number(self, data: Dict[str, Any]) -> str:
        """Obtener y normalizar el número telefónico del payload"""
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
        """Normalizar la lista de usuarios asegurando números válidos"""
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
    
    def _create_bulk_cache_empresa(self, alert_data: Dict, usuarios: List[Dict]) -> bool:
        """Crear cache masivo para todos los usuarios (similar a MQTT handler)"""
        if not self.whatsapp_service:
            self.logger.warning("⚠️ WhatsApp service no disponible para cache")
            return False
            
        try:
            import time
            
            alert_id = alert_data.get("_id", "")
            empresa_nombre = alert_data.get("empresa_nombre", "")
            sede_alerta = alert_data.get("sede", "")
            empresa_id = alert_data.get("empresa_id")

            if not empresa_id:
                empresa_data = alert_data.get("empresa")
                if isinstance(empresa_data, dict):
                    empresa_id = empresa_data.get("id") or empresa_data.get("_id")

            success_count = 0

            for usuario in usuarios:
                telefono = usuario.get("numero", "")
                nombre = usuario.get("nombre", "")
                user_id = usuario.get("usuario_id", "")

                if telefono.startswith("+"):
                    telefono = telefono[1:]

                if not telefono:
                    continue

                cache_data = {
                    "alert_active": True,
                    "disponible": usuario.get("disponible", False),
                    "embarcado": usuario.get("embarcado", False),
                    "empresa": empresa_nombre,
                    "sede": sede_alerta,
                    "id": user_id,
                    "info_alert": {
                        "alert_id": alert_id
                    }
                }

                role_info = usuario.get("rol") if isinstance(usuario, dict) else None
                if isinstance(role_info, dict):
                    cache_data["rol"] = {
                        "nombre": role_info.get("nombre") or role_info.get("name", ""),
                        "is_creator": bool(role_info.get("is_creator")),
                        "is_alert_manager": bool(role_info.get("is_alert_manager"))
                    }

                if empresa_id:
                    cache_data["empresa_id"] = empresa_id
                
                # Agregar al cache
                response = self.whatsapp_service.add_number_to_cache(
                    phone=telefono,
                    name=nombre,
                    data=cache_data,
                    empresa_id=empresa_id
                )
                
                if response:
                    success_count += 1
                    self.logger.debug(f"📝 Cache creado para usuario {telefono}")
                else:
                    self.logger.warning(f"⚠️ Error creando cache para usuario {telefono}")
            
            self.logger.info(f"✅ Cache masivo creado para {success_count}/{len(usuarios)} usuarios")
            return success_count > 0
            
        except Exception as e:
            self.logger.error(f"❌ Error creando cache masivo: {e}")
            return False
    
    def _send_mqtt_activation_commands(self, topics_hardware: List[str], alert_data: Dict) -> bool:
        """Enviar comandos de activación MQTT a dispositivos hardware (similar a MQTT handler)"""
        if not self.mqtt_publisher:
            self.logger.warning("⚠️ MQTT Publisher no disponible")
            return False
            
        if not topics_hardware:
            self.logger.info("ℹ️ No hay hardware para activar")
            return True
            
        try:
            success_count = 0
            
            self.logger.info(f"🔄 Enviando comandos de activación MQTT a {len(topics_hardware)} dispositivos")
            
            for topic in topics_hardware:
                # Construir topic completo igual que en MQTT handler
                full_topic = f"{self.pattern_topic}/{topic}"
                
                # Crear mensaje específico según el tipo de hardware
                activation_message = self._create_activation_message(topic=topic, alert_data=alert_data)
                
                # Enviar mensaje MQTT
                success = self._send_mqtt_message(message_data=activation_message, topic=full_topic)
                
                if success:
                    success_count += 1
                    hardware_name = topic.split("/")[-1] if "/" in topic else topic
                    self.logger.info(f"✅ Hardware activado: {hardware_name}")
                else:
                    self.logger.error(f"❌ Error activando hardware: {topic}")
            
            self.logger.info(f"📊 MQTT: {success_count}/{len(topics_hardware)} dispositivos activados")
            return success_count > 0
            
        except Exception as e:
            self.logger.error(f"❌ Error enviando comandos de activación MQTT: {e}")
            return False
    
    def _create_activation_message(self, topic: str, alert_data: Dict) -> Dict:
        """Crear mensaje de activación específico según el tipo de dispositivo (similar a MQTT handler)"""
        # Extraer datos de la alerta para el mensaje
        tipo_alerta = alert_data.get("tipo_alerta", "")
        prioridad = alert_data.get("prioridad", "media")

        data_payload = alert_data.get("data", {}) or {}
        alarm_color = (
            data_payload.get("tipo_alarma")
            or alert_data.get("nombre_alerta")
            or ""
        )
        
        if "SEMAFORO" in topic:
            # Para semáforos: solo tipo_alarma (color)
            return {
                "tipo_alarma": alarm_color,
            }
        elif "PANTALLA" in topic:
            if str(alert_data.get("tipo_alarma") or tipo_alerta).upper() == "NORMAL":
                return {
                    "tipo_alarma": "NORMAL",
                    "prioridad": prioridad.upper()
                }
            try:
                return normalize_alert_to_tv(alert_data)
            except AlertNormalizationError as exc:
                self.logger.error(f"❌ Error normalizando alerta para PANTALLA: {exc}")
                return {"alert": alert_data}
            except Exception as exc:
                self.logger.error(f"❌ Error inesperado normalizando alerta para PANTALLA: {exc}")
                return {"alert": alert_data}
        else:
            # Para dispositivos genéricos
            return {
                "tipo_alarma": alarm_color,
                "action": "activate",
                "message": f"Alerta de {tipo_alerta}",
            }
    
    def _send_mqtt_message(self, topic: str, message_data: Dict, qos: int = 0) -> bool:
        """Enviar mensaje MQTT a un topic específico"""
        try:
            if not self.mqtt_publisher:
                return False
                
            success = self.mqtt_publisher.publish_json(topic, message_data, qos)
            
            if success:
                self.logger.debug(f"✅ Mensaje MQTT enviado a topic: {topic}")
                return True
            else:
                self.logger.error(f"❌ Error enviando mensaje MQTT a topic: {topic}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Error enviando mensaje MQTT: {e}")
            return False

    def _resolve_tv_topic_parts(self, alert_data: Dict) -> tuple[str, str, str]:
        empresa = (
            alert_data.get("empresa")
            or alert_data.get("empresa_nombre")
            or "desconocida"
        )
        sede = alert_data.get("sede") or "desconocida"
        pantalla = (
            alert_data.get("pantalla")
            or alert_data.get("nombre_pantalla")
            or "principal"
        )
        return str(empresa), str(sede), str(pantalla)

    def _publish_tv_alert(self, alert_data: Dict) -> None:
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

        empresa, sede, pantalla = self._resolve_tv_topic_parts(alert_data)
        topic = build_tv_topic(empresa=empresa, sede=sede, pantalla=pantalla)
        self._send_mqtt_message(topic=topic, message_data=normalized)

    def get_statistics(self) -> Dict[str, Any]:
        """Obtener estadísticas del handler de empresa"""
        return {
            "processed_count": self.processed_count,
            "error_count": self.error_count,
            "error_rate": round(self.error_count / max(self.processed_count + self.error_count, 1) * 100, 2),
            "mqtt_publisher_available": self.mqtt_publisher is not None,
            "whatsapp_service_available": self.whatsapp_service is not None
        }

    def stop(self):
        """Detener el handler y cerrar conexiones"""
        try:
            if self.mqtt_publisher:
                self.mqtt_publisher.disconnect()
                self.logger.info("🔌 MQTT Publisher desconectado en Empresa Handler")

            self.logger.info("🛑 Empresa Alert Handler detenido")

        except Exception as e:
            self.logger.error(f"❌ Error deteniendo Empresa Handler: {e}")

    def _clean_focused_managers_after_deactivation(self, alert_id: str, nombre_alerta: str, sede: str) -> None:
        """Limpia info_alert/alert_active del cache de managers que tenían foco en esta alerta.
        Sin esto, sus mensajes posteriores quedaban asociados a la alerta cerrada."""
        if not self.whatsapp_service or not alert_id:
            return
        try:
            focused = self.whatsapp_service.find_numbers_by_alert(alert_id=str(alert_id), manager_only=True) or []
            if not focused:
                return
            patch_data = {
                "info_alert": "__DELETE__",
                "alert_active": "__DELETE__",
                "disponible": "__DELETE__",
                "embarcado": "__DELETE__"
            }
            notify_text = f"La alerta '{nombre_alerta}' de sede {sede or 'desconocida'} fue desactivada. Ya no estás en su conversación."
            for m in focused:
                manager_phone = m.get("phone")
                if not manager_phone:
                    continue
                try:
                    self.whatsapp_service.update_number_cache(
                        phone=manager_phone,
                        data=patch_data,
                        empresa_id=(m.get("data") or {}).get("empresa_id")
                    )
                    self.whatsapp_service.send_individual_message(phone=manager_phone, message=notify_text)
                except Exception as ex:
                    self.logger.error(f"❌ Error limpiando foco manager {manager_phone}: {ex}")
        except Exception as ex:
            self.logger.error(f"Error en _clean_focused_managers_after_deactivation: {ex}")

    def _log_template_sends(self, alert_id, template_name: str, recipients: List[Dict], summary_body: str) -> None:
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
