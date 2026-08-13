"""
Manejador de mensajes WebSocket puro - SIN MQTT
Solo procesa mensajes entrantes de WhatsApp desde webhooks
"""
import json
import logging
import asyncio
import time
from asyncio import Queue
from typing import Dict, Any, Optional, Set, List
from utils.redis_queue_manager import RedisQueueManager
from utils.alert_normalizer import (
    AlertNormalizationError,
    build_tv_topic,
    normalize_alert_to_tv,
)
from clients.mqtt_publisher_lite import MQTTPublisherLite
from config.settings import MQTTConfig
from handlers.empresa_alert_handler import EmpresaAlertHandler
from handlers.alert_action_templates import action_message
from models.alert_user import make_whatsapp_user
from datetime import datetime, timedelta


class WebSocketMessageHandler:
    """Manejador de mensajes WebSocket puro - SIN dependencias de MQTT"""
    
    def __init__(self, backend_client, whatsapp_service=None, config=None, enable_mqtt_publisher=False):
        self.backend_client = backend_client
        self.whatsapp_service = whatsapp_service
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Estadísticas solo para WhatsApp
        self.whatsapp_processed_count = 0
        self.whatsapp_error_count = 0
        
        # Usar settings en lugar de .env directo (igual que en MQTT handler)
        self.pattern_topic = config.mqtt.topic if config else "empresas"
        
        # MQTT Publisher OPCIONAL (solo para envío desde WhatsApp)
        self.mqtt_publisher = None
        if enable_mqtt_publisher and config:
            try:
                publisher_config = MQTTConfig(
                    broker=config.mqtt.broker,
                    port=config.mqtt.port,
                    topic=config.mqtt.topic,
                    username=config.mqtt.username,
                    password=config.mqtt.password,
                    client_id=f"{config.mqtt.client_id}_websocket_publisher",
                    keep_alive=config.mqtt.keep_alive
                )
                self.mqtt_publisher = MQTTPublisherLite(publisher_config)
                if self.mqtt_publisher.connect():
                    self.logger.info("✅ MQTT Publisher conectado desde WebSocket handler")
                else:
                    # NO descartar el publisher: paho reconecta solo y
                    # ensure_connected() reintenta antes de cada publicación.
                    self.logger.critical(
                        "🚨 MQTT Publisher (WebSocket handler) sin conexión inicial a %s:%s — "
                        "el hardware no recibirá comandos hasta que reconecte",
                        config.mqtt.broker, config.mqtt.port
                    )
            except Exception as e:
                self.logger.error(f"❌ Error iniciando MQTT Publisher: {e}")
                self.mqtt_publisher = None
        
        # Handler específico para alertas desactivadas por empresa
        self.empresa_handler = None
        if enable_mqtt_publisher and config:
            self.empresa_handler = EmpresaAlertHandler(
                whatsapp_service=whatsapp_service,
                config=config,
                enable_mqtt_publisher=enable_mqtt_publisher,
                backend_client=self.backend_client
            )
        
        # Sistema de colas Redis para mensajes de WhatsApp ENTRANTES
        self.redis_queue = None
        if whatsapp_service:
            try:
                redis_config = config.redis if config else None
                self.redis_queue = RedisQueueManager(redis_config)
                self.redis_queue.start_workers(self._process_single_whatsapp_message_sync)
                self.logger.info("✅ Sistema de colas Redis iniciado para WhatsApp")
            except Exception as e:
                self.logger.error(f"❌ Error iniciando Redis, usando cola en memoria: {e}")
                self.redis_queue = None
        
        # Cola en memoria como fallback
        self.whatsapp_queue = Queue(maxsize=1000)
        self.is_processing = False
        self._queue_task = None
        
        self.logger.info("📱 WebSocket Message Handler - SOLO procesamiento WhatsApp")
        self.logger.info("❌ SIN procesamiento de mensajes MQTT")

    def _get_first_name(self, name: str) -> str:
        """Usar solo el primer nombre para evitar headers muy largos."""
        if not name:
            return ""
        parts = str(name).strip().split()
        return parts[0] if parts else ""

    async def queue_whatsapp_message(self, message: str) -> bool:
        """Agregar mensaje de WhatsApp a la cola para procesamiento"""
        #print(f"🔍 DEBUG: Mensaje recibido en WebSocket: {message}")
        try:
            # Primero verificar si es un mensaje de empresa
            try:
                json_message = json.loads(message)
                message_type = json_message.get("type")
                
                if message_type == "alert_deactivated_by_empresa":
                    self.logger.info("🏢 Detectado mensaje de desactivación por empresa")
                    return self._handle_empresa_message_sync(json_message)
                elif message_type == "create_empresa_alert":
                    self.logger.info("🏢 Detectado mensaje para crear alerta de empresa")
                    return self._handle_create_empresa_alert_sync(json_message)
            except (json.JSONDecodeError, KeyError):
                # Si no es JSON válido o no tiene type, procesar como mensaje normal
                pass
            
            # Procesar como mensaje normal de WhatsApp
            # Usar Redis si está disponible
            if self.redis_queue and self.redis_queue.is_healthy():
                return self.redis_queue.add_message(message)
            else:
                # Fallback a cola en memoria
                await self.whatsapp_queue.put(message)
                
                # Iniciar el procesador de cola si no está corriendo
                if not self.is_processing:
                    self._queue_task = asyncio.create_task(self._process_whatsapp_queue())
                return True
        except asyncio.QueueFull:
            self.logger.warning(f"⚠️ Cola de WhatsApp llena, descartando mensaje")
            self.whatsapp_error_count += 1
            return False
        except Exception as e:
            self.logger.error(f"❌ Error agregando mensaje a cola: {e}")
            self.whatsapp_error_count += 1
            return False

    async def _process_whatsapp_queue(self):
        """Procesar mensajes de WhatsApp desde la cola de forma secuencial"""
        self.is_processing = True
        self.logger.info("🔄 Iniciando procesador de cola de WhatsApp")
        
        try:
            while True:
                try:
                    # Obtener el siguiente mensaje de la cola
                    message = await self.whatsapp_queue.get()
                    
                    # Procesar el mensaje
                    self._process_single_whatsapp_message_sync(message)
                    
                    # Marcar la tarea como completada
                    self.whatsapp_queue.task_done()
                    
                except asyncio.CancelledError:
                    self.logger.info("🛑 Procesador de cola cancelado")
                    break
                except Exception as e:
                    self.logger.error(f"❌ Error procesando mensaje de cola: {e}")
                    self.whatsapp_error_count += 1
                    
        finally:
            self.is_processing = False
            self.logger.info("🔄 Procesador de cola de WhatsApp detenido")

    def _send_permission_denied_message(self, phone: str, user: str, action: str) -> None:
        """Notificar al usuario que no tiene privilegios para la acción solicitada"""
        if not self.whatsapp_service:
            return

        friendly_user = user or "Usuario"
        message = (
            f"Lo siento {friendly_user}, no tienes permisos para {action}."
            "\nContacta al administrador si necesitas acceso."
        )
        self.whatsapp_service.send_individual_message(phone=phone, message=message)

    def _process_single_whatsapp_message_sync(self, message: str) -> bool:
        """Procesar un solo mensaje de WhatsApp (versión síncrona para Redis)"""
        try:
            # Parsear JSON
            json_message = json.loads(message)
            
            # PRIMERO: Verificar si es un mensaje de empresa
            message_type = json_message.get("type")
            if message_type == "alert_deactivated_by_empresa":
                self.logger.info("🏢 Detectado mensaje de desactivación por empresa (Redis)")
                return self._handle_empresa_message_sync(json_message)
            
            # Validar estructura del mensaje antes de procesar como WhatsApp webhook
            if (not self._is_valid_whatsapp_webhook(json_message)):
                return True  # No es un error, simplemente ignoramos
            
            entry = json_message["entry"][0]["changes"][0]["value"]["messages"][0]
            
            # Obtener número
            number_client = entry["from"]
            
            # Validar si el usuario ya existe 
            save_message = json_message.get("save_number")
            if save_message:
                # Usuario guardado
                cached_info = json_message["cached_info"]
                self._process_save_number(entry=entry, cached_info=cached_info)
            else:
                # Usuario nuevo
                response_verify = self._process_new_number_sync(number=number_client, entry=entry)
                if not response_verify:
                    if self.whatsapp_service:
                        self.whatsapp_service.send_individual_message(
                            phone=number_client, 
                            message="Lo siento 😞, Pero actualmente no te encuentras registrado en el sistema RESCUE."
                        )
            
            self.logger.info(f"📱 Mensaje WhatsApp #{self.whatsapp_processed_count + 1}:")
            self.logger.info(f"   📞 Número: {number_client}")
            
            self.whatsapp_processed_count += 1
            return True
            
        except json.JSONDecodeError:
            self.logger.error(f"❌ Error: Mensaje no es JSON válido")
            self.whatsapp_error_count += 1
            return False
        except Exception as e:
            self.logger.error(f"❌ Error procesando mensaje: {e}")
            self.whatsapp_error_count += 1
            return False


    def _is_valid_whatsapp_webhook(self, json_message: Dict) -> bool:
        """Validar si el mensaje es un webhook válido de WhatsApp"""
        try:
            return (
                "entry" in json_message and 
                json_message["entry"] and 
                "changes" in json_message["entry"][0] and
                json_message["entry"][0]["changes"] and
                "value" in json_message["entry"][0]["changes"][0] and
                "messages" in json_message["entry"][0]["changes"][0]["value"] and
                json_message["entry"][0]["changes"][0]["value"]["messages"]
            )
        except (KeyError, IndexError, TypeError):
            return False
    def _alarm_back_save(self,entry_alarm:Dict,cached_info:Dict) -> bool:
        """Guardar la alerta antes de crearla"""
        try:
            self.logger.info("Procesando en alarm_back_save")
            number = cached_info["phone"]
            new_data = {
                "alert_active" : False,
                "info_alert" :{
                    "type_alert": entry_alarm["id"],
                    "description": entry_alarm["description"],
                    "datetime": str(datetime.now()),
                    "alert_title": entry_alarm["title"]
                }
            }
            # print(json.dumps(cached_info,indent=4))
            body_message = f"¡{cached_info['name']}!\nPara crear la alerta de {entry_alarm['title']}\nPrimero debes proporcionar la ubicación"
            self.whatsapp_service.update_number_cache(
                phone=number,
                data=new_data,
                empresa_id=cached_info.get("data", {}).get("empresa_id")
            )
            self.whatsapp_service.send_location_request(phone=number,body_text=body_message)
            return True
        except Exception as ex:
            self.logger.error(f"Hubo un error en alarm_back_save {ex}")
            return False
    def _create_alarm(self,cached_info:Dict,ubication:Dict) -> bool:
        try:
            #print(json.dumps(ubication,indent=3))
            alert_info = cached_info["data"]["info_alert"]
            response_alarm = self._create_alarm_in_back(
                descripcion=alert_info["description"],
                latitud=str(ubication["latitude"]),
                longitud=str(ubication["longitude"]),
                tipo_alerta=alert_info["type_alert"],
                usuario_id=cached_info["data"]["id"]
            )
           
   
            if not response_alarm:
                self.logger.error("❌ Backend no respondió al crear la alerta")
                return False

            data_alert = (
                response_alarm.get("alert")
                or response_alarm.get("data")
                or response_alarm.get("alerta")
                or {}
            )
            list_users = data_alert.get("numeros_telefonicos") if isinstance(data_alert, dict) else None
            if not list_users:
                list_users = response_alarm.get("numeros_telefonicos", [])

            if isinstance(list_users, dict):
                list_users = list(list_users.values())
            elif not isinstance(list_users, list):
                list_users = []

            alert_id = None
            if isinstance(data_alert, dict):
                alert_id = data_alert.get("_id") or data_alert.get("alert_id")
            if not alert_id:
                alert_id = response_alarm.get("alert_id") or response_alarm.get("_id")

            # print(f"data_alert: {json.dumps(data_alert, indent=2)}")
            # print(f"list_users: {json.dumps(list_users, indent=2)}")
            # print(f"hardware_location: {json.dumps(hardware_location, indent=2)}")
 
            # Template ya se envía via backend fanout (process_empresa_activation).
            # No mandarlo aquí para evitar duplicado.
            if not list_users:
                self.logger.info("ℹ️ Sin destinatarios en respuesta de creación de alerta")

            # Fanout MQTT siempre, independiente de si hay usuarios con teléfono
            topics = data_alert.get("topics_otros_hardware") or response_alarm.get("topics_otros_hardware") or []
            if topics:
                self._intermediate_to_mqtt(alert=data_alert, topics=topics)
            
            # print(f"list_users exists: {bool(list_users)}")
            # print(f"hardware_location exists: {bool(hardware_location)}")
            # print(f"data_alert has direccion_url: {bool(data_alert.get('direccion_url'))}")
            # print(f"Enviando ubicación: {bool(list_users and data_alert.get('direccion_url'))}")
            #print(json.dumps(response_alarm,indent=4))
           
            # Cache bulk ya lo hace el fanout backend (process_empresa_activation._create_bulk_cache_empresa)
            return True
        except Exception as ex:
            self.logger.error(f"Error en create_alarm {ex}")
            return False

    def _ensure_whatsapp_alert_activation(self, alert_data: Dict, cached_info: Optional[Dict] = None) -> None:
        """Prepare WhatsApp-originated alerts for TV normalization."""
        if not isinstance(alert_data, dict):
            return

        activacion_alerta = alert_data.get("activacion_alerta")
        if not isinstance(activacion_alerta, dict):
            activacion_alerta = {}
            alert_data["activacion_alerta"] = activacion_alerta

        if not activacion_alerta.get("tipo_activacion"):
            activacion_alerta["tipo_activacion"] = "whatsapp"

        if cached_info and not activacion_alerta.get("nombre"):
            creator_name = cached_info.get("name")
            if creator_name:
                activacion_alerta["nombre"] = creator_name

        ubicacion = alert_data.get("ubicacion")
        if not isinstance(ubicacion, dict):
            ubicacion = {}
            alert_data["ubicacion"] = ubicacion

        if not str(ubicacion.get("nombre") or "").strip():
            ubicacion["nombre"] = "whatsapp"
        if not str(ubicacion.get("direccion") or "").strip():
            ubicacion["direccion"] = "whatsapp"

    def _send_bulk_text_message(self,body:str,list_users: list[Dict],name_made:str) -> bool:
        try:
            recipients = []
            for user in list_users:
                message = {
                    "phone":user["numero"],
                    "message" : f"*{name_made}*\n{body}"
                }
                recipients.append(message)
            self.whatsapp_service.send_bulk_individual(recipients = recipients)

        except Exception as ex:
            self.logger.error(f"Error en _send_bulk_text_message {ex}")
            return False
    def _send_bulk_team(self,type_message:str,name_made:str,list_users:list[Dict],message)-> bool:
        try:
            list_validate = [u for u in list_users if u["disponible"]]
            if not list_validate:
                self.logger.info("No hay usuario a quien enviarle informacion")
                return True
            match type_message:
                case "text":
                    self._send_bulk_text_message(list_users=list_validate,body=message,name_made=name_made)
            return True
        except Exception as ex:
            self.logger.error(f"Error en _send_bulk_team {ex}")
            return False

    def _send_bulk_team_media(self, list_users: list[Dict], media_type: str, media_id: str,
                              caption: str, name_made: str) -> bool:
        """Reenvía una media entrante (image/audio/video/sticker) al equipo disponible.

        Usa el media_id entrante directamente (envío inmediato, sin re-subir ni esperar
        al guardado). Para image/video el caption lleva el nombre de quien la envía.
        """
        try:
            list_validate = [u for u in list_users if u.get("disponible")]
            if not list_validate:
                self.logger.info("No hay usuario disponible a quien reenviar la media")
                return True
            friendly = self._get_first_name(name_made) or name_made or "Equipo"
            full_caption = f"{friendly}: {caption}".strip(": ").strip() if caption else friendly
            for usuario in list_validate:
                numero = usuario.get("numero")
                if not numero:
                    continue
                self.whatsapp_service.send_media_by_id(
                    phone=numero,
                    media_type=media_type,
                    media_id=media_id,
                    caption=full_caption,
                )
            return True
        except Exception as ex:
            self.logger.error(f"Error en _send_bulk_team_media {ex}")
            return False
    def _process_save_number(self, entry: Dict, cached_info: Dict) -> None:
        """Procesar mensaje de número guardado"""
        if not cached_info or not self.whatsapp_service:
            return
        number = cached_info["phone"]
        user = cached_info["name"]
        type_message = entry["type"]
        message = None
        #print(f"el entry es {entry}")
        is_list = entry[type_message].get("list_reply", False)
        is_button = entry[type_message].get("button_reply", False)
        #es para crear alarma
        exist_alert = cached_info["data"]
        current_alert_id = (exist_alert.get("info_alert") or {}).get("alert_id", "")
        user_obj = make_whatsapp_user(cached_info, alert_id=current_alert_id)
        is_creator = user_obj.is_creator        # alias de compatibilidad
        is_alert_manager = user_obj.is_manager  # alias de compatibilidad
        # Log IN: registrar mensaje entrante si hay alerta asociada al usuario
        try:
            current_alert_id = (exist_alert.get("info_alert") or {}).get("alert_id")
            sender_role = ""
            rol_data = exist_alert.get("rol") if isinstance(exist_alert, dict) else None
            if isinstance(rol_data, dict):
                sender_role = (rol_data.get("nombre") or rol_data.get("name") or "").strip()
            if current_alert_id:
                self._log_message_to_alert(
                    alert_id=current_alert_id,
                    phone=number,
                    direction="in",
                    msg_type=type_message,
                    entry=entry,
                    user_id=exist_alert.get("id"),
                    user_name=user,
                    user_role=sender_role
                )
                # Forward en tiempo real a managers con foco en esta alerta
                self._forward_to_subscribed_managers(
                    alert_id=current_alert_id,
                    sender_phone=number,
                    sender_name=user,
                    sender_role=sender_role,
                    entry=entry,
                    msg_type=type_message
                )
        except Exception as exc:
            self.logger.debug(f"Log IN omitido: {exc}")

        # Template "Ver detalles": handler universal (creador, sede, manager).
        # Manejar antes de cualquier rama. Acepta varios shapes (button, interactive, text).
        self.logger.info(
            f"🔍 Ver detalles check: type_message={type_message}, entry={json.dumps(entry, default=str)[:500]}"
        )
        if self._is_ver_detalles_tap(entry, type_message):
            self.logger.info("✅ Ver detalles tap detectado → _send_alert_details_to_user")
            # Prioridad: last_notified_alert (la más reciente notificada al usuario)
            # sobre info_alert (foco activo, puede ser una alerta vieja). El tap viene
            # del template de la última alerta notificada, así que apuntamos a ESA.
            target_alert_id = (exist_alert.get("last_notified_alert") or {}).get("alert_id")
            if not target_alert_id:
                target_alert_id = (exist_alert.get("info_alert") or {}).get("alert_id")
            self._send_alert_details_to_user(
                number=number,
                user=user,
                alert_id=target_alert_id,
                user_lookup_id=exist_alert.get("id", ""),
                cached_info=cached_info
            )
            return

        if "alert_active" in exist_alert:
            id_user = exist_alert["id"]
            alert_create = exist_alert["alert_active"]
            if alert_create:
                """ esto es porque la alerta ya se creo"""
                id_alert = exist_alert["info_alert"]["alert_id"]
                #aqui se valida si es un boton lo que llega
                if is_button:
                    type_button = is_button["id"]
                    self.logger.info(
                        "🔘 Boton recibido: %s | disponible=%s",
                        type_button,
                        exist_alert.get("disponible")
                    )
                    #esto valida si es para activacion de un usuario
                    if type_button == "Activar_User":
                        # CRÍTICO: el botón vino del template/Ver detalles de la última alerta
                        # notificada al usuario, NO necesariamente la que tiene en foco.
                        # Priorizar last_notified_alert.alert_id sobre info_alert.alert_id.
                        target_alert_id = (
                            (exist_alert.get("last_notified_alert") or {}).get("alert_id")
                            or id_alert
                        )
                        self.logger.info(
                            f"🟢 Activar_User: target_alert={target_alert_id} "
                            f"(last_notified={(exist_alert.get('last_notified_alert') or {}).get('alert_id')}, "
                            f"info_alert={id_alert})"
                        )
                        # Disponibilidad es POR ALERTA en backend. Siempre informar.
                        self.backend_client.update_user_status(
                            alert_id=target_alert_id,
                            usuario_id=id_user,
                            disponible=True,
                        )
                        # Cache: actualizar info_alert al nuevo foco + disponible=True.
                        # Esto es elección del usuario (tocó disponible → se enfoca).
                        self.whatsapp_service.update_number_cache(
                            phone=number,
                            data={
                                "disponible": True,
                                "alert_active": True,
                                "info_alert": {"alert_id": target_alert_id},
                            },
                            empresa_id=cached_info.get("data", {}).get("empresa_id")
                        )
                        self.whatsapp_service.send_individual_message(
                            phone=number,
                            message="Ahora recibiras mensajes de los miembros del equipo"
                        )
                        self._log_action_to_alert(alert_id=target_alert_id, phone=number, user=user, action="DISPONIBLE", user_id=id_user)
                    elif type_button == "APAGAR ALARMA":
                        if not user_obj.can_apagar():
                            self._send_permission_denied_message(number, user, "apagar la alarma")
                            return
                        try:
                            # Registrar la acción ANTES de desactivar (luego la alerta queda inactiva).
                            self._log_action_to_alert(alert_id=id_alert, phone=number, user=user, action="APAGAR", user_id=id_user)
                            response_desactivate = self._desactivate_alarm_to_back(id_alert=id_alert,cached=cached_info)
                            
                            # Verificar si la desactivación fue exitosa
                            if response_desactivate and response_desactivate.get('success', False):
                                # Lista del response es opcional — cleanup robusto usa
                                # find_numbers_by_alert(alert_id) como fuente primaria.
                                list_users = response_desactivate.get("numeros_telefonicos", []) or []
                                if not list_users:
                                    self.logger.info(
                                        "ℹ️ Backend deactivate sin lista — usando find_numbers_by_alert para cleanup"
                                    )

                                # Limpiar cache de TODOS los teléfonos con foco en esta alerta
                                try:
                                    self._clean_bulk_cache_alert(list_user=list_users, alert_id=id_alert)
                                except Exception as cache_error:
                                    self.logger.error(f"❌ Error limpiando cache: {cache_error}")

                                # Notificar a TODOS los managers de la empresa + limpiar cache de los que tenían foco
                                try:
                                    self._clean_managers_after_deactivation(
                                        alert_id=id_alert,
                                        sede=response_desactivate.get("sede", ""),
                                        nombre_alerta=response_desactivate.get("nombre_alerta") or response_desactivate.get("tipo_alerta", "Alerta"),
                                        all_managers=response_desactivate.get("alert_managers") or []
                                    )
                                except Exception as mgr_error:
                                    self.logger.error(f"❌ Error limpiando managers: {mgr_error}")
                                
                                # Enviar mensajes a otros usuarios (normaliza y skip si sin lista)
                                try:
                                    _norm_p = lambda p: (p or "").strip().lstrip("+")
                                    norm_self = _norm_p(number)
                                    list_not_you = [
                                        u for u in list_users
                                        if isinstance(u, dict) and _norm_p(u.get("numero", "")) != norm_self
                                    ]
                                    if list_not_you:
                                        self._send_bulk_team(list_users=list_not_you,
                                                             name_made=user,type_message="text",
                                                             message="Alerta desactivada.\nConversación grupal concluida.")
                                except Exception as msg_error:
                                    self.logger.error(f"❌ Error enviando mensajes bulk: {msg_error}")
                                
                                # Enviar confirmación al usuario que desactivó
                                try:
                                    self.whatsapp_service.send_individual_message(phone=number,
                                                                                  message="Desactivaste la alarma exitosamente\nConversación grupal concluida.")
                                except Exception as confirm_error:
                                    self.logger.error(f"❌ Error enviando confirmación: {confirm_error}")
                                
                                # Enviar comando MQTT de desactivación
                                try:
                                    topics = response_desactivate.get("topics", [])
                                    if topics:
                                        prioridad = response_desactivate.get("prioridad", "media")
                                        self._send_deactivation_to_mqtt(topics=topics, prioridad=prioridad)
                                    else:
                                        self.logger.info("ℹ️ No hay topics MQTT para desactivar")
                                except Exception as mqtt_error:
                                    self.logger.error(f"❌ Error enviando comandos MQTT: {mqtt_error}")
                                
                                self.logger.info(f"✅ Alarma {id_alert} desactivada exitosamente por usuario {user}")
                            else:
                                # Error en la desactivación - enviar mensaje de error
                                error_msg = response_desactivate.get('message', 'Error desconocido') if response_desactivate else 'Sin respuesta del servidor'
                                self.logger.error(f"❌ Error desactivando alarma {id_alert}: {error_msg}")
                                
                                # Enviar mensaje personalizado de error al usuario
                                try:
                                    self.whatsapp_service.send_individual_message(
                                        phone=number,
                                        message=f"⚠️ {user}, no se pudo desactivar la alarma.\n\nPosibles causas:\n• La alarma ya fue desactivada por otro usuario\n• Error temporal del sistema\n\nPor favor intenta nuevamente o contacta al administrador."
                                    )
                                except Exception as error_msg_error:
                                    self.logger.error(f"❌ Error enviando mensaje de error: {error_msg_error}")
                        
                        except Exception as general_error:
                            # Capturar cualquier error no manejado para evitar que Redis reintente
                            self.logger.error(f"💥 Error general en desactivación de alarma: {general_error}")
                            try:
                                self.whatsapp_service.send_individual_message(
                                    phone=number,
                                    message=f"❌ {user}, ocurrió un error inesperado al desactivar la alarma. Por favor contacta al administrador."
                                )
                            except Exception:
                                pass  # Si ni siquiera podemos enviar el mensaje de error, no hacer nada
                    elif isinstance(exist_alert.get("disponible"), bool) and not exist_alert["disponible"]:
                        self.logger.info(
                            "📍 Reenviando disponibilidad y mapa por boton sin payload"
                        )
                        data_alert = self.backend_client.get_alert_by_id(alert_id = id_alert,user_id=id_user).get("alert",{}) or {}
                        _norm_n3 = lambda p: (p or "").strip().lstrip("+")
                        norm_target3 = _norm_n3(number)
                        data_user = [u for u in (data_alert.get("numeros_telefonicos") or []) if _norm_n3(u.get("numero", "")) == norm_target3]
                        if not data_user and user_obj.is_in_sede_for_alert(data_alert):
                            data_user = [{"numero": number, "nombre": user}]
                        if data_user:
                            self._send_create_active_user(alert=data_alert,list_users=data_user,data_user=cached_info)
                        loc_recipients = data_user if data_user else [{"numero": number, "nombre": user}]
                        self._send_location_personalized_message(
                            numeros_data=loc_recipients,
                            tipo_alarma_info=data_alert
                        )
                elif is_list:
                    #Esto es para si es una lista despues de que ya se activo
                    opcion = is_list["id"]
                    data_alert = self.backend_client.get_alert_by_id(alert_id = id_alert,user_id=id_user).get("alert",{}) or {}
                    numeros_list = data_alert.get("numeros_telefonicos") or []
                    # Normalizar comparación de tel para que dual-role/managers en sede pasen el check.
                    # Si pertenece a sede vía OOP, construir data_user con entrada propia si no aparece raw.
                    _norm_n = lambda p: (p or "").strip().lstrip("+")
                    norm_target = _norm_n(number)
                    data_user = [u for u in numeros_list if _norm_n(u.get("numero", "")) == norm_target]
                    if not data_user and user_obj.is_in_sede_for_alert(data_alert):
                        data_user = [{"numero": number, "nombre": user}]
                    if opcion == "APAGAR":
                        if not user_obj.can_apagar():
                            self._send_permission_denied_message(number, user, "apagar la alarma")
                            self._send_options_user(number=number, user=user, can_manage_alarm=False, is_alert_manager=is_alert_manager, is_in_alert=bool(data_user))
                            return
                        if not user_obj.is_in_sede_for_alert(data_alert):
                            self.whatsapp_service.send_individual_message(
                                phone=number,
                                message="No perteneces a la sede de esta alerta."
                            )
                            self._send_options_user(number=number, user=user, can_manage_alarm=is_creator, is_alert_manager=is_alert_manager, is_in_alert=False)
                            return
                        # Registrar la acción ANTES de desactivar: al apagarse la alerta
                        # queda inactiva y el backend rechaza logs sobre alertas inactivas.
                        self._log_action_to_alert(alert_id=id_alert, phone=number, user=user, action="APAGAR", user_id=id_user)
                        self._send_create_down_alarma(alert=data_alert,data_user=cached_info,list_users=data_user)
                    elif opcion == "UBICACION":
                        # Manager observando alerta ajena: enviar ubicación a su propio número
                        recipients_loc = data_user if data_user else [{"numero": number, "nombre": user}]
                        self._send_location_personalized_message(numeros_data=recipients_loc,tipo_alarma_info=data_alert)
                        self._log_action_to_alert(alert_id=id_alert, phone=number, user=user, action="UBICACION", user_id=id_user)
                    elif opcion == "EMBARCADO":
                        if not user_obj.can_embarcado(data_alert):
                            self.whatsapp_service.send_individual_message(
                                phone=number,
                                message="No perteneces a la sede de esta alerta."
                            )
                            self._send_options_user(number=number, user=user, can_manage_alarm=is_creator, is_alert_manager=is_alert_manager, is_in_alert=False)
                            return
                        data_user_not_you = [u for u in numeros_list if _norm_n(u.get("numero", "")) != norm_target]
                        self.backend_client.update_user_status(alert_id=exist_alert["info_alert"]["alert_id"],
                                                                usuario_id = exist_alert["id"],
                                                                embarcado = True)
                        self.whatsapp_service.update_number_cache(
                            phone = number,
                            data = {"embarcado" : True},
                            empresa_id=cached_info.get("data", {}).get("empresa_id")
                        )
                        self._send_bulk_team(list_users=data_user_not_you,message="Estoy camino a la emergencia",name_made=user,type_message="text")
                        self._log_action_to_alert(alert_id=id_alert, phone=number, user=user, action="EMBARCADO", user_id=id_user)
                    elif opcion == "CAMBIAR_ALERTA":
                        if not user_obj.can_cambiar_alerta():
                            self._send_permission_denied_message(number, user, "cambiar de alerta")
                            self._send_options_user(number=number, user=user, can_manage_alarm=is_creator, is_alert_manager=False, is_in_alert=bool(data_user))
                            return
                        self._send_manager_alert_picker(number=number, user=user, usuario_id=id_user)
                    elif opcion.startswith("SWITCH_ALERT_"):
                        if not user_obj.can_switch_alert():
                            self._send_permission_denied_message(number, user, "cambiar de alerta")
                            return
                        target_alert_id = opcion.replace("SWITCH_ALERT_", "", 1)
                        self._handle_manager_switch(number=number, user=user, target_alert_id=target_alert_id, user_obj=user_obj)


                elif isinstance(exist_alert.get("disponible"), bool) and not exist_alert["disponible"]:
                    #quiere decir que mando un mensaje cuando aun no puede hablar
                    data_alert = self.backend_client.get_alert_by_id(alert_id = id_alert,user_id=id_user).get("alert",{}) or {}
                    _norm_n2 = lambda p: (p or "").strip().lstrip("+")
                    norm_target2 = _norm_n2(number)
                    data_user = [u for u in (data_alert.get("numeros_telefonicos") or []) if _norm_n2(u.get("numero", "")) == norm_target2]
                    if not data_user and user_obj.is_in_sede_for_alert(data_alert):
                        data_user = [{"numero": number, "nombre": user}]
                    if data_user:
                        self._send_create_active_user(alert=data_alert,list_users=data_user,data_user=cached_info)
                    loc_recipients = data_user if data_user else [{"numero": number, "nombre": user}]
                    self._send_location_personalized_message(
                        numeros_data=loc_recipients,
                        tipo_alarma_info=data_alert
                    )
                else:
                    """Aqui se deberia colocar el envio de mensaje a todos los usuarios, pero..
                    por ahora se procesa solo mensajes de texto"""
                    if type_message:
                        inner_payload = entry.get(type_message) or {}

                        # Media (imagen/audio/video/sticker): reenviar al equipo por su
                        # media_id de inmediato (documentos NO). El guardado para el panel
                        # lo hace RescueBack aparte al registrar el mensaje.
                        if type_message in ("image", "audio", "video", "sticker") and isinstance(inner_payload, dict):
                            media_id = inner_payload.get("id")
                            caption = inner_payload.get("caption", "")
                            if media_id:
                                data_alert = self.backend_client.get_alert_by_id(alert_id=id_alert, user_id=id_user).get("alert", {}) or {}
                                team = [u for u in (data_alert.get("numeros_telefonicos") or []) if u.get("numero") != number]
                                if team:
                                    self._send_bulk_team_media(
                                        list_users=team,
                                        media_type=type_message,
                                        media_id=media_id,
                                        caption=caption,
                                        name_made=user,
                                    )
                                else:
                                    self.logger.info("Media sin destinatarios de equipo")
                            else:
                                self.logger.info(f"Media {type_message} sin id, no se reenvía")
                            return

                        if isinstance(inner_payload, dict):
                            body_text = (
                                inner_payload.get("body")
                                or inner_payload.get("text")
                                or inner_payload.get("payload")
                                or inner_payload.get("caption")
                                or ""
                            )
                        else:
                            body_text = ""
                        if not body_text:
                            self.logger.info(f"Mensaje sin body procesable (type={type_message}), ignorando")
                            return
                        if len(body_text) > 1000:
                            self.whatsapp_service.send_individual_message(
                                phone=number,
                                message="Tu mensaje excede los 1000 caracteres. Por favor envía un mensaje más corto."
                            )
                            return
                        comandos_opciones = [
                            "OPCIONES",
                            "MENU",
                            "MENÚ",
                            "OPCION",
                            "OPCIÓN",
                            ".",
                            "LISTA",
                            "LISTADO",
                            "MOSTRAR",
                            "MOSTRAR OPCIONES",
                            "MOSTRAR MENU",
                            "MOSTRAR MENÚ",
                            "VER OPCIONES",
                            "VER MENÚ",
                            "INSTRUCCIONES",
                            "AYUDA",
                            "HELP"
                        ]
                        if body_text.upper() in comandos_opciones:
                            # Membresía de sede via método polimórfico (cubre creator,
                            # manager, dual-role, regular — sin mismatch de formato de tel).
                            data_alert_menu = self.backend_client.get_alert_by_id(alert_id=id_alert, user_id=id_user).get("alert", {}) or {}
                            is_in_current_alert = user_obj.is_in_sede_for_alert(data_alert_menu)
                            self._send_options_user(number=number,user=user,can_manage_alarm=is_creator,is_alert_manager=is_alert_manager,is_in_alert=is_in_current_alert)
                        else:
                            data_alert = self.backend_client.get_alert_by_id(alert_id = id_alert,user_id=id_user).get("alert",{}) or {}
                            data_user = [u for u in (data_alert.get("numeros_telefonicos") or []) if u.get("numero") != number]
                            if data_user:
                                self._send_bulk_team(name_made=user,message=body_text,list_users=data_user,type_message=type_message)
                            else:
                                self.logger.info("El mensaje no tiene destinatarios")
                return
            else:
                #No se ha creado la alerma pero ya se escogio
                fecha_crear_str = exist_alert["info_alert"]["datetime"]
                fecha_crear = datetime.fromisoformat(fecha_crear_str)
                ahora = datetime.now()

                if ahora - fecha_crear < timedelta(minutes=5):
                    #Han pasado menos de 5 minutos
                    #print(json.dumps(entry,indent=4))
                    location = entry.get("location",False)
                    if location:
                        if not user_obj.can_activar_alarma():
                            self._send_permission_denied_message(number, user, "activar la alarma")
                            return
                        self._create_alarm(cached_info=cached_info,ubication=location)
                    else:
                        if not user_obj.can_activar_alarma():
                            self._send_permission_denied_message(number, user, "activar la alarma")
                        else:
                            message_location = f"{user}\nPara crear la alerta {exist_alert['info_alert']['alert_title']}\nDebes enviar la ubicacion."
                            self.whatsapp_service.send_location_request(phone=number,body_text=message_location)
                    return
                else:
                    #Ya pasaron 5 minutos o más
                    message = f"Lo siento {user}.\nPero excediste el tiempo límite de 5 min."
                    data_delete = {
                        "info_alert":"__DELETE__",
                        "alert_active":"__DELETE__"
                    }
                    self.whatsapp_service.update_number_cache(
                        phone=number,
                        data=data_delete,
                        empresa_id=cached_info.get("data", {}).get("empresa_id")
                    )
        else:
            if is_list:
                opcion_id = is_list.get("id", "")
                # Selector de modo para usuarios con ambos roles
                if opcion_id == "MODE_CREATE_ALERT":
                    if not user_obj.can_activar_alarma():
                        self._send_permission_denied_message(number, user, "crear alertas")
                        return
                    self._send_create_alarma(
                        number=number,
                        usuario=user,
                        is_in_cached=True,
                        message_time=message,
                        empresa_id=cached_info.get("data", {}).get("empresa_id")
                    )
                    return
                if opcion_id == "MODE_MANAGER":
                    if not user_obj.can_cambiar_alerta():
                        self._send_permission_denied_message(number, user, "usar modo manager")
                        return
                    id_user_manager = exist_alert.get("id", "")
                    self._send_manager_alert_picker(number=number, user=user, usuario_id=id_user_manager)
                    return
                # Manager: opciones de cambiar foco no requieren is_creator
                if opcion_id == "CAMBIAR_ALERTA":
                    if not user_obj.can_cambiar_alerta():
                        self._send_permission_denied_message(number, user, "cambiar de alerta")
                        return
                    id_user_manager = exist_alert.get("id", "")
                    self._send_manager_alert_picker(number=number, user=user, usuario_id=id_user_manager)
                    return
                if opcion_id.startswith("SWITCH_ALERT_"):
                    if not user_obj.can_switch_alert():
                        self._send_permission_denied_message(number, user, "cambiar de alerta")
                        return
                    target_alert_id = opcion_id.replace("SWITCH_ALERT_", "", 1)
                    self._handle_manager_switch(number=number, user=user, target_alert_id=target_alert_id, user_obj=user_obj)
                    return
                if not user_obj.can_activar_alarma():
                    self._send_permission_denied_message(number, user, "activar una alarma")
                    return
                self._alarm_back_save(entry_alarm=is_list,cached_info=cached_info)
                self.logger.info("Procesando selección de alarma")
                return
            # if is_button:
            #     self.logger.info("Procesando apagar alarma")
            #     #print(f"es para apagar {json.dumps(entry,indent=4)}")
            #     response = self._desactivate_alarm_to_back(entry=is_button,cached=cached_info)
            #     if response and response.get('success'):
            #         # Si se desactivó exitosamente, enviar confirmación personalizada
            #         self._send_alarm_deactivation_success_message(number, user, response)
            #         # También enviar comando de desactivación a los dispositivos MQTT
            #         topics = response.get("topics", [])
            #         prioridad = response.get("prioridad", "media")
            #         if topics:
            #             self._send_deactivation_to_mqtt(topics=topics, prioridad=prioridad)
                    
            #         return  # No enviar lista de alarmas después de desactivar
            #     else:
            #         # Si falló, enviar mensaje de error personalizado 
            #         self._send_alarm_deactivation_error_message(number, user, response)
            #         return  # No enviar lista de alarmas si hubo error
        
        

        # Defensive button dispatcher: si el cache no tiene alert_active pero el usuario
        # tocó un botón conocido (ej. Activar_User desde Ver detalles), procesar el botón
        # antes de caer al role-picker. Evita que tap a botón se confunda con "no hay alerta".
        if is_button:
            button_id = is_button.get("id", "")
            if button_id == "Activar_User":
                # PRIORIDAD: last_notified_alert (la más reciente notificada via template/Ver detalles)
                # > info_alert (foco anterior, posiblemente stale de alerta vieja)
                # > lookup backend (fallback si cache no tiene nada)
                target_alert_id = (
                    (exist_alert.get("last_notified_alert") or {}).get("alert_id")
                    or (exist_alert.get("info_alert") or {}).get("alert_id")
                    or self._find_active_alert_for_user(
                        telefono=number,
                        usuario_id=exist_alert.get("id", ""),
                        user_sede=exist_alert.get("sede", ""),
                    )
                )
                self.logger.info(
                    f"🟢 Activar_User defensive: target={target_alert_id} "
                    f"(last_notified={(exist_alert.get('last_notified_alert') or {}).get('alert_id')}, "
                    f"info_alert={(exist_alert.get('info_alert') or {}).get('alert_id')})"
                )
                if not target_alert_id:
                    self.whatsapp_service.send_individual_message(
                        phone=number,
                        message="No hay alerta activa para marcar disponibilidad."
                    )
                    return
                # Disponibilidad es POR ALERTA en backend (no global). Siempre informar
                # backend — él decide idempotencia per-alerta. Cache.disponible es hint
                # local que puede estar stale de otra alerta anterior.
                try:
                    self.backend_client.update_user_status(
                        alert_id=target_alert_id,
                        usuario_id=exist_alert.get("id", ""),
                        disponible=True,
                    )
                except Exception as ex:
                    self.logger.error(f"❌ Error update_user_status defensive: {ex}")
                try:
                    self.whatsapp_service.update_number_cache(
                        phone=number,
                        data={
                            "disponible": True,
                            "alert_active": True,
                            "info_alert": {"alert_id": target_alert_id},
                        },
                        empresa_id=exist_alert.get("empresa_id"),
                    )
                except Exception as ex:
                    self.logger.error(f"❌ Error update_number_cache defensive: {ex}")
                self.whatsapp_service.send_individual_message(
                    phone=number,
                    message="Ahora recibiras mensajes de los miembros del equipo"
                )
                self._log_action_to_alert(alert_id=target_alert_id, phone=number, user=user, action="DISPONIBLE", user_id=exist_alert.get("id", ""))
                return

        # Usuario con ambos roles: pedir que elija modo primero
        if is_creator and is_alert_manager:
            self._send_role_mode_picker(number=number, user=user)
            return
        # Manager sin permisos de creator: ofrecer menú con opción "Cambiar alerta"
        if not is_creator:
            if is_alert_manager:
                id_user_manager = exist_alert.get("id", "")
                self._send_manager_alert_picker(number=number, user=user, usuario_id=id_user_manager)
                return
            self._send_permission_denied_message(number, user, "activar una alarma")
            return
        self._send_create_alarma(
            number=number,
            usuario=user,
            is_in_cached=True,
            message_time=message,
            empresa_id=cached_info.get("data", {}).get("empresa_id")
        )
    def _clean_managers_after_deactivation(self, alert_id: str, sede: str, nombre_alerta: str,
                                           all_managers: List[Dict] = None) -> None:
        """Notificar a TODOS los managers de la empresa + limpiar foco de los que observaban esta alerta.

        Args:
            alert_id: ID de la alerta desactivada
            sede: Sede de la alerta
            nombre_alerta: Nombre legible para el mensaje
            all_managers: Lista completa de managers de la empresa (vienen del backend)
        """
        if not self.whatsapp_service or not alert_id:
            return
        try:
            notify_text = f"La alerta '{nombre_alerta}' de sede {sede or 'desconocida'} fue desactivada."

            # Notificar a TODOS los managers de la empresa
            notified_phones = set()
            if isinstance(all_managers, list):
                for m in all_managers:
                    raw_phone = m.get("numero") or m.get("telefono") or m.get("phone")
                    if not raw_phone:
                        continue
                    manager_phone = str(raw_phone).lstrip("+")
                    if not manager_phone or manager_phone in notified_phones:
                        continue
                    notified_phones.add(manager_phone)
                    try:
                        self.whatsapp_service.send_individual_message(phone=manager_phone, message=notify_text)
                    except Exception as ex:
                        self.logger.error(f"❌ Error notificando manager {manager_phone}: {ex}")

            # Limpiar cache + notificar (si no fue notificado ya) a managers que tenían foco en esta alerta
            focused_managers = self.whatsapp_service.find_numbers_by_alert(alert_id=str(alert_id), manager_only=True) or []
            patch_data = {
                "info_alert": "__DELETE__",
                "alert_active": "__DELETE__",
                "disponible": "__DELETE__",
                "embarcado": "__DELETE__"
            }
            extra_focus_text = f"Ya no estás en el flujo de la alerta '{nombre_alerta}'."
            for m in focused_managers:
                manager_phone = m.get("phone")
                if not manager_phone:
                    continue
                try:
                    self.whatsapp_service.update_number_cache(
                        phone=manager_phone,
                        data=patch_data,
                        empresa_id=(m.get("data") or {}).get("empresa_id")
                    )
                except Exception as ex:
                    self.logger.error(f"❌ Error limpiando cache manager {manager_phone}: {ex}")
                # Aviso adicional solo si no estaba en la lista global
                if manager_phone not in notified_phones:
                    try:
                        self.whatsapp_service.send_individual_message(phone=manager_phone, message=notify_text)
                    except Exception as ex:
                        self.logger.error(f"❌ Error notificando manager {manager_phone}: {ex}")
                else:
                    try:
                        self.whatsapp_service.send_individual_message(phone=manager_phone, message=extra_focus_text)
                    except Exception as ex:
                        self.logger.error(f"❌ Error avisando salida foco {manager_phone}: {ex}")
        except Exception as ex:
            self.logger.error(f"Error en _clean_managers_after_deactivation: {ex}")

    def _clean_bulk_cache_alert(self, list_user: list = None, alert_id: str = "") -> None:
        """Limpia campos de alerta del cache para TODOS los teléfonos con foco en esta alerta.

        Fuentes (union):
        - find_numbers_by_alert(alert_id): query directo del backend, cubre cualquier
          phone con info_alert.alert_id == este (regular + manager + dual-role).
        - list_user: defensive, soporta entrega parcial del response del deactivate.

        Usa update_number_cache (individual) en lugar de bulk_update_numbers porque
        bulk_update setea __DELETE__ como string literal en vez de remover el campo —
        deja el cache con "alert_active": "__DELETE__" lo que el handler interpreta
        como alerta activa, y la conversación nunca vuelve al estado default.
        """
        try:
            if not self.whatsapp_service:
                return

            patch_data = {
                "info_alert": "__DELETE__",
                "alert_active": "__DELETE__",
                "disponible": "__DELETE__",
                "embarcado": "__DELETE__",
            }

            phones_set: set = set()

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

            if list_user:
                for n in list_user:
                    if not isinstance(n, dict):
                        continue
                    phone = n.get("numero") or n.get("phone") or n.get("telefono")
                    if phone:
                        phones_set.add(str(phone).strip().lstrip("+"))

            if not phones_set:
                self.logger.info(f"ℹ️ Sin teléfonos para limpiar cache de alerta {alert_id}")
                return

            self.logger.info(f"🧹 Limpiando cache de alerta {alert_id} en {len(phones_set)} teléfonos")
            for phone in phones_set:
                try:
                    self.whatsapp_service.update_number_cache(phone=phone, data=patch_data)
                except Exception as ex:
                    self.logger.warning(f"⚠️ No se pudo limpiar cache {phone}: {ex}")
        except Exception as ex:
            self.logger.error(f"Error en _clean_bulk_cache_alert {ex}")
    def _desactivate_alarm_to_back(self,id_alert, cached:Dict) ->Optional[Dict]:
        try:
            user_id = cached["data"]["id"]
            response = self.backend_client.deactivate_user_alert(
                alert_id = id_alert,
                desactivado_por_id = user_id,
                desactivado_por_tipo = "usuario"
            )
            return response
        except Exception as ex:
            self.logger.error(f"Error al tratar de desactivar alerta {ex}")
            return None
      
    def _process_new_number_sync(self, number: str, entry: Dict = None) -> Optional[bool]:
        """Procesar nuevo número (versión síncrona para Redis)"""
        if not self.backend_client:
            return False
            
        response = self.backend_client.verify_user_number(number)
        
        # Verificar si la respuesta es None (error de conexión)
        if response is None:
            self.logger.error("❌ Sin respuesta del servidor")
            return False
            
        # Verificar códigos de estado específicos
        status_code = response.get('_status_code', 200)
        
        if status_code == 404:
            self.logger.info("🔍 Usuario no encontrado (404)")
            return False
        elif status_code == 401:
            self.logger.info("🔒 No autorizado (401)")
            return False
        
        # Para respuestas exitosas, obtener los datos
        if not response.get('success', False):
            self.logger.error(f"❌ Verificación fallida: {response.get('message', 'Error desconocido')}")
            return False
        
        verify_number = response.get("data")
        if verify_number is None or "telefono" not in verify_number:
            self.logger.error("❌ Datos de usuario incompletos")
            return False
            
        usuario = verify_number.get("nombre", "")

        # La clave de sesión debe ser el número que entrega WhatsApp (E.164 sin
        # '+'), no el formato histórico guardado en RescueBack. Si el backend
        # devuelve 310… pero el webhook llega como 57310…, guardar 310… hace que
        # cada interacción posterior vuelva a entrar como usuario nuevo y el flujo
        # nunca avance de la lista de alertas al mapa.
        session_phone = number

        # Normalizar información de empresa (puede venir como string o dict)
        empresa_info = verify_number.get("empresa")
        empresa_id = verify_number.get("empresa_id")
        empresa_nombre = ""

        if isinstance(empresa_info, dict):
            empresa_id = empresa_id or empresa_info.get("id") or empresa_info.get("_id")
            empresa_nombre = empresa_info.get("nombre") or empresa_info.get("name") or ""
        else:
            empresa_nombre = empresa_info or ""

        # Normalizar información de sede (igual puede venir como string o dict)
        sede_info = verify_number.get("sede")
        sede_nombre = ""
        if isinstance(sede_info, dict):
            sede_nombre = sede_info.get("nombre") or sede_info.get("name") or ""
        else:
            sede_nombre = sede_info or ""
        
        # Preparar información de rol (por defecto sin privilegios)
        raw_role = verify_number.get("rol")
        normalized_role = {}
        if isinstance(raw_role, dict):
            normalized_role = {
                "nombre": raw_role.get("nombre") or raw_role.get("name", ""),
                "is_creator": bool(raw_role.get("is_creator")),
                "is_alert_manager": bool(raw_role.get("is_alert_manager"))
            }
        else:
            normalized_role = {"is_creator": False, "is_alert_manager": False}

        # Misma decisión de rol que _process_save_number, vía modelo OOP.
        # Usuario nuevo aún sin cache → snapshot mínimo desde verify_number.
        user_obj = make_whatsapp_user({
            "phone": session_phone,
            "name": usuario,
            "data": {"rol": normalized_role},
        })
        is_creator = user_obj.is_creator
        is_alert_manager = user_obj.is_manager

        # Agregar número al cache de WhatsApp
        if self.whatsapp_service:
            response_verify = self.whatsapp_service.add_number_to_cache(
                phone=session_phone,
                name=verify_number.get("nombre", ""),           
                data={
                    "id": verify_number.get("id"),
                    "empresa": empresa_nombre,
                    "sede": sede_nombre,
                    "rol": normalized_role,
                    **({"empresa_id": empresa_id} if empresa_id else {})
                },
                empresa_id=empresa_id
            )
            if not response_verify:
                return False
        # # Procesar tipo de mensaje si existe
        # if entry:
        #     type_message = entry["type"]
        #     # Procesar desactivación de alarma (apagar)
        #     is_down_alarm = entry[type_message].get("button_reply", False)
        #     if is_down_alarm:
        #         self.logger.info("Procesando apagar alarma (usuario nuevo)")
                
        #         # DEBUG: Imprimir datos del usuario nuevo
        #         print(f"\n=== DEBUG USUARIO NUEVO DESACTIVACION ===")
        #         print(f"Usuario: {usuario}")
        #         print(f"Numero: {number}")
        #         print(f"verify_number: {json.dumps(verify_number, indent=2)}")
        #         print(f"is_down_alarm: {json.dumps(is_down_alarm, indent=2)}")
        #         print("=== END DEBUG USUARIO NUEVO DESACTIVACION ===")
                
        #         # Desactivar alarma igual que usuarios cached
        #         response = self._desactivate_alarm_to_back(
        #             entry=is_down_alarm,
        #             cached={"data": {"id": verify_number.get("id")}}
        #         )
                
        #         # DEBUG: Imprimir respuesta de desactivación
        #         print(f"\n=== DEBUG RESPONSE DESACTIVACION USUARIO NUEVO ===")
        #         #print(f"Response: {json.dumps(response, indent=2)}")
        #         print("=== END DEBUG RESPONSE DESACTIVACION USUARIO NUEVO ===")
                
        #         if response and response.get('success'):
        #             # Si se desactivó exitosamente, enviar confirmación personalizada
        #             self._send_alarm_deactivation_success_message(number, usuario, response)
                    
        #             # También enviar comando de desactivación a los dispositivos MQTT
        #             topics = response.get("topics", [])
        #             prioridad = response.get("prioridad", "media")
        #             if topics:
        #                 self._send_deactivation_to_mqtt(topics=topics, prioridad=prioridad)
        #         else:
        #             # Si falló, enviar mensaje de error personalizado 
        #             print("⚠️ Desactivación falló, enviando mensaje de error")
        #             self._send_alarm_deactivation_error_message(number, usuario, response)
                
        #         return True
        
        # Si la primera interacción del usuario es el tap "Ver detalles" de la plantilla,
        # responder con mapa + botón disponible (si pertenece a la sede de alguna alerta activa)
        # antes de los fallbacks de menú.
        if entry and self._is_ver_detalles_tap(entry, entry.get("type", "")):
            target_alert_id = self._find_active_alert_for_user(
                telefono=session_phone,
                usuario_id=verify_number.get("id", ""),
                user_sede=sede_nombre,
            )
            if target_alert_id:
                synthetic_cached_info = {
                    "phone": session_phone,
                    "name": usuario,
                    "data": {
                        "id": verify_number.get("id"),
                        "empresa": empresa_nombre,
                        "sede": sede_nombre,
                        "rol": normalized_role,
                        **({"empresa_id": empresa_id} if empresa_id else {}),
                    },
                }
                self._send_alert_details_to_user(
                    number=number,
                    user=usuario,
                    alert_id=target_alert_id,
                    user_lookup_id=verify_number.get("id", ""),
                    cached_info=synthetic_cached_info,
                )
                return True

        # Usuario con ambos roles: pedir que elija modo primero
        if is_creator and is_alert_manager:
            self._send_role_mode_picker(number=number, user=usuario)
            return True
        if not is_creator:
            if is_alert_manager:
                self._send_manager_alert_picker(number=number, user=usuario, usuario_id=verify_number.get("id", ""))
                return True
            self._send_permission_denied_message(number, usuario, "crear alertas")
            return True

        # Enviar lista de alarmas
        self._send_create_alarma(number=number, usuario=usuario, empresa_id=empresa_id)
        return True

    def _find_active_alert_for_user(self, telefono: str, usuario_id: str, user_sede: str) -> str:
        """Busca alerta activa que matchee la sede del usuario.
        Usa manager_list_active_alerts del backend, filtra por sede.
        Retorna alert_id si encuentra, "" si no."""
        if not self.backend_client:
            return ""
        try:
            resp = self.backend_client.manager_list_active_alerts(
                telefono=telefono, usuario_id=usuario_id
            ) or {}
            alerts = resp.get("alerts") or resp.get("data") or []
            if not isinstance(alerts, list):
                return ""
            user_sede_norm = (user_sede or "").strip()
            for alert in alerts:
                if not isinstance(alert, dict):
                    continue
                alert_sede = (alert.get("sede") or "").strip()
                if user_sede_norm and alert_sede == user_sede_norm:
                    return alert.get("_id") or alert.get("alert_id") or ""
            # Si no matchea sede pero solo hay una alerta activa, usarla
            if len(alerts) == 1 and isinstance(alerts[0], dict):
                return alerts[0].get("_id") or alerts[0].get("alert_id") or ""
            return ""
        except Exception as ex:
            self.logger.debug(f"_find_active_alert_for_user error: {ex}")
            return ""

    def _send_options_user(self, number: str, user: str, can_manage_alarm: bool = False, is_alert_manager: bool = False, is_in_alert: bool = True) -> bool:
        if not self.whatsapp_service:
            self.logger.warning("⚠️ WhatsApp service no disponible")
            return False
        try:
            rows = []
            # APAGAR y EMBARCADO solo si el usuario pertenece a la sede de la alerta
            if is_in_alert and can_manage_alarm:
                rows.append({
                    "id": "APAGAR",
                    "title": "Apagar Alarma",
                    "description": "Al seleccionar esta opción, la alarma en cuestión se apagará."
                })

            # UBICACION disponible para usuarios de la sede y para managers observando
            rows.append({
                "id": "UBICACION",
                "title": "Ubicación de la alarma",
                "description": "Obtener la ubicación de la alarma"
            })

            if is_in_alert:
                rows.append({
                    "id": "EMBARCADO",
                    "title": "Embarcarme",
                    "description": "Indica que ya estás en camino a la emergencia"
                })

            if is_alert_manager:
                rows.append({
                    "id": "CAMBIAR_ALERTA",
                    "title": "Cambiar alerta",
                    "description": "Ver y cambiar a una alerta activa de otra sede"
                })

            sections = [
                {
                    "title": "Servicios técnicos",
                    "rows": rows
                }
            ]
            friendly_user = self._get_first_name(user) or "Usuario"
            body_text = f"{friendly_user}\nEstas son las opciones disponibles"
            self.whatsapp_service.send_list_message(
                        phone=number,
                        header_text=body_text,
                        body_text="Selecciona la opción que deseas",
                        footer_text="RESCUE SYSTEM",
                        button_text="Ver opciones",
                        sections=sections
                    )
   

        except Exception  as ex:
            self.logger.error(f"Error en _send_options_user {ex}")
            return False

    def _send_role_mode_picker(self, number: str, user: str) -> None:
        """Envía selector de modo a usuarios con ambos roles (creator + alert_manager)"""
        if not self.whatsapp_service:
            return
        try:
            rows = [
                {
                    "id": "MODE_CREATE_ALERT",
                    "title": "Crear alerta",
                    "description": "Generar una nueva alerta en tu sede"
                },
                {
                    "id": "MODE_MANAGER",
                    "title": "Modo manager",
                    "description": "Ver y cambiar a una alerta activa de otra sede"
                }
            ]
            sections = [{"title": "Modos disponibles", "rows": rows}]
            friendly_user = self._get_first_name(user) or "Usuario"
            self.whatsapp_service.send_list_message(
                phone=number,
                header_text=f"{friendly_user}\nTienes dos roles disponibles",
                body_text="Elige cómo quieres continuar",
                footer_text="RESCUE SYSTEM",
                button_text="Ver opciones",
                sections=sections
            )
        except Exception as ex:
            self.logger.error(f"Error en _send_role_mode_picker: {ex}")

    def _send_manager_alert_picker(self, number: str, user: str, usuario_id: str) -> None:
        """Enviar lista de alertas activas (una por sede) para que el manager elija una"""
        if not self.whatsapp_service or not self.backend_client:
            return
        try:
            payload = self.backend_client.manager_list_active_alerts(telefono=number, usuario_id=usuario_id)
            alerts = (payload or {}).get("alerts", []) if isinstance(payload, dict) else []
            if not alerts:
                self.whatsapp_service.send_individual_message(
                    phone=number,
                    message="No hay alertas activas en ninguna sede de tu empresa."
                )
                return

            rows = []
            for a in alerts[:10]:
                alert_id = a.get("alert_id")
                if not alert_id:
                    continue
                sede = a.get("sede") or "Sin sede"
                titulo = (a.get("nombre_alerta") or a.get("tipo_alerta") or "Alerta")[:24]
                fecha = a.get("fecha_creacion") or ""
                rows.append({
                    "id": f"SWITCH_ALERT_{alert_id}",
                    "title": f"{sede}: {titulo}"[:24],
                    "description": (a.get("descripcion") or fecha)[:72]
                })

            if not rows:
                self.whatsapp_service.send_individual_message(
                    phone=number,
                    message="No hay alertas activas disponibles para cambiar."
                )
                return

            sections = [{"title": "Alertas activas por sede", "rows": rows}]
            friendly_user = self._get_first_name(user) or "Manager"
            self.whatsapp_service.send_list_message(
                phone=number,
                header_text=f"{friendly_user}\nElige la alerta en la que quieres participar",
                body_text="Cada opción es la última alerta activa de una sede.",
                footer_text="RESCUE SYSTEM",
                button_text="Ver alertas",
                sections=sections
            )
        except Exception as ex:
            self.logger.error(f"Error en _send_manager_alert_picker: {ex}")

    def _handle_manager_switch(self, number: str, user: str, target_alert_id: str, user_obj=None) -> None:
        """Procesar cambio de foco a la alerta elegida por el manager"""
        if not self.whatsapp_service or not self.backend_client:
            return
        try:
            result = self.backend_client.manager_switch_focus(telefono=number, alert_id=target_alert_id)
            if isinstance(result, dict) and result.get("success"):
                alert_info = result.get("alert", {})
                sede = alert_info.get("sede") or "Sin sede"
                nombre = alert_info.get("nombre_alerta") or alert_info.get("tipo_alerta") or "Alerta"
                self.whatsapp_service.send_individual_message(
                    phone=number,
                    message=f"Foco cambiado a la alerta '{nombre}' (sede {sede})."
                )
                # Enviar resumen de últimos mensajes de usuarios (direction=in)
                self._send_alert_conversation_summary(number=number, alert_id=target_alert_id, limit=15)
            else:
                err = (result or {}).get("error") if isinstance(result, dict) else "Error desconocido"
                self.whatsapp_service.send_individual_message(
                    phone=number,
                    message=f"No se pudo cambiar la alerta: {err}"
                )
        except Exception as ex:
            self.logger.error(f"Error en _handle_manager_switch: {ex}")

    @staticmethod
    def _is_ver_detalles_tap(entry: Dict, type_message: str) -> bool:
        """Detecta tap del usuario en el botón 'Ver detalles' (cualquier shape de webhook).
        Cubre: quick-reply de plantilla (type=button), botón interactivo (type=interactive)
        con button_reply, mensaje de texto que sea el label del botón."""
        if not isinstance(entry, dict):
            return False

        def _has_ver_detalles(s) -> bool:
            if not isinstance(s, str):
                return False
            return "VER DETALLES" in s.strip().upper()

        # Quick-reply de plantilla
        if type_message == "button":
            btn = entry.get("button") or {}
            if isinstance(btn, dict):
                if _has_ver_detalles(btn.get("text")) or _has_ver_detalles(btn.get("payload")):
                    return True
            elif _has_ver_detalles(btn):
                return True

        # Botón interactivo (no debería pero por si acaso)
        if type_message == "interactive":
            inner = entry.get("interactive") or {}
            btn_reply = inner.get("button_reply") or {}
            if _has_ver_detalles(btn_reply.get("title")) or _has_ver_detalles(btn_reply.get("id")):
                return True
            list_reply = inner.get("list_reply") or {}
            if _has_ver_detalles(list_reply.get("title")) or _has_ver_detalles(list_reply.get("id")):
                return True

        # Texto plano "Ver detalles" (algunos clientes lo envían así)
        if type_message == "text":
            txt = (entry.get("text") or {}).get("body") if isinstance(entry.get("text"), dict) else entry.get("text")
            if _has_ver_detalles(txt):
                return True

        return False

    def _send_alert_details_to_user(self, number: str, user: str, alert_id: Optional[str],
                                    user_lookup_id: str = "",
                                    cached_info: Optional[Dict] = None) -> None:
        """Responde al tap 'Ver detalles': mapa siempre + botón disponible si pertenece a la sede."""
        if not self.whatsapp_service:
            return

        if not alert_id:
            self.logger.info("Ver detalles: sin alert_id en cache del usuario")
            self.whatsapp_service.send_individual_message(
                phone=number,
                message="No hay alerta activa para mostrar."
            )
            return

        try:
            resp = self.backend_client.get_alert_by_id(alert_id=alert_id, user_id=user_lookup_id) or {}
            data_alert = resp.get("alert", {}) or {}
        except Exception as ex:
            self.logger.error(f"Error obteniendo alerta {alert_id} para Ver detalles: {ex}")
            self.whatsapp_service.send_individual_message(
                phone=number,
                message="No se pudo obtener la alerta. Intenta de nuevo."
            )
            return

        if not data_alert:
            self.logger.warning(f"Ver detalles: backend devolvió alert vacío para {alert_id}")
            self.whatsapp_service.send_individual_message(
                phone=number,
                message="No se pudo obtener la alerta. Intenta de nuevo."
            )
            return

        # Construir AlertUser para delegar lógica de membresía de sede (OOP)
        user_obj = make_whatsapp_user(cached_info or {}, alert_id=alert_id)
        is_in_sede = user_obj.is_in_sede_for_alert(data_alert)

        # data_user: entrada de numeros_telefonicos para _send_create_active_user
        # Si manager no está en numeros_telefonicos pero sí en su sede → entrada sintética
        def _norm(p: str) -> str:
            return (p or "").strip().lstrip("+")

        norm_number = _norm(number)
        data_user = [u for u in (data_alert.get("numeros_telefonicos") or [])
                     if _norm(u.get("numero", "")) == norm_number]
        if not data_user and is_in_sede:
            data_user = [{"numero": number, "nombre": user}]

        # Mapa siempre (si hay url_maps)
        ubicacion = data_alert.get("ubicacion", {})
        if isinstance(ubicacion, dict) and ubicacion.get("url_maps"):
            self._send_location_personalized_message(
                numeros_data=[{"numero": number, "nombre": user}],
                tipo_alarma_info=data_alert
            )
        else:
            alert_name = data_alert.get("nombre_alerta") or data_alert.get("tipo_alerta", "Alerta")
            sede_name = data_alert.get("sede", "")
            msg = f"Alerta {alert_name}"
            if sede_name:
                msg += f" en sede {sede_name}"
            msg += ".\nSin ubicación disponible."
            self.whatsapp_service.send_individual_message(phone=number, message=msg)

        # Botón "Estoy disponible": pertenece a la sede + no está disponible PARA ESTA alerta.
        # CRÍTICO: leer disponibilidad del backend (data_alert.numeros_telefonicos[].disponible),
        # NO del cache local. El cache puede tener disponible=True de una alerta anterior
        # que aún no se ha desactivado — sería bug enviar/no enviar basado en ese flag stale.
        if is_in_sede:
            ya_disponible_en_esta_alerta = False
            if data_user and isinstance(data_user[0], dict):
                ya_disponible_en_esta_alerta = bool(data_user[0].get("disponible", False))
            if not ya_disponible_en_esta_alerta:
                self._send_create_active_user(
                    alert=data_alert,
                    list_users=data_user,
                    data_user=cached_info or {}
                )
            else:
                self.logger.info(
                    f"ℹ️ Usuario {number} ya marcado disponible PARA ESTA alerta — botón omitido"
                )

    def _send_alert_conversation_summary(self, number: str, alert_id: str, limit: int = 15) -> None:
        """Envía al manager un resumen con los últimos N mensajes de usuarios (IN) de la alerta"""
        if not self.whatsapp_service or not self.backend_client:
            return
        try:
            payload = self.backend_client.get_alert_messages(alert_id=alert_id, direction="in", limit=limit)
            messages = (payload or {}).get("messages", []) if isinstance(payload, dict) else []
            if not messages:
                self.whatsapp_service.send_individual_message(
                    phone=number,
                    message="Sin mensajes previos de usuarios en esta alerta."
                )
                return
            lines = ["Últimos mensajes de usuarios:"]
            for m in messages:
                fecha_raw = m.get("fecha") or ""
                hora = fecha_raw[11:16] if len(fecha_raw) >= 16 else fecha_raw
                nombre = (m.get("user_name") or m.get("phone") or "Usuario")[:24]
                rol = (m.get("user_role") or "").strip()
                body = (m.get("body") or "").strip().replace("\n", " ")
                if len(body) > 120:
                    body = body[:117] + "..."
                if not body:
                    body = f"[{m.get('type', 'mensaje')}]"
                etiqueta = f"{nombre} ({rol})" if rol else nombre
                lines.append(f"[{hora}] {etiqueta}: {body}")
            text = "\n".join(lines)
            if len(text) > 3500:
                text = text[:3500] + "\n..."
            self.whatsapp_service.send_individual_message(phone=number, message=text)
        except Exception as ex:
            self.logger.error(f"Error enviando resumen de conversación: {ex}")

    def _forward_to_subscribed_managers(self, alert_id: str, sender_phone: str,
                                        sender_name: str, entry: Dict, msg_type: str,
                                        sender_role: str = "") -> None:
        """Reenvía mensaje IN del usuario a managers con foco en esta alerta (tiempo real)"""
        if not self.whatsapp_service or not alert_id:
            return
        try:
            # Extraer texto del mensaje
            body_text = ""
            if isinstance(entry, dict):
                inner = entry.get(msg_type)
                if isinstance(inner, dict):
                    if msg_type == "text":
                        body_text = inner.get("body", "")
                    elif msg_type == "interactive":
                        list_reply = inner.get("list_reply") or {}
                        btn_reply = inner.get("button_reply") or {}
                        body_text = list_reply.get("title") or btn_reply.get("title") or ""
                    else:
                        body_text = inner.get("caption") or inner.get("body") or f"[{msg_type}]"
            if not body_text:
                body_text = f"[{msg_type}]"

            subscribers = self.whatsapp_service.find_numbers_by_alert(alert_id=alert_id, manager_only=True)
            if not subscribers:
                return

            friendly_sender = self._get_first_name(sender_name) or sender_phone
            if sender_role:
                forward_text = f"{friendly_sender} ({sender_role}): {body_text}"
            else:
                forward_text = f"{friendly_sender}: {body_text}"

            # Si es media (image/audio/video/sticker), reenviar el archivo real por su
            # media_id en vez de "[image]".
            media_type = msg_type if msg_type in ("image", "audio", "video", "sticker") else None
            media_id = None
            media_caption = ""
            if media_type and isinstance(entry, dict):
                media_inner = entry.get(msg_type) or {}
                if isinstance(media_inner, dict):
                    media_id = media_inner.get("id")
                    cap = media_inner.get("caption", "")
                    media_caption = f"{friendly_sender}: {cap}".strip(": ").strip() if cap else friendly_sender

            import threading

            def _fire():
                try:
                    for sub in subscribers:
                        manager_phone = sub.get("phone")
                        if not manager_phone or manager_phone == sender_phone:
                            continue
                        try:
                            if media_type and media_id:
                                self.whatsapp_service.send_media_by_id(
                                    phone=manager_phone,
                                    media_type=media_type,
                                    media_id=media_id,
                                    caption=media_caption,
                                )
                            else:
                                self.whatsapp_service.send_individual_message(
                                    phone=manager_phone,
                                    message=forward_text
                                )
                        except Exception:
                            continue
                except Exception:
                    pass

            threading.Thread(target=_fire, daemon=True).start()
        except Exception as exc:
            self.logger.debug(f"_forward_to_subscribed_managers ignorado: {exc}")

    def _log_message_to_alert(self, alert_id: str, phone: str, direction: str,
                              msg_type: str, entry: Optional[Dict] = None,
                              body: Optional[str] = None,
                              user_id: Optional[str] = None, user_name: Optional[str] = None,
                              user_role: str = "",
                              is_template: bool = False) -> None:
        """Fire-and-forget: registra mensaje en backend para auditoría/resumen"""
        if not self.backend_client or not alert_id:
            return
        try:
            import threading
            extracted_body = body
            payload_raw = {}
            if entry and isinstance(entry, dict):
                payload_raw = entry
                inner = entry.get(msg_type)
                if isinstance(inner, dict):
                    if extracted_body is None:
                        if msg_type == "text":
                            extracted_body = inner.get("body", "")
                        elif msg_type == "interactive":
                            list_reply = inner.get("list_reply") or {}
                            btn_reply = inner.get("button_reply") or {}
                            extracted_body = list_reply.get("title") or btn_reply.get("title") or list_reply.get("id") or btn_reply.get("id") or ""
                        elif msg_type == "button":
                            extracted_body = inner.get("text") or inner.get("payload") or ""
                        else:
                            extracted_body = inner.get("caption") or inner.get("body") or ""

            # Detectar mensajes de navegación (replies a menús/botones, no input orgánico)
            is_navigation = False
            if direction == "in" and entry and isinstance(entry, dict):
                inner = entry.get(msg_type) if msg_type else None
                if msg_type == "interactive" and isinstance(inner, dict):
                    if inner.get("list_reply") or inner.get("button_reply"):
                        is_navigation = True
                elif msg_type == "button" and isinstance(inner, dict):
                    is_navigation = True

            data = {
                "phone": phone,
                "direction": direction,
                "type": msg_type or "text",
                "body": extracted_body or "",
                "payload": payload_raw,
                "user_id": user_id,
                "user_name": user_name,
                "user_role": user_role or "",
                "is_template": bool(is_template),
                "is_navigation": is_navigation
            }

            def _fire():
                try:
                    self.backend_client.log_alert_message(alert_id=alert_id, payload=data)
                except Exception:
                    pass

            threading.Thread(target=_fire, daemon=True).start()
        except Exception as exc:
            self.logger.debug(f"_log_message_to_alert ignorado: {exc}")

    def _log_action_to_alert(self, alert_id: str, phone: str, user: str,
                             action: str, user_id: Optional[str] = None) -> None:
        """Registra la ACCIÓN del usuario (embarcado/apagar/disponible/ubicación)
        como un mensaje de conversación visible en el panel de la empresa.

        Se registra como texto limpio (entry=None → is_navigation=False) para que
        NO se filtre como ruido de menú y aparezca en el chat en vivo.
        """
        body = action_message(action, user)
        if not body:
            return
        self._log_message_to_alert(
            alert_id=alert_id,
            phone=phone,
            direction="in",
            msg_type="text",
            body=body,
            user_id=user_id,
            user_name=user,
            user_role="usuario",
        )

    def _resolve_empresa_id(self, phone: str, current_empresa_id: Optional[str] = None) -> Optional[str]:
        """Obtener empresa_id a partir del número de teléfono"""
        if current_empresa_id:
            return current_empresa_id

        if not self.backend_client:
            self.logger.error("❌ No hay backend_client para resolver empresa_id")
            return None

        try:
            response = self.backend_client.verify_user_number(phone)
        except Exception as ex:
            self.logger.error(f"❌ Error verificando número {phone} para obtener empresa_id: {ex}")
            return None

        if not response:
            self.logger.error("❌ Respuesta vacía al verificar número para empresa_id")
            return None

        status_code = response.get('_status_code', 200)
        if status_code in (401, 404):
            self.logger.error(f"❌ Backend devolvió status {status_code} al verificar número {phone}")
            return None

        if not response.get('success', False):
            self.logger.error("❌ Verificación de número no exitosa, no se puede obtener empresa_id")
            return None

        data = response.get('data', {}) or {}
        empresa_id = data.get('empresa_id')
        empresa_info = data.get('empresa')

        if isinstance(empresa_info, dict):
            empresa_id = empresa_id or empresa_info.get('id') or empresa_info.get('_id')

        if not empresa_id and isinstance(empresa_info, str):
            self.logger.warning(f"⚠️ Empresa proporcionada sin identificador para {phone}: {empresa_info}")

        return empresa_id

    def _ensure_unique_row_id(
        self,
        base_id: str,
        alert_type: Dict[str, Any],
        seen_ids: Set[str],
        index: int,
        empresa_id: Optional[str],
    ) -> str:
        """Garantizar que el ID de la fila sea único en la sección"""
        candidates = [
            alert_type.get("id"),
            alert_type.get("_id"),
            alert_type.get("uuid"),
            alert_type.get("uid"),
            alert_type.get("codigo"),
            alert_type.get("code"),
            alert_type.get("tipo_alerta"),
            alert_type.get("identificador")
        ]

        for candidate in candidates:
            candidate_str = str(candidate).strip() if candidate is not None else ""
            if candidate_str and candidate_str not in seen_ids:
                self.logger.warning(
                    "⚠️ ID duplicado en lista WhatsApp (%s). Ajustando a '%s'",
                    base_id,
                    candidate_str,
                )
                return candidate_str

        suffix = 1
        unique_id = f"{base_id or 'option'}-{index + 1}"
        while unique_id in seen_ids:
            suffix += 1
            unique_id = f"{base_id or 'option'}-{index + 1}-{suffix}"

        self.logger.warning(
            "⚠️ ID duplicado en lista WhatsApp (%s) para empresa %s. Se asigna '%s'",
            base_id,
            empresa_id,
            unique_id,
        )
        return unique_id


    def _send_alert_created_template(
        self,
        recipients: List[Dict],
        alert_info: Dict,
        creator_name: Optional[str]
    ) -> None:
        """Enviar plantilla de alerta creada antes de otros mensajes"""
        if not self.whatsapp_service:
            return

        template_recipients = []
        alert_name = alert_info.get("nombre_alerta") or alert_info.get("nombre") or "Alerta"
        creator = creator_name or alert_info.get("activacion_alerta", {}).get("nombre", "un miembro autorizado")
        sede = alert_info.get("sede")
        # Embebemos zona dentro del 3er param (template Meta tiene solo 3 placeholders)
        creator_con_zona = f"{creator}, en la zona {sede}" if sede else creator

        for usuario in recipients:
            numero = usuario.get("numero")
            if not numero:
                continue

            recipient_name = usuario.get("nombre") or usuario.get("name") or "Usuario"

            template_recipients.append({
                "phone": numero,
                "template_name": "crear_alerta",
                "language": "es_CO",
                "components": [
                    {
                        "type": "body",
                        "parameters": [
                            {"type": "text", "text": recipient_name},
                            {"type": "text", "text": alert_name},
                            {"type": "text", "text": creator_con_zona}
                        ]
                    }
                ]
            })

        if template_recipients:
            self.logger.info(
                "📨 Enviando plantilla crear_alerta a %s destinatarios",
                len(template_recipients)
            )
            if self.logger.isEnabledFor(logging.INFO):
                sample_recipient = template_recipients[0]
                self.logger.info(
                    "📨 Crear_alerta sample: phone=%s params=%s",
                    sample_recipient.get("phone"),
                    [
                        param.get("text")
                        for param in sample_recipient.get("components", [{}])[0].get("parameters", [])
                    ]
                )
            self.whatsapp_service.send_bulk_template(
                recipients=template_recipients,
                use_queue=True
            )
            # Audit: registrar envíos de plantilla
            try:
                import threading
                alert_id_for_log = alert_info.get("_id")
                if alert_id_for_log and self.backend_client:
                    alert_id_str = str(alert_id_for_log)
                    recipients_copy = list(template_recipients)
                    summary = f"Plantilla crear_alerta enviada (alerta {alert_name})"

                    def _fire():
                        try:
                            for r in recipients_copy:
                                phone = r.get("phone")
                                if not phone:
                                    continue
                                payload = {
                                    "phone": phone,
                                    "direction": "out",
                                    "type": "template",
                                    "body": summary,
                                    "payload": r,
                                    "is_template": True
                                }
                                self.backend_client.log_alert_message(alert_id=alert_id_str, payload=payload)
                        except Exception:
                            pass

                    threading.Thread(target=_fire, daemon=True).start()
            except Exception as exc:
                self.logger.debug(f"audit template ignorado: {exc}")

    def _map_backend_alert_type(self, alert_type: Dict[str, Any]) -> Optional[Dict[str, str]]:
        """Convertir un tipo de alerta del backend al formato de WhatsApp list"""
        if not isinstance(alert_type, dict):
            return None

        row_id = (
            alert_type.get("id")
            or alert_type.get("_id")
            or alert_type.get("uuid")
            or alert_type.get("uid")
            or alert_type.get("codigo")
            or alert_type.get("code")
            or alert_type.get("tipo_alerta")
            or alert_type.get("identificador")
        )

        if not row_id:
            return None

        row_id_str = str(row_id).strip()
        if not row_id_str:
            return None

        title = alert_type.get("nombre") or alert_type.get("titulo") or alert_type.get("name") or str(row_id)
        description = alert_type.get("descripcion") or alert_type.get("description") or ""

        return {
            "id": row_id_str,
            "title": str(title),
            "description": str(description)
        }

    def _build_alert_sections(self, phone: str, empresa_id: Optional[str]) -> list[Dict[str, Any]]:
        """Obtener secciones de alertas consultando el backend"""
        sections: list[Dict[str, Any]] = []

        resolved_empresa_id = self._resolve_empresa_id(phone, empresa_id)

        if not resolved_empresa_id:
            self.logger.error("❌ No se recibió empresa_id para obtener tipos de alerta")
            return sections

        if not self.backend_client or not hasattr(self.backend_client, "get_empresa_alarm_types"):
            self.logger.error("❌ Backend client no soporta consulta de tipos de alerta")
            return sections

        try:
            response = self.backend_client.get_empresa_alarm_types(resolved_empresa_id)
        except Exception as ex:
            self.logger.error(f"❌ Error consultando tipos de alerta para empresa {empresa_id}: {ex}")
            return sections

        if not response:
            self.logger.error("❌ Respuesta vacía al consultar tipos de alerta")
            return sections

        success_value = response.get("success")
        if success_value is False:
            error_msg = response.get("message", "Respuesta inválida")
            self.logger.error(f"❌ Backend rechazó la consulta de tipos de alerta: {error_msg}")
            return sections

        alert_types_raw = response.get("data")

        potential_lists = []
        if isinstance(alert_types_raw, list):
            potential_lists.append(alert_types_raw)
        elif isinstance(alert_types_raw, dict):
            for key in ("tipos", "tipos_alerta", "alert_types", "items", "results", "data"):
                value = alert_types_raw.get(key)
                if isinstance(value, list):
                    potential_lists.append(value)
            # some APIs return {'success': True, 'data': {'items': [...] }} and also include list under same dict
            if not potential_lists and all(isinstance(v, dict) for v in alert_types_raw.values()):
                potential_lists.append(list(alert_types_raw.values()))

        for alert_type_list in potential_lists:
            rows = []
            seen_row_ids: Set[str] = set()
            for index, alert_type in enumerate(alert_type_list):
                mapped_row = self._map_backend_alert_type(alert_type)
                if not mapped_row:
                    continue

                row_id = mapped_row.get("id", "").strip()
                if not row_id:
                    row_id = f"option-{index + 1}"

                if row_id in seen_row_ids:
                    row_id = self._ensure_unique_row_id(
                        base_id=row_id,
                        alert_type=alert_type,
                        seen_ids=seen_row_ids,
                        index=index,
                        empresa_id=resolved_empresa_id,
                    )

                mapped_row["id"] = row_id
                seen_row_ids.add(row_id)
                rows.append(mapped_row)

            if rows:
                sections.append({"title": "Servicios técnicos", "rows": rows})
                break

        if not sections:
            self.logger.error("❌ No se encontraron tipos de alerta válidos en la respuesta del backend")

        return sections

    def _send_create_alarma(self, number, usuario, is_in_cached: bool = False, message_time=None, empresa_id: Optional[str] = None) -> bool:
        """Crear y enviar lista de alarmas por WhatsApp"""
        if not self.whatsapp_service:
            self.logger.warning("⚠️ WhatsApp service no disponible")
            return False
            
        try:
            resolved_empresa_id = empresa_id or self._resolve_empresa_id(number)

            if not resolved_empresa_id:
                self.logger.error("❌ No se enviará menú de alertas: empresa_id desconocido")
                self.whatsapp_service.send_individual_message(
                    phone=number,
                    message=(
                        "⚠️ No se pudo identificar la empresa asociada a tu cuenta. "
                        "Contacta al administrador para validar tu registro."
                    ),
                    use_queue=True
                )
                return False

            if not empresa_id:
                try:
                    self.whatsapp_service.update_number_cache(
                        phone=number,
                        data={"empresa_id": resolved_empresa_id},
                        empresa_id=resolved_empresa_id
                    )
                except Exception as ex:
                    self.logger.warning(f"⚠️ No se pudo actualizar el cache con empresa_id: {ex}")

            sections = self._build_alert_sections(number, resolved_empresa_id)
            if not sections:
                self.logger.error("❌ No se enviará menú de alertas por falta de datos dinámicos")
                if self.whatsapp_service:
                    info_message = (
                        "⚠️ No se encontraron tipos de alerta configurados para tu empresa. "
                        "Contacta al administrador para habilitarlos."
                    )
                    self.whatsapp_service.send_individual_message(
                        phone=number,
                        message=info_message,
                        use_queue=True
                    )
                return False
  
            if message_time is not None:
                body_text = message_time
            else:
                display_name = self._get_first_name(usuario) or "Usuario"
                body_text = (
                    f"Hola de nuevo {display_name}.\nUn gusto tenerte de vuelta"
                    if is_in_cached
                    else f"Hola {display_name}.\nBienvenido al Sistema de Alertas RESCUE"
                )
            
            self.whatsapp_service.send_list_message(
                phone=number,
                header_text=body_text,
                body_text="Selecciona la alerta que deseas activar",
                footer_text="RESCUE SYSTEM",
                button_text="Ver alertas",
                sections=sections
            )
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error creando alarma: {e}")
            return False

    def _create_alarm_in_back(self, usuario_id: str, tipo_alerta: str, descripcion: str, 
                            latitud: str = "0.0", longitud: str = "0.0") -> Dict:
        """
        Crear alerta en el backend con ubicación.
        Si no se proporcionan coordenadas, se usan valores por defecto.
        """
        prioridad = "media"
        try:
            response = self.backend_client.create_user_alert(
                usuario_id=usuario_id,
                latitud=latitud,
                longitud=longitud,
                tipo_alerta=tipo_alerta, 
                descripcion=descripcion, 
                prioridad=prioridad
            )
            self.logger.info(f"Alerta {tipo_alerta}, creada por {usuario_id} en ubicación ({latitud}, {longitud})")
            return response
        except Exception as ex:
            self.logger.error(f"Error al tratar de crear la alerta (websocket service): {ex}")
            return None

    def send_mqtt_message(self, topic: str, message_data: Dict, qos: int = 0) -> bool:
        """Enviar mensaje MQTT desde el handler de WebSocket"""
        if not self.mqtt_publisher:
            self.logger.warning("⚠️ No hay cliente MQTT publisher disponible en WebSocket handler")
            return False
            
        try:
            success = self.mqtt_publisher.publish_json(topic, message_data, qos)
            
            if success:
                self.logger.info(f"✅ Mensaje MQTT enviado desde WebSocket a topic: {topic}")
                return True
            else:
                self.logger.error(f"❌ Error enviando mensaje MQTT desde WebSocket a topic: {topic}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Error enviando mensaje MQTT desde WebSocket: {e}")
            return False
    
    def get_whatsapp_statistics(self) -> Dict[str, Any]:
        """Obtener estadísticas del procesador de WhatsApp"""
        stats = {
            "processed_messages": self.whatsapp_processed_count,
            "error_count": self.whatsapp_error_count,
            "error_rate": round(self.whatsapp_error_count / max(self.whatsapp_processed_count, 1) * 100, 2),
            "queue_size": self.whatsapp_queue.qsize(),
            "queue_max_size": self.whatsapp_queue.maxsize,
            "is_processing": self.is_processing
        }
        
        # Agregar estadísticas del handler de empresa si está disponible
        if self.empresa_handler:
            empresa_stats = self.empresa_handler.get_statistics()
            stats["empresa_handler"] = empresa_stats
        
        return stats
    
    def _send_mqtt_message(self, topic: str, message_data: Dict, qos: int = 0) -> bool:
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
                self.logger.critical(
                    "🚨 No hay cliente MQTT publisher: el hardware NO recibió el mensaje de %s",
                    topic
                )
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

        self._ensure_whatsapp_alert_activation(alert_data=alert_data)
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
  
    def _send_create_down_alarma(self,list_users: list, alert: Dict, data_user: Dict = {}) -> bool:
        """Crear notificación de alarma por WhatsApp"""
        if not self.whatsapp_service:
            self.logger.warning("⚠️ WhatsApp service no disponible")
            return False

        if not make_whatsapp_user(data_user).is_creator:
            return False
        try:
           # print(json.dumps(alert,indent=4))
            image = alert["image_alert"]
            alert_name = alert["nombre_alerta"]
            empresa = data_user["data"]["empresa"]
            recipients = []
            for item in list_users:
                body_text = f"¡Hola {item['nombre']}!.\nAlerta de {alert_name} {empresa}"
                data = {
                    "phone": item.get("numero", ""),
                    "body_text": body_text
                }
                recipients.append(data)
                
            buttons = [
                {
                    "id": "APAGAR ALARMA",
                    "title": "Apagar alarma"
                }
            ]
            
            self.whatsapp_service.send_bulk_button_message(
                header_type="image",
                header_content=image,
                buttons=buttons,
                footer_text="Sistema RESCUE",
                recipients=recipients,
                use_queue=True
            )
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error enviando notificación de alarma: {e}")
            return False
    def _send_create_active_user(self, list_users: list, alert: Dict, data_user: Dict = {}) -> bool:
        """Crear notificación de alarma por WhatsApp con botón 'Estoy disponible'.
        Defensivo: todos los campos opcionales con fallback. Si falta image_alert,
        usa header de texto. Si list_users vacío, salta con warning. NUNCA debe
        fallar silente — el botón es crítico para la UX."""
        if not self.whatsapp_service:
            self.logger.error("❌ _send_create_active_user: whatsapp_service no disponible")
            return False
        if not list_users:
            self.logger.warning("⚠️ _send_create_active_user: list_users vacío, nada que enviar")
            return False

        # Extraer todos los campos con fallback (nunca KeyError)
        data_create = alert.get("activacion_alerta") or {}
        creador_nombre = data_create.get("nombre") or alert.get("empresa_nombre") or "el equipo"
        image = alert.get("image_alert") or ""
        alert_name = alert.get("nombre_alerta") or alert.get("tipo_alerta") or "Alerta"
        cached_data = (data_user or {}).get("data") or {}
        empresa = (
            cached_data.get("empresa")
            or alert.get("empresa_nombre")
            or alert.get("empresa")
            or "la empresa"
        )
        if isinstance(empresa, dict):
            empresa = empresa.get("nombre") or empresa.get("name") or "la empresa"

        footer = f"Creada por {creador_nombre}\nEquipo RESCUE"
        recipients = []
        for item in list_users:
            phone = item.get("numero") or item.get("phone") or ""
            if not phone:
                self.logger.warning(f"⚠️ _send_create_active_user: usuario sin numero: {item}")
                continue
            nombre_raw = item.get("nombre") or item.get("name") or "Usuario"
            primer_nombre = str(nombre_raw).split()[0].upper() if str(nombre_raw).strip() else "USUARIO"
            body_text = f"¡Hola {primer_nombre}!.\nAlerta de {alert_name} en {empresa}."
            recipients.append({"phone": phone, "body_text": body_text})

        if not recipients:
            self.logger.warning("⚠️ _send_create_active_user: 0 destinatarios válidos tras filtrar")
            return False

        buttons = [{"id": "Activar_User", "title": "Estoy disponible"}]

        try:
            if image:
                result = self.whatsapp_service.send_bulk_button_message(
                    header_type="image",
                    header_content=image,
                    buttons=buttons,
                    footer_text=footer,
                    recipients=recipients,
                    use_queue=True,
                )
            else:
                # Fallback texto: si no hay image_alert, header textual
                result = self.whatsapp_service.send_bulk_button_message(
                    header_type="text",
                    header_content=f"ALERTA {str(alert_name).upper()}",
                    buttons=buttons,
                    footer_text=footer,
                    recipients=recipients,
                    use_queue=True,
                )
            self.logger.info(
                f"✅ Botón 'Estoy disponible' enviado a {len(recipients)} usuarios "
                f"(header={'image' if image else 'text'}, alert={alert_name})"
            )
            return bool(result) if result is not None else True
        except Exception as e:
            self.logger.error(
                f"❌ _send_create_active_user falló al enviar bulk_button_message: {e} "
                f"| recipients={len(recipients)}, image={'sí' if image else 'no'}"
            )
            return False
   
    def _intermediate_to_mqtt(self, topics, alert) -> Dict[str, Any]:
        """Publicar la activación a cada hardware. Devuelve un reporte para que
        el fallo se pueda ver arriba en lugar de perderse."""
        report = {"attempted": 0, "published": 0, "failed": []}
        try:
            #print(json.dumps(alert,indent=4))
            #enviar alerta a mqtt
            for topic in topics:
                topic = self.pattern_topic + "/" + topic
                report["attempted"] += 1
                message_hardware = self._select_data_hardware(alert=alert,topic=topic)
                if self._send_mqtt_message(message_data=message_hardware,topic=topic):
                    report["published"] += 1
                else:
                    report["failed"].append(topic)

            if report["failed"]:
                self.logger.error(
                    "❌ Activación MQTT incompleta: %s/%s publicados, fallaron %s",
                    report["published"], report["attempted"], report["failed"]
                )

        except Exception as ex:
            self.logger.error(f"Error en el intermedario a enviar mensajes al mqtt {ex}")

        return report

    def _select_data_hardware(self, topic, alert:Dict) -> Dict:
        """Seleccionar datos específicos según el tipo de hardware"""
        data_alert = alert.get("data", {}) or {}
        alarm_color = (
            data_alert.get("tipo_alarma")
            or alert.get("nombre_alerta")
            or ""
        )
        if "SEMAFORO" in topic:
            message_data = {
                "tipo_alarma": alarm_color,
            }
        elif "PANTALLA" in topic:
            if str(alert.get("tipo_alarma") or alarm_color).upper() == "NORMAL":
                return {
                    "tipo_alarma": "NORMAL",
                    "prioridad": alert.get("prioridad","").upper()
                }
            self._ensure_whatsapp_alert_activation(alert_data=alert)
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

    def _send_deactivation_to_mqtt(self, topics: list, prioridad: str) -> Dict[str, Any]:
        """Enviar comandos de desactivación MQTT a dispositivos hardware.
        Devuelve un reporte con lo publicado y lo fallido."""
        report = {"attempted": 0, "published": 0, "failed": []}
        try:
            self.logger.info(f"🔄 Enviando comandos de desactivación MQTT a {len(topics)} dispositivos")

            for topic in topics:
                # Agregar pattern_topic igual que en activación
                full_topic = self.pattern_topic + "/" + topic
                report["attempted"] += 1

                # Crear mensaje de desactivación según el tipo de dispositivo
                deactivation_message = self._create_deactivation_message(topic=topic, prioridad=prioridad)

                # Enviar mensaje MQTT con el topic completo
                success = self._send_mqtt_message(message_data=deactivation_message, topic=full_topic)

                if success:
                    report["published"] += 1
                    self.logger.info(f"✅ Dispositivo desactivado: {topic.split('/')[-1]}")
                else:
                    report["failed"].append(full_topic)
                    self.logger.error(f"❌ Error desactivando dispositivo: {topic}")

            if report["failed"]:
                self.logger.error(
                    "❌ Desactivación MQTT incompleta: %s/%s publicados, fallaron %s",
                    report["published"], report["attempted"], report["failed"]
                )

        except Exception as ex:
            self.logger.error(f"❌ Error enviando comandos de desactivación MQTT: {ex}")

        return report
    
    def _create_deactivation_message(self, topic: str, prioridad: str) -> Dict:
        """Crear mensaje de desactivación específico según el tipo de dispositivo"""
        if "SEMAFORO" in topic:
            # Para semáforos: solo tipo_alarma NORMAL
            return {
                "tipo_alarma": "NORMAL"
            }
        elif "PANTALLA" in topic:
            # Para televisores: tipo_alarma NORMAL + prioridad del response
            return {
                "tipo_alarma": "NORMAL",
                "prioridad": prioridad.upper()
            }
        else:
            # Para dispositivos genéricos
            return {
                "tipo_alarma": "NORMAL",
                "action": "deactivate"
            }

    def trigger_fanout(self, alert_data: Dict, alert_managers: List[Dict] = None) -> bool:
        """Disparar fanout MQTT con datos de alerta - llamado via HTTP desde RescueBack"""
        try:
            if not self.empresa_handler:
                self.logger.error("❌ Empresa handler no disponible para fanout")
                return False

            empresa_message = {
                "type": "alert_created_by_empresa",
                "timestamp": alert_data.get("fecha_creacion", ""),
                "alert": alert_data,
                "alert_managers": alert_managers or []
            }

            success = self.empresa_handler.process_empresa_activation(empresa_message)
            if success:
                self.logger.info("✅ Fanout MQTT disparado via HTTP")
            else:
                self.logger.error("❌ Error en fanout MQTT via HTTP")
            return success
        except Exception as e:
            self.logger.error(f"❌ Error en trigger_fanout: {e}")
            return False

    def trigger_alert_event(self, message_data: Dict) -> bool:
        """Procesar un evento de alerta llegado por HTTP interno desde RescueBack.

        Hoy se usa para la desactivación (`alert_deactivated_by_empresa`), que
        antes dependía de que el navegador mandara el mensaje por WebSocket.
        """
        try:
            self.logger.info(
                "🌐 Evento de alerta recibido por HTTP interno: %s",
                message_data.get("type", "desconocido")
            )
            return self._handle_empresa_message_sync(message_data)
        except Exception as e:
            self.logger.error(f"❌ Error en trigger_alert_event: {e}")
            return False

    def _handle_create_empresa_alert_sync(self, message_data: Dict) -> bool:
        """Manejar mensaje de creación de alerta de empresa con alert_data incluido"""
        try:
            self.logger.info("🏢 Procesando comando de crear alerta de empresa")
            
            # Validar campos requeridos en el nuevo formato
            required_fields = ["type", "alert_data"]
            for field in required_fields:
                if field not in message_data:
                    self.logger.error(f"❌ Campo requerido faltante: {field}")
                    return False
            
            # Extraer alert_data completo del mensaje
            alert_data = message_data["alert_data"]
            
            # Validar que alert_data tenga los campos mínimos necesarios
            required_alert_fields = ["_id", "tipo_alerta", "descripcion"]
            for field in required_alert_fields:
                if field not in alert_data:
                    self.logger.error(f"❌ Campo requerido en alert_data faltante: {field}")
                    return False
            
            # Extraer información básica para logging
            alert_id = alert_data.get("_id", "N/A")
            tipo_alerta = alert_data.get("tipo_alerta", "N/A")
            descripcion = alert_data.get("descripcion", "N/A")
            prioridad = alert_data.get("prioridad", "media")
            empresa_nombre = alert_data.get("empresa_nombre", "N/A")
            sede = alert_data.get("sede", "N/A")
            
            self.logger.info(f"📋 Procesando alerta de empresa recibida:")
            self.logger.info(f"   🆔 Alert ID: {alert_id}")
            self.logger.info(f"   🏢 Empresa: {empresa_nombre}")
            self.logger.info(f"   🏛️ Sede: {sede}")
            self.logger.info(f"   🔔 Tipo: {tipo_alerta}")
            self.logger.info(f"   📝 Descripción: {descripcion}")
            self.logger.info(f"   ⚡ Prioridad: {prioridad}")
            
            # Ya no es necesario crear la alerta en el backend, solo procesarla
            self.logger.info(f"✅ Alert data recibido completo, procesando con empresa handler...")
            
            # Crear estructura compatible con empresa handler
            empresa_message = {
                "type": "alert_created_by_empresa",
                "timestamp": alert_data.get("fecha_creacion", ""),
                "alert": alert_data,
                "alert_managers": message_data.get("alert_managers", [])
            }
            
            # Procesar con el empresa handler
            if not self.empresa_handler:
                self.logger.error("❌ Empresa handler no disponible")
                return False
            
            success = self.empresa_handler.process_empresa_activation(empresa_message)
            
            if success:
                self.whatsapp_processed_count += 1
                self.logger.info("✅ Alerta de empresa procesada exitosamente")
            else:
                self.whatsapp_error_count += 1
                self.logger.error("❌ Error procesando alerta con empresa handler")
                
            return success
            
        except Exception as e:
            self.logger.error(f"❌ Error manejando creación de alerta de empresa: {e}")
            self.whatsapp_error_count += 1
            return False
    
    def _handle_empresa_message_sync(self, message_data: Dict) -> bool:
        """Manejar mensaje de desactivación por empresa (versión síncrona para Redis)"""
        try:
            if not self.empresa_handler:
                self.logger.error("❌ Empresa handler no disponible")
                return False
            
            # Procesar con el handler específico de empresa (ahora maneja activación y desactivación)
            success = self.empresa_handler.process_empresa_alert(message_data)
            
            if success:
                self.whatsapp_processed_count += 1
                self.logger.info("✅ Mensaje de empresa procesado exitosamente")
            else:
                self.whatsapp_error_count += 1
                self.logger.error("❌ Error procesando mensaje de empresa")
                
            return success
            
        except Exception as e:
            self.logger.error(f"❌ Error manejando mensaje de empresa: {e}")
            self.whatsapp_error_count += 1
            return False


    async def stop_whatsapp_processing(self):
        """Detener el procesamiento de la cola de WhatsApp"""
        # Detener workers de Redis si existen
        if self.redis_queue:
            self.redis_queue.stop_workers()
        
        # Detener empresa handler si existe
        if self.empresa_handler:
            self.empresa_handler.stop()
        
        # Detener cola en memoria si existe
        if hasattr(self, '_queue_task') and self._queue_task and not self._queue_task.done():
            self._queue_task.cancel()
            try:
                await self._queue_task
            except asyncio.CancelledError:
                pass
        
        if hasattr(self, 'is_processing'):
            self.is_processing = False
        
        self.logger.info("🛑 Procesador de WhatsApp detenido")

    async def clear_whatsapp_queue(self):
        """Limpiar la cola de WhatsApp"""
        cleared_count = 0
        while not self.whatsapp_queue.empty():
            try:
                self.whatsapp_queue.get_nowait()
                self.whatsapp_queue.task_done()
                cleared_count += 1
            except asyncio.QueueEmpty:
                break
        
        self.logger.info(f"🗑️ Cola de WhatsApp limpiada: {cleared_count} mensajes eliminados")
        return cleared_count
    
    def _send_alarm_deactivation_success_message(self, phone: str, user: str, response: Dict) -> bool:
        """Enviar mensaje personalizado de éxito en desactivación de alarma usando send_bulk_individual"""
        try:
            if not self.whatsapp_service:
                return False
            
            # Obtener información adicional de la respuesta
            fecha_desactivacion = response.get('desactivado_por', {}).get('fecha_desactivacion', '')
            numeros_telefonicos = response.get('numeros_telefonicos', [])
            
            # Preparar mensajes personalizados para todos los usuarios
            recipients = []
            for contact in numeros_telefonicos:
                phone_number = contact.get('numero', '')
                nombre = contact.get('nombre', '')
                
                # Determinar si es quien desactivó la alarma o es otra persona
                if phone_number == phone:
                    # Mensaje para quien desactivó
                    success_message = f"¡Perfecto {nombre.split()[0].upper()}!"
                    success_message += f"\n\nAlarma desactivada exitosamente"
                    success_message += f"\nFecha: {fecha_desactivacion.split('T')[0] if fecha_desactivacion else 'Ahora'}"
                    success_message += f"\n\nEl sistema RESCUE ha registrado tu acción"
                    success_message += f"\n¡Gracias por mantener la seguridad!"
                    success_message += f"\n\nSistema RESCUE - Siempre Vigilante"
                else:
                    # Mensaje para otros usuarios
                    success_message = f"¡Hola {nombre.split()[0].upper()}!"
                    success_message += f"\n\nLa alarma ha sido DESACTIVADA"
                    success_message += f"\nDesactivada por: {user}"
                    success_message += f"\nMomento: Ahora mismo"
                    success_message += f"\n\nEl sistema vuelve a estar en estado normal"
                    success_message += f"\nEQUIPO RESCUE"
                
                recipients.append({
                    "phone": phone_number,
                    "message": success_message
                })
            
            # Enviar usando el cliente de WhatsApp para envío masivo
            if recipients:
                response_bulk = self.whatsapp_service.send_bulk_individual(
                    recipients=recipients,
                    use_queue=True
                )
                
                if response_bulk:
                    self.logger.info(f"✅ Notificación de desactivación exitosa enviada a {len(recipients)} usuarios")
                    return True
                else:
                    self.logger.error(f"❌ Error enviando notificación masiva de desactivación exitosa")
                    # Fallback al método individual
                    if self.whatsapp_service:
                        simple_message = f"✅ Alarma desactivada exitosamente por {user}.\n\nGracias por usar el Sistema RESCUE 🚨"
                        self.whatsapp_service.send_individual_message(phone=phone, message=simple_message)
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error enviando mensaje de éxito de desactivación: {e}")
            return False
    
    def _send_alarm_deactivation_error_message(self, phone: str, user: str, response: Optional[Dict]) -> bool:
        """Enviar mensaje personalizado de error en desactivación de alarma"""
        try:
            if not self.whatsapp_service:
                return False
            
            # Siempre enviar mensaje de alarma ya desactivada (sin importar el tipo de error)
            error_message = f"¡Hola {user.split()[0].upper()}!"
            error_message += f"\n\nℹEsta alarma ya fue DESACTIVADA anteriormente"
            
            # Obtener información de quién la desactivó si está disponible
            if response and response.get('desactivado_por', {}):
                desactivado_por = response.get('desactivado_por', {})
                fecha = desactivado_por.get('fecha_desactivacion', '')
                fecha_formato = fecha.split('T')[0] if fecha else 'Fecha desconocida'
                error_message += f"\n\nDesactivada el: {fecha_formato}"
                error_message += f"\nPor otro usuario del sistema"
            
            error_message += f"\n\nESTADO: La alarma ya está INACTIVA"
            error_message += f"\nNo hay riesgo activo en el sistema"
            error_message += f"\n\nGracias por tu atención a la seguridad"
            
            error_message += f"\n\nEQUIPO RESCUE"
            
            self.whatsapp_service.send_individual_message(
                phone=phone,
                message=error_message
            )
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error enviando mensaje de error de desactivación: {e}")
            return False
    
    def _send_bulk_deactivation_notification(self, numeros_telefonicos: list, deactivated_by: str, response: Dict, exclude_phone: str) -> bool:
        """Enviar notificación masiva a otros usuarios sobre desactivación de alarma"""
        try:
            # Preparar lista de destinatarios (excluir quien desactivó)
            recipients = []
            for contact in numeros_telefonicos:
                phone = contact.get('numero', '')
                nombre = contact.get('nombre', '')
                
                # Excluir el número de quien desactivó la alarma
                if phone != exclude_phone:
                    notification_message = f"ℹ¡Hola {nombre.split()[0].upper()}!"
                    notification_message += f"\n\nLa alarma ha sido DESACTIVADA"
                    notification_message += f"\nDesactivada por: {deactivated_by}"
                    notification_message += f"\nMomento: Ahora mismo"
                    notification_message += f"\n\nEl sistema vuelve a estar en estado normal"
                    notification_message += f"\nEQUIPO RESCUE"
                    
                    recipients.append({
                        "phone": phone,
                        "message": notification_message
                    })
            
            # Enviar usando el cliente de WhatsApp para envío masivo si hay destinatarios
            if recipients:
                response = self.whatsapp_service.send_bulk_individual(
                    recipients=recipients,
                    use_queue=True
                )
                
                if response:
                    self.logger.info(f"✅ Notificación de desactivación enviada a {len(recipients)} usuarios")
                    return True
                else:
                    self.logger.error(f"❌ Error enviando notificación masiva de desactivación")
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error enviando notificación masiva de desactivación: {e}")
            return False
    
    def _send_location_personalized_message(self, numeros_data: list, tipo_alarma_info: Dict) -> bool:
        """Enviar ubicación por WhatsApp usando CTA 'Abrir en Maps'"""

        if not self.whatsapp_service:
            self.logger.warning("⚠️ WhatsApp service no disponible")
            return False

        try:
            ubicacion = tipo_alarma_info.get("ubicacion", {}) if tipo_alarma_info else {}
            url_maps = ubicacion.get("url_maps") if isinstance(ubicacion, dict) else None

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
