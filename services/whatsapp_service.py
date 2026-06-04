"""
Servicio de WhatsApp para envío de mensajes
"""
import logging
import time
from typing import Dict, Any, Optional, List
from clients.whatsapp_client import WhatsAppClient
from config.settings import WhatsAppConfig


class WhatsAppService:
    """
    Servicio para envío de mensajes WhatsApp que se integra con la arquitectura existente
    """
    
    def __init__(self, config: WhatsAppConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Crear cliente WhatsApp
        self.client = WhatsAppClient(config)
        
        # Estadísticas del servicio
        self.stats = {
            "start_time": time.time(),
            "individual_messages_sent": 0,
            "broadcast_messages_sent": 0,
            "total_recipients": 0,
            "errors": 0
        }
    def send_location_request(self,phone:str,body_text:str) -> bool:
        try:
            """
            Enviar mensaje individual de peticion de ubicacion
            Args:
            phone: Formato internacional
            body_text: texto para el envio
            """
            if not self.config.enabled:
                self.logger.warning("⚠️ Servicio WhatsApp deshabilitado")
                return False
            
            
            response = self.client.send_location_request(phone, body_text)
            
            if response:
                self.stats["individual_messages_sent"] += 1
                self.stats["total_recipients"] += 1
                
                self.logger.info(f"Mensaje individual de peticion de ubicacion enviado a {phone}")
                return True
            else:
                self.stats["errors"] += 1
                self.logger.error(f"Error enviando mensaje de peticion de ubicacion individual a {phone}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error en servicio WhatsApp: {e}")
    def send_individual_message(self, phone: str, message: str, use_queue: bool = False) -> bool:
        """
        Enviar mensaje individual de WhatsApp
        
        Args:
            phone: Número del destinatario (formato internacional)
            message: Texto del mensaje
            use_queue: Si usar cola o no
            
        Returns:
            bool: True si se envió exitosamente, False en caso contrario
        """
        
        try:
            if not self.config.enabled:
                self.logger.warning("⚠️ Servicio WhatsApp deshabilitado")
                return False
            
            
            response = self.client.send_individual_message(phone, message, use_queue)
            
            if response:
                self.stats["individual_messages_sent"] += 1
                self.stats["total_recipients"] += 1
                
                self.logger.info(f"Mensaje individual enviado a {phone}")
                return True
            else:
                self.stats["errors"] += 1
                self.logger.error(f"Error enviando mensaje individual a {phone}")
                return False
                
        except Exception as e:
            self.stats["errors"] += 1
            self.logger.error(f"Error en servicio WhatsApp: {e}")
            return False
    
    def send_bulk_individual(self, recipients: List[Dict], use_queue: bool = True) -> bool:
        """
        Enviar mensajes individuales masivos usando el endpoint send-bulk
        
        Args:
            recipients: Lista de diccionarios con 'phone' y 'message'
            use_queue: Si usar cola o no (opcional, por defecto True)
            
        Returns:
            bool: True si se envió exitosamente, False en caso contrario
        """
        try:
            if not self.config.enabled:
                self.logger.warning("⚠️ Servicio WhatsApp deshabilitado")
                return False
            
            
            response = self.client.send_bulk_individual(
                recipients=recipients,
                use_queue=use_queue
            )
            
            if response:
                sent_count = response.get('sent_count', len(recipients))
                self.stats["individual_messages_sent"] += sent_count
                self.stats["total_recipients"] += len(recipients)
                
                self.logger.info(f"Mensajes masivos individuales enviados a {len(recipients)} destinatarios")
                return True
            else:
                self.stats["errors"] += 1
                self.logger.error(f"Error enviando mensajes masivos individuales a {len(recipients)} destinatarios")
                return False
                
        except Exception as e:
            self.stats["errors"] += 1
            self.logger.error(f"Error en servicio WhatsApp masivo individual: {e}")
            return False
    
    def send_broadcast_message(self, phones: List[str], header_type: str, header_content: str,
                             body_text: str, button_text: str, button_url: str,
                             footer_text: str, use_queue: bool = True) -> bool:
        """
        Enviar mensaje broadcast interactivo de WhatsApp
        
        Args:
            phones: Lista de números destinatarios
            header_type: Tipo de encabezado
            header_content: Texto del encabezado
            body_text: Texto principal del mensaje
            button_text: Texto del botón
            button_url: URL del botón
            footer_text: Texto de pie de página
            use_queue: Si usar cola o no
            
        Returns:
            bool: True si se envió exitosamente, False en caso contrario
        """
        try:
            if not self.config.enabled:
                self.logger.warning("⚠️ Servicio WhatsApp deshabilitado")
                return False
            
            
            response = self.client.send_broadcast_message(
                phones=phones,
                header_type=header_type,
                header_content=header_content,
                body_text=body_text,
                button_text=button_text,
                button_url=button_url,
                footer_text=footer_text,
                use_queue=use_queue
            )
            
            if response:
                self.stats["broadcast_messages_sent"] += 1
                self.stats["total_recipients"] += len(phones)
                
                self.logger.info(f"Broadcast enviado a {len(phones)} números")
                return True
            else:
                self.stats["errors"] += 1
                self.logger.error(f"Error enviando broadcast a {len(phones)} números")
                return False
                
        except Exception as e:
            self.stats["errors"] += 1
            self.logger.error(f"Error en servicio WhatsApp broadcast: {e}")
            return False
    
    def send_personalized_broadcast(self, recipients: List[Dict], header_type: str, header_content: str,
                                   button_text: str, button_url: str, footer_text: str, use_queue: bool = True) -> bool:
        """
        Enviar mensaje broadcast personalizado de WhatsApp
        
        Args:
            recipients: Lista de diccionarios con 'phone' y 'body_text'
            header_type: Tipo de encabezado
            header_content: Contenido del encabezado
            button_text: Texto del botón
            button_url: URL del botón
            footer_text: Texto de pie de página
            use_queue: Si usar cola o no
            
        Returns:
            bool: True si se envió exitosamente, False en caso contrario
        """
        try:
            if not self.config.enabled:
                self.logger.warning("⚠️ Servicio WhatsApp deshabilitado")
                return False
            
            
            response = self.client.send_personalized_broadcast(
                recipients=recipients,
                header_type=header_type,
                header_content=header_content,
                button_text=button_text,
                button_url=button_url,
                footer_text=footer_text,
                use_queue=use_queue
            )
            
            if response:
                self.stats["broadcast_messages_sent"] += 1
                self.stats["total_recipients"] += len(recipients)
                
                self.logger.info(f"Broadcast personalizado enviado a {len(recipients)} destinatarios")
                return True
            else:
                self.stats["errors"] += 1
                self.logger.error(f"Error enviando broadcast personalizado a {len(recipients)} destinatarios")
                return False
                
        except Exception as e:
            self.stats["errors"] += 1
            self.logger.error(f"Error en servicio WhatsApp broadcast personalizado: {e}")
            return False
    
    def send_list_message(self, phone: str, header_text: str, body_text: str, 
                         footer_text: str, button_text: str, sections: List[Dict]) -> bool:
        """
        Enviar mensaje de lista de WhatsApp
        
        Args:
            phone: Número del destinatario (formato internacional)
            header_text: Texto del encabezado
            body_text: Texto principal del mensaje
            footer_text: Texto de pie de página
            button_text: Texto del botón
            sections: Lista de secciones con sus opciones
            
        Returns:
            bool: True si se envió exitosamente, False en caso contrario
        """
        try:
            if not self.config.enabled:
                self.logger.warning("⚠️ Servicio WhatsApp deshabilitado")
                return False
            
            
            response = self.client.send_list_message(
                phone=phone,
                header_text=header_text,
                body_text=body_text,
                footer_text=footer_text,
                button_text=button_text,
                sections=sections
            )
            
            if response:
                self.stats["individual_messages_sent"] += 1
                self.stats["total_recipients"] += 1
                
                self.logger.info(f"Mensaje de lista enviado a {phone}")
                return True
            else:
                self.stats["errors"] += 1
                self.logger.error(f"Error enviando mensaje de lista a {phone}")
                return False
                
        except Exception as e:
            self.stats["errors"] += 1
            self.logger.error(f"Error en servicio WhatsApp enviando lista: {e}")
            return False
    
    def send_bulk_list_message(self, header_text: str, footer_text: str, button_text: str, 
                              sections: List[Dict], recipients: List[Dict], use_queue: bool = True) -> bool:
        """
        Enviar mensaje de lista de manera masiva a múltiples destinatarios
        
        Args:
            header_text: Texto del encabezado (común para todos)
            footer_text: Texto de pie de página (común para todos)
            button_text: Texto del botón (común para todos)
            sections: Lista de secciones con sus opciones (común para todos)
            recipients: Lista de diccionarios con 'phone' y 'body_text' personalizado
            use_queue: Si usar cola o no (default True)
            
        Returns:
            bool: True si se envió exitosamente, False en caso contrario
        """
        try:
            if not self.config.enabled:
                self.logger.warning("⚠️ Servicio WhatsApp deshabilitado")
                return False
            
            
            response = self.client.send_bulk_list_message(
                header_text=header_text,
                footer_text=footer_text,
                button_text=button_text,
                sections=sections,
                recipients=recipients,
                use_queue=use_queue
            )
            
            if response:
                # Actualizar estadísticas - consideramos bulk list como un broadcast
                self.stats["broadcast_messages_sent"] += 1
                self.stats["total_recipients"] += len(recipients)
                
                self.logger.info(f"Bulk list enviado a {len(recipients)} destinatarios")
                return True
            else:
                self.stats["errors"] += 1
                self.logger.error(f"Error enviando bulk list a {len(recipients)} destinatarios")
                return False
                
        except Exception as e:
            self.stats["errors"] += 1
            self.logger.error(f"Error en servicio WhatsApp bulk list: {e}")
            return False
    
    def send_bulk_button_message(self, header_type: str, header_content: str, buttons: List[Dict], 
                                footer_text: str, recipients: List[Dict], use_queue: bool = True) -> bool:
        """
        Enviar mensaje con botones de manera masiva a múltiples destinatarios

        Args:
            header_type: Tipo de encabezado (e.g., 'text')
            header_content: Contenido del encabezado (común para todos)
            buttons: Lista de botones con 'id' y 'title' (común para todos)
            footer_text: Texto de pie de página (común para todos)
            recipients: Lista de diccionarios con 'phone' y 'body_text' personalizado
            use_queue: Si usar cola o no (default True)
            
        Returns:
            bool: True si se envió exitosamente, False en caso contrario
        """
        try:
            if not self.config.enabled:
                self.logger.warning("⚠️ Servicio WhatsApp deshabilitado")
                return False
            
            
            response = self.client.send_bulk_button_message(
                header_type=header_type,
                header_content=header_content,
                buttons=buttons,
                footer_text=footer_text,
                recipients=recipients,
                use_queue=use_queue
            )
            
            if response:
                # Actualizar estadísticas - consideramos bulk button como un broadcast
                self.stats["broadcast_messages_sent"] += 1
                self.stats["total_recipients"] += len(recipients)
                
                self.logger.info(f"Bulk button enviado a {len(recipients)} destinatarios")
                return True
            else:
                self.stats["errors"] += 1
                self.logger.error(f"Error enviando bulk button a {len(recipients)} destinatarios")
                return False
                
        except Exception as e:
            self.stats["errors"] += 1
            self.logger.error(f"Error en servicio WhatsApp bulk button: {e}")
            return False

    def send_bulk_location_button_message(
        self,
        recipients: List[Dict],
        url_maps: str,
        footer_text: str = "",
        use_queue: bool = True
    ) -> bool:
        """Enviar ubicación mediante CTA 'Abrir en Maps' usando broadcast personalizado"""

        if not self.config.enabled:
            self.logger.warning("⚠️ Servicio WhatsApp deshabilitado")
            return False

        if not url_maps:
            self.logger.warning("⚠️ No se puede enviar ubicación: falta URL de Maps")
            return False

        try:
            header_type = "text"
            header_content = "¡RESCUE SYSTEM UBICACIÓN!"

            enriched_recipients = []
            for recipient in recipients:
                name = (recipient.get("nombre") or recipient.get("name") or "").strip()
                phone = recipient.get("phone") or recipient.get("numero")

                if not phone:
                    self.logger.debug("⚠️ Se omite destinatario sin número válido en ubicación")
                    continue

                body_lines = []
                if name:
                    body_lines.append(f"¡HOLA {name}!")
                else:
                    body_lines.append("¡HOLA!")
                body_lines.append("Rescue te ayuda a llegar a la emergencia.")

                body_text = "\n".join(body_lines)

                enriched_recipients.append({
                    "phone": phone,
                    "body_text": body_text
                })

            if not enriched_recipients:
                self.logger.warning("⚠️ No hay destinatarios válidos para enviar ubicación")
                return False

            response = self.client.send_personalized_broadcast_message(
                recipients=enriched_recipients,
                header_type=header_type,
                header_content=header_content,
                button_text="Abrir en Maps",
                button_url=url_maps,
                footer_text=footer_text,
                use_queue=use_queue
            )

            if response:
                self.stats["broadcast_messages_sent"] += 1
                self.stats["total_recipients"] += len(enriched_recipients)
                self.logger.info(
                    f"✅ Mensaje de ubicación enviado a {len(enriched_recipients)} destinatarios"
                )
                return True

            self.stats["errors"] += 1
            self.logger.error("❌ Error enviando mensaje de ubicación con CTA")
            return False

        except Exception as e:
            self.stats["errors"] += 1
            self.logger.error(f"❌ Error en envío de ubicación con CTA: {e}")
            return False
    
    def find_numbers_by_alert(self, alert_id: str, manager_only: bool = False) -> List[Dict]:
        """Devuelve los números con foco en una alerta dada (consulta WhatsApp HTTP API)"""
        try:
            if not self.config.enabled:
                return []
            endpoint = f"/api/numbers/by-alert/{alert_id}"
            params = {"manager_only": "true"} if manager_only else None
            response = self.client._make_request("GET", endpoint, params=params)
            if isinstance(response, dict) and response.get("success"):
                return response.get("numbers", []) or []
            return []
        except Exception as e:
            self.logger.error(f"Error en find_numbers_by_alert: {e}")
            return []

    def add_number_to_cache(self, phone: str, name: str = None, data: Dict = None, empresa_id: str = None) -> bool:
        """
        Agregar número al cache de WhatsApp

        Args:
            phone: Número de teléfono (formato internacional)
            name: Nombre del contacto
            data: Datos adicionales del contacto
            empresa_id: Identificador de la empresa al mismo nivel que phone
            
        Returns:
            bool: True si se agregó exitosamente, False en caso contrario
        """
        try:
            if not self.config.enabled:
                self.logger.warning("⚠️ Servicio WhatsApp deshabilitado")
                return False
            
            
            response = self.client.add_number_to_cache(phone, name, data, empresa_id=empresa_id)
            
            if response:
                self.logger.info(f"Número {phone} agregado al cache")
                return True
            else:
                self.logger.error(f"Error agregando número {phone} al cache")
                return False
                
        except Exception as e:
            self.logger.error(f"Error en servicio WhatsApp agregando al cache: {e}")
            return False
    
    def update_number_cache(self, phone: str, data: Dict, empresa_id: str = None) -> bool:
        """
        Actualizar información del cache de un usuario de WhatsApp
        
        Args:
            phone: Número de teléfono (formato internacional)
            data: Datos a actualizar en el cache (ej: {"email": "nuevo@email.com", "company": "Nueva Empresa"})
            empresa_id: Identificador de la empresa a mantener en la entrada
            
        Returns:
            bool: True si se actualizó exitosamente, False en caso contrario
        """
        try:
            if not self.config.enabled:
                self.logger.warning("⚠️ Servicio WhatsApp deshabilitado")
                return False
            
            
            response = self.client.update_number_cache(phone, data, empresa_id=empresa_id)
            
            if response:
                self.logger.info(f"Cache del número {phone} actualizado con datos: {data}")
                return True
            else:
                self.logger.error(f"Error actualizando cache del número {phone}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error en servicio WhatsApp actualizando cache: {e}")
            return False

    def send_bulk_template(self, recipients: List[Dict], use_queue: bool = True) -> bool:
        """
        Enviar plantillas de WhatsApp de manera masiva con componentes personalizados
        
        Args:
            recipients: Lista de diccionarios, cada uno con:
                - phone: Número de teléfono (formato internacional)
                - template_name: Nombre de la plantilla
                - language: Idioma (ej: 'es', 'es_CO')
                - components: Lista de componentes de la plantilla (opcional)
                - parameters: Lista de parámetros simples (opcional, para compatibilidad)
            use_queue: Si usar cola o no (default True)
            
        Returns:
            bool: True si se envió exitosamente, False en caso contrario
            
        Example:
            recipients = [
                {
                    "phone": "573103391854",
                    "template_name": "location_alert",
                    "language": "es_CO",
                    "components": [
                        {
                            "type": "body",
                            "parameters": [
                                {"type": "text", "text": "Juan Pérez"}
                            ]
                        },
                        {
                            "type": "button",
                            "sub_type": "url",
                            "index": "0",
                            "parameters": [
                                {"type": "text", "text": "param_url_usuario1"}
                            ]
                        }
                    ]
                }
            ]
        """
        try:
            if not self.config.enabled:
                self.logger.warning("⚠️ Servicio WhatsApp deshabilitado")
                return False
            
            
            response = self.client.send_bulk_template(
                recipients=recipients,
                use_queue=use_queue
            )
            
            if response:
                # Actualizar estadísticas - consideramos bulk template como broadcast
                self.stats["broadcast_messages_sent"] += 1
                self.stats["total_recipients"] += len(recipients)
                
                self.logger.info(f"Bulk template enviado a {len(recipients)} destinatarios")
                return True
            else:
                self.stats["errors"] += 1
                self.logger.error(f"Error enviando bulk template a {len(recipients)} destinatarios")
                return False
                
        except Exception as e:
            self.stats["errors"] += 1
            self.logger.error(f"Error en servicio WhatsApp bulk template: {e}")
            return False
    
    def bulk_update_numbers(self, phones: List[str], data: Dict) -> bool:
        """
        Actualizar información de múltiples números de WhatsApp de forma masiva
        
        Args:
            phones: Lista de números de teléfono (formato internacional)
            data: Datos a actualizar para todos los números (ej: {"status": "active", "campaign": "summer_2024"})
            
        Returns:
            bool: True si se actualizó exitosamente, False en caso contrario
        """
        try:
            if not self.config.enabled:
                self.logger.warning("⚠️ Servicio WhatsApp deshabilitado")
                return False
            
            
            response = self.client.bulk_update_numbers(phones, data)
            
            if response:
                updated_count = response.get('updated_count', len(phones))
                self.logger.info(f"Actualización masiva completada: {updated_count}/{len(phones)} números actualizados con datos: {data}")
                return True
            else:
                self.logger.error(f"Error en actualización masiva de {len(phones)} números")
                return False
                
        except Exception as e:
            self.logger.error(f"Error en servicio WhatsApp actualización masiva: {e}")
            return False
    
    def process_whatsapp_notification(self, notification: Dict[str, Any]) -> bool:
        """
        Procesar notificación de WhatsApp desde el backend
        
        Args:
            notification: Diccionario con datos de la notificación
            
        Returns:
            bool: True si se procesó exitosamente
        """
        try:
            message_type = notification.get('type', 'individual')
            
            if message_type == 'individual':
                return self._process_individual_notification(notification)
            elif message_type == 'broadcast':
                return self._process_broadcast_notification(notification)
            else:
                return False
                
        except Exception as e:
            self.logger.error(f"Error procesando notificación WhatsApp: {e}")
            return False
    
    def _process_individual_notification(self, notification: Dict[str, Any]) -> bool:
        """Procesar notificación individual"""
        try:
            phone = notification.get('phone')
            message = notification.get('message')
            use_queue = notification.get('use_queue', False)
            
            if not phone or not message:
                return False
            
            return self.send_individual_message(phone, message, use_queue)
            
        except Exception as e:
            return False
    
    def _process_broadcast_notification(self, notification: Dict[str, Any]) -> bool:
        """Procesar notificación broadcast"""
        try:
            phones = notification.get('phones', [])
            header_type = notification.get('header_type', 'text')
            header_content = notification.get('header_content', '')
            body_text = notification.get('body_text', '')
            button_text = notification.get('button_text', '')
            button_url = notification.get('button_url', '')
            footer_text = notification.get('footer_text', '')
            use_queue = notification.get('use_queue', True)
            
            if not phones or not body_text:
                return False
            
            return self.send_broadcast_message(
                phones=phones,
                header_type=header_type,
                header_content=header_content,
                body_text=body_text,
                button_text=button_text,
                button_url=button_url,
                footer_text=footer_text,
                use_queue=use_queue
            )
            
        except Exception as e:
            return False
    
    def health_check(self) -> bool:
        """Verificar estado del servicio WhatsApp"""
        try:
            if not self.config.enabled:
                return False
            
            return self.client.health_check()
            
        except Exception as e:
            self.logger.error(f"Error en health check WhatsApp: {e}")
            return False
    
    def get_status(self) -> Dict[str, Any]:
        """Obtener estado completo del servicio"""
        try:
            uptime = time.time() - self.stats["start_time"]
            client_status = self.client.get_status()
            
            return {
                "service": {
                    "enabled": self.config.enabled,
                    "uptime_seconds": round(uptime, 2),
                    "individual_messages_sent": self.stats["individual_messages_sent"],
                    "broadcast_messages_sent": self.stats["broadcast_messages_sent"],
                    "total_recipients": self.stats["total_recipients"],
                    "errors": self.stats["errors"],
                    "success_rate": self._calculate_success_rate()
                },
                "client": client_status
            }
            
        except Exception as e:
            return {
                "service": {
                    "enabled": False,
                    "error": str(e)
                },
                "client": {
                    "healthy": False,
                    "error": str(e)
                }
            }
    
    def _calculate_success_rate(self) -> float:
        """Calcular tasa de éxito"""
        total_attempts = (
            self.stats["individual_messages_sent"] + 
            self.stats["broadcast_messages_sent"] + 
            self.stats["errors"]
        )
        
        if total_attempts == 0:
            return 100.0
        
        successful = self.stats["individual_messages_sent"] + self.stats["broadcast_messages_sent"]
        return round((successful / total_attempts) * 100, 2)
    
    def get_simple_status(self) -> Dict[str, Any]:
        """Obtener estado simple del servicio"""
        status = self.get_status()
        return {
            "enabled": status["service"]["enabled"],
            "healthy": status["client"]["healthy"],
            "messages_sent": status["service"]["individual_messages_sent"] + status["service"]["broadcast_messages_sent"],
            "success_rate": status["service"]["success_rate"]
        }
