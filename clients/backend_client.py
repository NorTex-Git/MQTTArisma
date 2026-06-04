"""
Cliente para comunicación con el backend
"""
import json
import logging
import threading
from typing import Dict, Any, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from config.settings import BackendConfig


class BackendClient:
    """Cliente para comunicación con el backend"""

    def __init__(self, config: BackendConfig):

        self.config = config
        self.session = requests.Session()
        self._auth_header_lock = threading.Lock()
        self.logger = logging.getLogger(__name__)
        
        # Configurar reintentos automáticos
        retry_strategy = Retry(
            total=config.retry_attempts,
            backoff_factor=config.retry_delay,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Configurar headers por defecto
        self.session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': 'MQTT-Backend-Client/1.0'
        })
        
        # Configurar API Key si está disponible
        if config.api_key:
            self.session.headers.update({
                'Authorization': f'Bearer {config.api_key}'
            })
    
    def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None, 
                     params: Optional[Dict] = None) -> Optional[Dict]:
        """Realizar petición HTTP con manejo de errores"""
        url = f"{self.config.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        
        try:
            response = self.session.request(
                method=method,
                url=url,
                json=data,
                params=params,
                timeout=self.config.timeout
            )
            
            # Manejar códigos de estado específicos sin lanzar excepción
            if response.status_code == 404:
                self.logger.info(f"🔍 Recurso no encontrado (404): {method} {url}")
                try:
                    result = response.json()
                    result['_status_code'] = 404
                    return result
                except json.JSONDecodeError:
                    return {'_status_code': 404, 'raw_response': response.text, 'message': 'Recurso no encontrado'}
            
            elif response.status_code == 401:
                self.logger.info(f"🔒 No autorizado (401): {method} {url}")
                try:
                    result = response.json()
                    result['_status_code'] = 401
                    return result
                except json.JSONDecodeError:
                    return {'_status_code': 401, 'raw_response': response.text, 'message': 'No autorizado'}
            
            # Para otros códigos de error (500, 502, 503, etc.), usar raise_for_status
            # que activará los reintentos automáticos configurados
            elif response.status_code >= 500:
                response.raise_for_status()  # Esto lanzará excepción y activará reintentos
            
            # Para códigos de éxito (2xx) y otros códigos (3xx, 4xx excepto 401/404)
            # intentar parsear JSON normalmente
            try:
                result = response.json()
                if isinstance(result, list):
                    return {'success': bool(result), 'data': result, '_status_code': response.status_code}
                result['_status_code'] = response.status_code
                return result
            except json.JSONDecodeError:
                return {'_status_code': response.status_code, 'raw_response': response.text}
                
        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ ERROR HTTP {method} {url}: {e}")
            if hasattr(e, 'response') and e.response is not None:
                self.logger.error(f"Código: {e.response.status_code}, Respuesta: {e.response.text}")
            return None
   
    def get(self,endpoint:str,data:Optional[Dict]= None) -> Optional[Dict]:
        """Realizar petición GET"""
        return self._make_request('GET',endpoint,params=data)
    def post(self, endpoint: str, data: Optional[Dict] = None) -> Optional[Dict]:
        """Realizar petición POST"""
        return self._make_request('POST', endpoint, data=data)
    def verify_user_number(self,number) -> Optional[Dict]:
        try:
            endpoint = "/api/phone-lookup"
            data = {
                "telefono":number
            }
            response = self.get(data=data,endpoint=endpoint)
            
            # Si no hay respuesta (error de conexión), return None
            if response is None:
                return None
            
            # Manejar códigos de estado específicos
            status_code = response.get('_status_code', 200)
            
            if status_code == 404:
                return response  # Devolver la respuesta para que se pueda manejar en el código que llama
            
            elif status_code == 401:
                return response  # Devolver la respuesta para que se pueda manejar en el código que llama
            
            # Para códigos de éxito, verificar el campo 'success'
            validate_response = response.get('success', False)
            if validate_response:
                return response
            else:
                return response  # Devolver la respuesta completa, no None
        
        except Exception as ex:
            self.logger.error(f"Error al tratar de verificar el numero: {ex}")
            return None
    def authenticate_hardware(self, mqtt_data: Dict[str, Any]) -> Optional[str]:
        """Autenticar hardware y obtener token"""
        try:
            # Extraer credenciales del mensaje MQTT
            auth_data = {
                'empresa': mqtt_data.get('empresa'), 
                'sede': mqtt_data.get('sede'),
                'hardware': mqtt_data.get('nombre_hardware'),
                'tipo_hardware': mqtt_data.get('tipo_hardware')
            }
            
            response = self.post('/api/hardware-auth/authenticate', data=auth_data)
            
            # Si no hay respuesta (error de conexión), return None
            if response is None:
                return None
            
            # Manejar códigos de estado específicos
            status_code = response.get('_status_code', 200)
            
            if status_code == 404:
                return None
            
            elif status_code == 401:
                return None
            
            # Para respuestas exitosas, verificar el token
            if response and response.get('success'):
                # Obtener token de la respuesta
                if 'token' in response:
                    token = response['token']
                    return token
                else:
                    return None
            else:
                error_msg = response.get('message', 'Error desconocido') if response else 'Sin respuesta'
                return None
                
        except Exception as e:
            return None
    
    def send_alarm_data(self, mqtt_data: Dict[str, Any], token: str) -> Optional[Dict]:
        """Enviar datos de alarma al backend usando token (thread-safe)"""
        try:
            self.logger.info("🚨 Enviando alarma MQTT al backend")
            endpoint = '/api/mqtt-alerts'
            with self._auth_header_lock:
                self.session.headers['Authorization'] = f'Bearer {token}'
                try:
                    response = self.post(endpoint, data=mqtt_data)
                finally:
                    self.session.headers.pop('Authorization', None)

            if response:
                return response
            return None

        except Exception as e:
            self.logger.error("❌ send_alarm_data excepción: %s", e)
            return None
    
    def health_check(self) -> bool:
        """Verificar que el backend esté disponible"""
        try:
            response = self.session.get(f'{self.config.base_url}/api/mqtt-alerts/test-flow')
            if response.status_code == 200:
                return True
            else:
                self.logger.error(f"Health check falló con status: {response.status_code}")
                return False
        except Exception as e:
            self.logger.error(f"Error en health check: {e}")
            return False
    
    def create_user_alert(self, usuario_id: str, latitud: str, longitud: str, tipo_alerta: str, descripcion: str, 
                          prioridad: str = "media", tipo_creador: str = "usuario") -> Optional[Dict]:
        """
        Crear una alerta de usuario y obtener topics e ID de alerta
        
        Args:
            usuario_id: ID del usuario que crea la alerta (obligatorio)
            latitud: Latitud de la ubicación (obligatorio)
            longitud: Longitud de la ubicación (obligatorio)
            tipo_alerta: Tipo de alerta (obligatorio)
            descripcion: Descripción de la alerta (obligatorio)
            prioridad: Prioridad de la alerta (opcional, por defecto 'media')
            tipo_creador: Tipo del creador (opcional, por defecto 'usuario')
            
        Returns:
            Dict con la respuesta del backend incluyendo topics e ID de alerta
        """
        try:
            self.logger.info(
                "🚨 Creando alerta de usuario para %s (%s)",
                usuario_id,
                tipo_alerta
            )
            endpoint = '/api/mqtt-alerts/user-alert'
            
            data = {
                "creador": {
                    "usuario_id": usuario_id,
                    "tipo": tipo_creador
                },
                "ubicacion": {
                    "latitud": latitud,
                    "longitud": longitud
                },
                "tipo_alerta": tipo_alerta,
                "descripcion": descripcion,
                "prioridad": prioridad
            }
            
            
            response = self.post(endpoint, data=data)
            
            if response and response.get('success'):
                alert_id = response.get('alert_id', 'N/A')
                topics = response.get('topics_otros_hardware', [])
                
                
                return response
            else:
                error_msg = response.get('message', 'Error desconocido') if response else 'Sin respuesta'
                return None
                
        except Exception as e:
            return None
    
    def deactivate_user_alert(self, alert_id: str, desactivado_por_id: str, desactivado_por_tipo: str = "usuario") -> Optional[Dict]:
        """
        Desactivar/apagar una alerta de usuario
        
        Args:
            alert_id: ID de la alerta a desactivar (obligatorio)
            desactivado_por_id: ID del usuario que desactiva la alerta (obligatorio)
            desactivado_por_tipo: Tipo de quien desactiva la alerta (opcional, por defecto 'usuario')
            
        Returns:
            Dict con la respuesta del backend
        """
        try:
            self.logger.info(
                "🛑 Desactivando alerta %s por %s",
                alert_id,
                desactivado_por_id
            )
            endpoint = '/api/mqtt-alerts/user-alert/deactivate'
            
            data = {
                "alert_id": alert_id,
                "desactivado_por_id": desactivado_por_id,
                "desactivado_por_tipo": desactivado_por_tipo
            }
            
            
            response = self.put(endpoint, data=data)
            
            if response and response.get('success'):
                return response

            if response:
                error_msg = response.get('message', 'Error desconocido')
                status_code = response.get('_status_code', 'N/A')
                self.logger.error(
                    "❌ Error desactivando alerta (status %s): %s",
                    status_code,
                    error_msg
                )
                return response

            self.logger.error("❌ Sin respuesta del backend al desactivar alerta")
            return None
                
        except Exception as e:
            return None
    
    def put(self, endpoint: str, data: Optional[Dict] = None) -> Optional[Dict]:
        """Realizar petición PUT"""
        return self._make_request('PUT', endpoint, data=data)
    
    def update_user_status(self, alert_id: str, usuario_id: str, disponible: Optional[bool] = None, 
                           embarcado: Optional[bool] = None) -> Optional[Dict]:
        """
        Actualizar el estado de disponibilidad y/o embarcado de un usuario en una alerta
        
        Args:
            alert_id: ID de la alerta (obligatorio)
            usuario_id: ID del usuario (obligatorio)
            disponible: Estado de disponibilidad del usuario (opcional)
            embarcado: Estado de embarcado del usuario (opcional)
            
        Returns:
            Dict con la respuesta del backend
        """
        try:
            self.logger.info(
                "✅ Actualizando estado de usuario %s en alerta %s",
                usuario_id,
                alert_id
            )
            endpoint = '/api/mqtt-alerts/update-user-status'
            
            # Construir data solo con los campos proporcionados
            data = {
                "alert_id": alert_id,
                "usuario_id": usuario_id
            }
            
            # Agregar campos opcionales solo si se proporcionan
            if disponible is not None:
                data["disponible"] = disponible
                
            if embarcado is not None:
                data["embarcado"] = embarcado
            
            # Validar que al menos un campo de estado se proporcione
            if disponible is None and embarcado is None:
                self.logger.error("❌ Debe proporcionar al menos 'disponible' o 'embarcado'")
                return None
            
            response = self.patch(endpoint, data=data)
            
            if response and response.get('success'):
                return response

            if response:
                error_msg = response.get('message', 'Error desconocido')
                status_code = response.get('_status_code', 'N/A')
                self.logger.error(
                    "❌ Error actualizando estado de usuario (status %s): %s",
                    status_code,
                    error_msg
                )
                return response

            self.logger.error("❌ Sin respuesta del backend al actualizar estado de usuario")
            return None

        except Exception as e:
            self.logger.error(f"❌ Error actualizando estado de usuario: {e}")
            return None
    
    def create_empresa_alert(self, empresa_id: str, sede: str, tipo_alerta: str, descripcion: str, 
                           prioridad: str = "media") -> Optional[Dict]:
        """
        Crear una alerta de empresa y obtener topics e ID de alerta
        
        Args:
            empresa_id: ID de la empresa que crea la alerta (obligatorio)
            sede: Sede de la empresa (obligatorio)
            tipo_alerta: Tipo de alerta (obligatorio)
            descripcion: Descripción de la alerta (obligatorio)
            prioridad: Prioridad de la alerta (opcional, por defecto 'media')
            
        Returns:
            Dict con la respuesta del backend incluyendo topics e ID de alerta
        """
        try:
            self.logger.info(
                "🏢 Creando alerta de empresa %s (%s)",
                empresa_id,
                tipo_alerta
            )
            endpoint = '/api/mqtt-alerts/user-alert'
            
            data = {
                "creador": {
                    "empresa_id": empresa_id,
                    "tipo": "empresa",
                    "sede": sede
                },
                "tipo_alerta": tipo_alerta,
                "descripcion": descripcion,
                "prioridad": prioridad
            }
            
            
            response = self.post(endpoint, data=data)
            
            if response and response.get('success'):
                alert_id = response.get('alert_id', 'N/A')
                topics = response.get('topics_otros_hardware', [])
                
                
                return response
            else:
                error_msg = response.get('message', 'Error desconocido') if response else 'Sin respuesta'
                return None
                
        except Exception as e:
            return None

    def get_empresa_alarm_types(self, empresa_id: str) -> Optional[Dict]:
        """Obtener lista completa de tipos de alarma para una empresa"""
        try:
            endpoint = f"/api/tipos-alarma/empresa/{empresa_id}/todos"


            response = self.get(endpoint=endpoint)

            if response:
                status_code = response.get('_status_code', 200)

                if status_code == 404:
                    return response

            if response and response.get('success'):
                tipos = response.get('data', [])
                return response

            error_msg = response.get('message', 'Error desconocido') if response else 'Sin respuesta'
            return response

        except Exception as e:
            return None

    def get_alert_by_id(self, alert_id: str, user_id: str) -> Optional[Dict]:
        """
        Obtener detalles de una alerta específica para un usuario
        
        Args:
            alert_id: ID de la alerta a consultar (obligatorio)
            user_id: ID del usuario que solicita los detalles (obligatorio)
            
        Returns:
            Dict con los datos de la alerta o None si no se encuentra/hay error
        """
        try:
            endpoint = '/api/mqtt-alerts/user-alert/details'
            
            data = {
                "alert_id": alert_id,
                "user_id": user_id
            }
            
            
            response = self.post(endpoint, data=data)
            
            if response:
                status_code = response.get('_status_code', 200)
                
                # Manejo específico para 404 (alerta no encontrada o usuario sin acceso)
                if status_code == 404:
                    error_msg = response.get('message', 'Alerta no encontrada o usuario sin acceso')
                    return response  # Devolver respuesta completa para manejo de errores
                
                # Manejo específico para 401 (no autorizado)
                elif status_code == 401:
                    error_msg = response.get('message', 'No autorizado')
                    return response  # Devolver respuesta completa para manejo de errores
                
                # Para respuestas exitosas
                if response.get('success'):
                    alert_data = response.get('alert')
                    if alert_data:
                        
                        return response  # Devolver respuesta completa
                    else:
                        return response
                else:
                    error_msg = response.get('error', response.get('message', 'Error desconocido'))
                    return response  # Devolver respuesta completa para manejo de errores
            else:
                return None
                
        except Exception as e:
            return None
    
    def patch(self, endpoint: str, data: Optional[Dict] = None) -> Optional[Dict]:
        """Realizar petición PATCH"""
        return self._make_request('PATCH', endpoint, data=data)

    def manager_list_active_alerts(self, telefono: Optional[str] = None, usuario_id: Optional[str] = None) -> Optional[Dict]:
        """Lista la última alerta activa por sede para un manager"""
        try:
            payload = {}
            if telefono:
                payload["telefono"] = telefono
            if usuario_id:
                payload["usuario_id"] = usuario_id
            response = self.post("/api/mqtt-alerts/manager/active-by-sede", data=payload)
            return response if isinstance(response, dict) else None
        except Exception:
            return None

    def manager_switch_focus(self, telefono: str, alert_id: str) -> Optional[Dict]:
        """Cambia el foco de alerta del manager en el cache de WhatsApp"""
        try:
            response = self.post("/api/mqtt-alerts/manager/switch-focus", data={
                "telefono": telefono,
                "alert_id": alert_id
            })
            return response if isinstance(response, dict) else None
        except Exception:
            return None

    def log_alert_message(self, alert_id: str, payload: Dict) -> Optional[Dict]:
        """Registra un mensaje de la conversación de una alerta (in/out)"""
        try:
            response = self.post(f"/api/mqtt-alerts/{alert_id}/messages", data=payload)
            return response if isinstance(response, dict) else None
        except Exception:
            return None

    def get_alert_messages(self, alert_id: str, direction: Optional[str] = None, limit: int = 15) -> Optional[Dict]:
        """Lista mensajes de la conversación de una alerta"""
        try:
            params = []
            if direction:
                params.append(f"direction={direction}")
            params.append(f"limit={limit}")
            qs = "&".join(params)
            response = self.get(f"/api/mqtt-alerts/{alert_id}/messages?{qs}")
            return response if isinstance(response, dict) else None
        except Exception:
            return None

    def _clear_token(self):
        """Limpiar el token de los headers después de usarlo"""
        if 'Authorization' in self.session.headers:
            del self.session.headers['Authorization']
