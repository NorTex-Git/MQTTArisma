"""
Servidor WebSocket puro para recibir mensajes de WhatsApp - SIN MQTT
"""

import asyncio
import websockets
import logging
import json
from typing import Optional, Dict, Any
from handlers.websocket_message_handler import WebSocketMessageHandler
from clients.backend_client import BackendClient
from config import AppConfig
from clients.realtime_hub import RealtimeHub

class WebSocketServer:
    """Servidor WebSocket puro - SOLO para WhatsApp, sin dependencias MQTT"""
    
    def __init__(self, host: str = None, port: int = None, backend_client=None, whatsapp_service=None, enable_mqtt_publisher=False):
        # Crear configuración completa
        self.config = AppConfig()
        
        # Usar configuración centralizada o parámetros proporcionados
        self.host = host or self.config.websocket.host
        self.port = port or self.config.websocket.port
        
        # Usar servicios externos si se proporcionan, sino crear propios
        self.backend_client = backend_client or BackendClient(self.config.backend)
        self.whatsapp_service = whatsapp_service
        
        # Inicializar manejador de mensajes WebSocket puro
        self.message_handler = WebSocketMessageHandler(
            backend_client=self.backend_client,
            whatsapp_service=self.whatsapp_service,
            config=self.config,
            enable_mqtt_publisher=enable_mqtt_publisher
        )
        
        self.server = None
        self.logger = logging.getLogger(__name__)
        self.is_running = False
        self.realtime_hub = RealtimeHub(self.config)
        
        # Estadísticas solo para WhatsApp
        self.stats = {
            'whatsapp_messages': 0,
            'whatsapp_errors': 0
        }
        
        self.logger.info("📱 WebSocket Server configurado SOLO para WhatsApp")
        self.logger.info("❌ MQTT completamente deshabilitado en WebSocket")
    
        
    async def handle_client(self, websocket):
        """Manejar conexión de cliente"""
        self.logger.info(f"Cliente conectado desde {websocket.remote_address}")

        if websocket.path.startswith('/realtime'):
            await self.realtime_hub.handle_client(websocket)
            return
        
        try:
            async for message in websocket:
                # Usar el sistema de colas asíncrono
                success = await self.message_handler.queue_whatsapp_message(message)
                
                if not success:
                    self.logger.warning(f"No se pudo procesar mensaje: cola llena o error")
                    
        except websockets.exceptions.ConnectionClosed:
            self.logger.info(f"Cliente desconectado")
        except Exception as e:
            self.logger.error(f"Error manejando cliente: {e}")
    async def start(self):
        """Iniciar el servidor WebSocket"""
        self.logger.info(f"Iniciando servidor WebSocket en ws://{self.host}:{self.port}")
        
        self.realtime_hub.start(asyncio.get_running_loop())
        self.server = await websockets.serve(
            self.handle_client,
            self.host,
            self.port,
            ping_interval=self.config.websocket.ping_interval,
            ping_timeout=self.config.websocket.ping_timeout
        )
        
        self.is_running = True
        self.logger.info(f"✅ Servidor WebSocket iniciado en ws://{self.host}:{self.port}")
        
        return self.server
    
    async def stop(self):
        """Detener el servidor WebSocket"""
        self.logger.info("🛑 Deteniendo servidor WebSocket...")
        self.is_running = False
        await self.realtime_hub.stop()
        
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            
        self.logger.info("✅ Servidor WebSocket detenido")
    
    def set_message_handler(self, message_handler: WebSocketMessageHandler):
        """Establecer el manejador de mensajes"""
        self.message_handler = message_handler
    
    def get_whatsapp_statistics(self):
        """Obtener estadísticas del procesador de WhatsApp"""
        return self.message_handler.get_whatsapp_statistics()
    
    async def stop_whatsapp_processing(self):
        """Detener el procesamiento de la cola de WhatsApp"""
        await self.message_handler.stop_whatsapp_processing()
    
    async def clear_whatsapp_queue(self):
        """Limpiar la cola de WhatsApp"""
        return await self.message_handler.clear_whatsapp_queue()
