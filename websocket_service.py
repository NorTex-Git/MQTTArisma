#!/usr/bin/env python3
"""
Servicio WebSocket independiente
Recibe mensajes de WhatsApp por WebSocket y puede enviar notificaciones MQTT.
"""

import asyncio
import signal
import sys
import os
import logging
import json
from aiohttp import web

# Agregar el directorio actual al path para las importaciones
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from clients.websocket_server import WebSocketServer
from clients.backend_client import BackendClient
from services.whatsapp_service import WhatsAppService
from handlers.websocket_message_handler import WebSocketMessageHandler
from utils.logger import setup_logger, setup_root_logging
from config import AppConfig
from config.settings import _env_int


def _internal_http_port() -> int:
    """Puerto del HTTP interno. docker-compose interpola ${VAR} a "" cuando la
    variable no existe en el .env, y un int("") tumbaba el arranque completo
    del servicio (con él, el endpoint de fanout)."""
    return _env_int("INTERNAL_HTTP_PORT", 8081)


class WebSocketService:
    """Servicio WebSocket independiente - SIN dependencias de MQTT"""
    
    def __init__(self):
        # Configuración
        self.config = AppConfig()
        # Logging del root ANTES de construir clientes: los publishers MQTT
        # loguean su conexión en el constructor y esos logs se perdían.
        setup_root_logging(self.config.log_level, log_file="logs/websocket_service.log")
        # Logger con archivo separado para poder hacer tail -f
        self.logger = setup_logger(
            "websocket_service",
            self.config.log_level,
            log_file="logs/websocket_service.log"
        )
        self.logger.info("⚙️ Config MQTT efectiva: %s", self.config.mqtt.summary())

        # Clientes independientes para WebSocket
        self.backend_client = BackendClient(self.config.backend)
        self.whatsapp_service = WhatsAppService(self.config.whatsapp)
        
        # Servidor WebSocket CON MQTT Publisher para envío
        self.websocket_server = WebSocketServer(
            host=self.config.websocket.host,
            port=self.config.websocket.port,
            backend_client=self.backend_client,
            whatsapp_service=self.whatsapp_service,
            enable_mqtt_publisher=True  # Habilitar envío MQTT desde WhatsApp
        )
        
        # Estado del servicio
        self.is_running = False
        self._http_runner = None

        # Configurar manejo de señales
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Manejar señales de terminación"""
        self.logger.info("\n¡Recibida señal de terminación para servicio WebSocket!")
        asyncio.create_task(self.stop())
        sys.exit(0)
    
    async def _handle_fanout(self, request: web.Request) -> web.Response:
        """Endpoint HTTP interno: POST /internal/fanout-alert"""
        try:
            payload = await request.json()
            if not isinstance(payload, dict):
                return web.json_response({"success": False, "error": "payload must be a JSON object"}, status=400)

            handler = self.websocket_server.message_handler

            alert_for_event = payload.get("alert") if isinstance(payload.get("alert"), dict) else payload
            raw_type = payload.get("type")
            event_type = (
                "alert.deactivated"
                if raw_type == "alert_deactivated_by_empresa"
                else "alert.created"
            )
            await self.websocket_server.realtime_hub.publish({
                "type": event_type,
                "empresaId": payload.get("empresa_id") or alert_for_event.get("empresa_id"),
                "entityId": payload.get("alert_id") or alert_for_event.get("_id"),
                "occurredAt": (
                    alert_for_event.get("fecha_actualizacion")
                    or alert_for_event.get("fecha_creacion")
                ),
                "payload": {"alert": alert_for_event},
            })

            # Desactivación: el backend manda el evento completo con su `type`.
            # Antes este endpoint solo sabía de activaciones y habría tratado una
            # desactivación como si fuera una alerta nueva.
            if payload.get("type") == "alert_deactivated_by_empresa":
                success = handler.trigger_alert_event(payload)
                return web.json_response({"success": success}, status=200 if success else 500)

            # Compatibilidad: payload puede venir como {alert, alert_managers} o como alert plano (legado)
            if "alert" in payload and isinstance(payload.get("alert"), dict):
                alert_data = payload["alert"]
                alert_managers = payload.get("alert_managers") or []
            else:
                alert_data = payload
                alert_managers = []

            success = handler.trigger_fanout(alert_data, alert_managers=alert_managers)
            return web.json_response({"success": success}, status=200 if success else 500)
        except Exception as e:
            self.logger.error(f"❌ Error en /internal/fanout-alert: {e}")
            return web.json_response({"success": False, "error": str(e)}, status=500)

    async def _handle_realtime_event(self, request: web.Request) -> web.Response:
        """Publicar eventos de dominio que no requieren fanout MQTT/WhatsApp."""
        try:
            payload = await request.json()
            if not isinstance(payload, dict) or not payload.get("type"):
                return web.json_response(
                    {"success": False, "error": "type is required"}, status=400
                )
            await self.websocket_server.realtime_hub.publish(payload)
            return web.json_response({"success": True})
        except Exception as exc:
            self.logger.error("Error publicando evento realtime: %s", exc)
            return web.json_response({"success": False, "error": str(exc)}, status=500)

    def _get_publishers(self) -> dict:
        """Publishers MQTT vivos en este proceso, por nombre"""
        publishers = {}
        try:
            handler = self.websocket_server.message_handler
            if handler.mqtt_publisher:
                publishers["websocket"] = handler.mqtt_publisher
            if handler.empresa_handler and handler.empresa_handler.mqtt_publisher:
                publishers["empresa"] = handler.empresa_handler.mqtt_publisher
        except Exception as e:
            self.logger.error(f"❌ Error obteniendo publishers: {e}")
        return publishers

    async def _handle_status(self, request: web.Request) -> web.Response:
        """GET /internal/status - diagnóstico: a qué broker publica y si está conectado"""
        publishers = self._get_publishers()
        payload = {
            "service": "websocket_service",
            "config": {
                **self.config.mqtt.summary(),
                "internal_http_port": _internal_http_port(),
                "backend_url": self.config.backend.base_url,
            },
            "publishers": {name: pub.get_status() for name, pub in publishers.items()},
        }
        try:
            handler = self.websocket_server.message_handler
            if handler.empresa_handler:
                payload["empresa_handler"] = handler.empresa_handler.get_statistics()
            payload["whatsapp"] = handler.get_whatsapp_statistics()
            payload["realtime"] = self.websocket_server.realtime_hub.get_statistics()
        except Exception as e:
            payload["stats_error"] = str(e)

        return web.json_response(payload)

    async def _handle_health(self, request: web.Request) -> web.Response:
        """GET /health - unhealthy si ningún publisher MQTT está conectado.
        Así la falla la ve el orquestador y no un usuario en una emergencia."""
        publishers = self._get_publishers()
        connected = {name: pub.is_connected for name, pub in publishers.items()}
        healthy = bool(connected) and all(connected.values())
        return web.json_response(
            {
                "status": "ok" if healthy else "degraded",
                "mqtt_publishers": connected,
                "broker": f"{self.config.mqtt.broker}:{self.config.mqtt.port}",
            },
            status=200 if healthy else 503
        )

    async def _start_http_server(self) -> None:
        """Iniciar servidor HTTP interno en puerto 8081"""
        http_port = _internal_http_port()
        app = web.Application()
        app.router.add_post("/internal/fanout-alert", self._handle_fanout)
        app.router.add_post("/internal/realtime-event", self._handle_realtime_event)
        app.router.add_get("/internal/status", self._handle_status)
        app.router.add_get("/health", self._handle_health)
        self._http_runner = web.AppRunner(app)
        await self._http_runner.setup()
        site = web.TCPSite(self._http_runner, "0.0.0.0", http_port)
        await site.start()
        self.logger.info(f"✅ HTTP interno iniciado en puerto {http_port}")

    async def start(self) -> bool:
        """Iniciar el servicio WebSocket"""
        self.logger.info("🚀 Iniciando servicio WebSocket independiente...")
        self.logger.info(f"📡 Servidor: ws://{self.config.websocket.host}:{self.config.websocket.port}")
        self.logger.info("📱 Procesamiento de WhatsApp con envío MQTT")
        self.logger.info("✅ MQTT Publisher habilitado para notificaciones")

        try:
            # Iniciar servidor WebSocket
            await self.websocket_server.start()
            # Iniciar servidor HTTP interno para fanout desde RescueBack.
            # Si falla, el servicio sigue vivo atendiendo WhatsApp en vez de
            # caer entero (y quedar en bucle de reinicio).
            try:
                await self._start_http_server()
            except Exception as http_error:
                self.logger.critical(
                    "🚨 HTTP interno NO disponible (%s): el backend no podrá disparar "
                    "activaciones ni desactivaciones de hardware", http_error
                )
            self.is_running = True

            self.logger.info("✅ Servicio WebSocket iniciado correctamente")
            self.logger.info("📱 Esperando mensajes de WhatsApp...")
            self.logger.info("📋 Presiona Ctrl+C para detener")

            return True

        except Exception as e:
            self.logger.error(f"❌ Error iniciando servicio WebSocket: {e}")
            return False
    
    async def run(self):
        """Ejecutar el servicio WebSocket de forma continua"""
        if not await self.start():
            self.logger.error("❌ No se pudo iniciar el servicio WebSocket")
            return False
        
        try:
            # Iniciar tarea de estadísticas periódicas
            stats_task = asyncio.create_task(self._show_statistics_periodically())
            
            try:
                # Mantener el servidor corriendo indefinidamente
                await asyncio.Future()
            finally:
                # Cancelar tarea de estadísticas
                stats_task.cancel()
                try:
                    await stats_task
                except asyncio.CancelledError:
                    pass
                    
        except KeyboardInterrupt:
            self.logger.info("Interrupción del usuario detectada")
        finally:
            await self.stop()
        
        return True
    
    async def stop(self):
        """Detener el servicio WebSocket"""
        self.logger.info("🛑 Deteniendo servicio WebSocket...")
        self.is_running = False
        
        try:
            # Detener procesamiento de cola WhatsApp
            if hasattr(self, 'websocket_server') and self.websocket_server:
                await self.websocket_server.stop_whatsapp_processing()
                
                # Mostrar estadísticas finales
                await self._show_final_statistics()
                
                # Detener servidor
                await self.websocket_server.stop()
                self.logger.info("✅ WebSocket Server detenido")
            
            if self._http_runner:
                await self._http_runner.cleanup()
                self.logger.info("✅ HTTP interno detenido")

        except Exception as e:
            self.logger.error(f"❌ Error deteniendo servicio: {e}")
    
    async def _show_statistics_periodically(self):
        """Mostrar estadísticas del WebSocket periódicamente"""
        while self.is_running:
            try:
                await asyncio.sleep(30)  # Mostrar estadísticas cada 30 segundos
                
                if self.websocket_server and self.websocket_server.is_running:
                    stats = self.websocket_server.get_whatsapp_statistics()
                    
                    self.logger.info("📊 Estadísticas WebSocket - WhatsApp:")
                    self.logger.info(f"  • Mensajes procesados: {stats['processed_messages']}")
                    self.logger.info(f"  • Errores: {stats['error_count']}")
                    self.logger.info(f"  • Cola actual: {stats['queue_size']}/{stats['queue_max_size']}")
                    self.logger.info(f"  • Procesando: {'Sí' if stats['is_processing'] else 'No'}")
                    self.logger.info(f"  • Tasa de error: {stats['error_rate']}%")
                    self.logger.info("=" * 50)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.warning(f"⚠️ Error mostrando estadísticas WebSocket: {e}")
    
    async def _show_final_statistics(self):
        """Mostrar estadísticas finales"""
        if self.websocket_server:
            stats = self.websocket_server.get_whatsapp_statistics()
            
            self.logger.info("📊 Estadísticas finales WebSocket:")
            self.logger.info(f"  • Total de mensajes procesados: {stats['processed_messages']}")
            self.logger.info(f"  • Total de errores: {stats['error_count']}")
            self.logger.info(f"  • Mensajes pendientes: {stats['queue_size']}")
            self.logger.info(f"  • Tasa de éxito final: {100 - stats['error_rate']}%")
    
    def get_status(self) -> dict:
        """Obtener estado del servicio"""
        websocket_stats = {}
        if self.websocket_server:
            websocket_stats = self.websocket_server.get_whatsapp_statistics()
        
        return {
            "service": "WebSocket Service",
            "running": self.is_running,
            "websocket_running": self.websocket_server.is_running if self.websocket_server else False,
            "backend_enabled": self.config.backend.enabled,
            "whatsapp_enabled": self.config.whatsapp.enabled,
            "whatsapp_stats": websocket_stats
        }


async def main():
    """Función principal para ejecutar el servicio WebSocket"""
    fallback_logger = None

    try:
        websocket_service = WebSocketService()
    except Exception as exc:
        fallback_logger = setup_logger(
            "websocket_service_bootstrap",
            os.getenv("LOG_LEVEL", "INFO"),
            log_file="logs/websocket_service.log"
        )
        fallback_logger.exception("Error inicializando servicio WebSocket", exc_info=exc)
        sys.exit(1)

    try:
        await websocket_service.run()

    except Exception as exc:
        logger = websocket_service.logger if websocket_service and websocket_service.logger else fallback_logger
        if logger is None:
            logger = setup_logger(
                "websocket_service_bootstrap",
                os.getenv("LOG_LEVEL", "INFO"),
                log_file="logs/websocket_service.log"
            )
        logger.exception("Error ejecutando servicio WebSocket", exc_info=exc)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
