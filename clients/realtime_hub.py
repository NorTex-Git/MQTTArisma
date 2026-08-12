"""Canal realtime autenticado y multi-tenant para navegadores."""

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import threading
import time
import uuid
from urllib.parse import parse_qs, urlparse

import redis
import requests


class RealtimeHub:
    CHANNEL = "rescue:realtime:events"

    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.secret = os.getenv("REALTIME_SECRET", "")
        self.clients = {}
        self.loop = None
        self.redis_client = None
        self.pubsub = None
        self.stop_event = threading.Event()
        self.local_nonces = {}
        self.stats = {"published": 0, "delivered": 0, "rejected": 0}

    def start(self, loop):
        self.loop = loop
        if not self.secret:
            self.logger.info("Realtime validara tickets contra RescueBack")
        try:
            self.redis_client = redis.Redis(
                host=self.config.redis.host,
                port=self.config.redis.port,
                db=self.config.redis.db,
                password=self.config.redis.password,
                decode_responses=True,
                socket_timeout=3,
            )
            self.redis_client.ping()
            self.pubsub = self.redis_client.pubsub(ignore_subscribe_messages=True)
            self.pubsub.subscribe(self.CHANNEL)
            threading.Thread(target=self._listen, daemon=True).start()
            self.logger.info("Canal realtime conectado a Redis Pub/Sub")
        except Exception as exc:
            self.redis_client = None
            self.pubsub = None
            self.logger.warning("Realtime sin Redis; fanout limitado a esta instancia: %s", exc)

    def _listen(self):
        try:
            for message in self.pubsub.listen():
                if self.stop_event.is_set():
                    return
                if message.get("type") != "message" or not self.loop:
                    continue
                try:
                    event = json.loads(message["data"])
                    asyncio.run_coroutine_threadsafe(self._broadcast(event), self.loop)
                except Exception as exc:
                    self.logger.warning("Evento realtime invalido: %s", exc)
        except Exception as exc:
            if not self.stop_event.is_set():
                self.logger.warning("Listener realtime detenido: %s", exc)

    @staticmethod
    def _decode_segment(value):
        padding = "=" * (-len(value) % 4)
        return base64.urlsafe_b64decode((value + padding).encode("ascii"))

    def _consume_ticket(self, ticket):
        if not ticket:
            return None
        if not self.secret:
            payload = self._validate_ticket_with_backend(ticket)
            if not payload:
                return None
            now = int(time.time())
            nonce = str(payload.get("nonce", ""))
            if not nonce or not self._claim_nonce(nonce, int(payload["exp"]) - now):
                return None
            return payload
        if "." not in ticket:
            return None
        encoded, supplied = ticket.split(".", 1)
        expected = base64.urlsafe_b64encode(
            hmac.new(self.secret.encode("utf-8"), encoded.encode("ascii"), hashlib.sha256).digest()
        ).rstrip(b"=").decode("ascii")
        if not hmac.compare_digest(expected, supplied):
            return None
        try:
            payload = json.loads(self._decode_segment(encoded))
            now = int(time.time())
            if int(payload.get("exp", 0)) <= now:
                return None
            if payload.get("role") not in ("empresa", "super_admin"):
                return None
            nonce = str(payload.get("nonce", ""))
            if not nonce or not self._claim_nonce(nonce, int(payload["exp"]) - now):
                return None
            return payload
        except (ValueError, TypeError, json.JSONDecodeError):
            return None

    def _validate_ticket_with_backend(self, ticket):
        try:
            response = requests.post(
                f"{self.config.backend.base_url}/auth/realtime-ticket/validate",
                json={"ticket": ticket},
                headers=(
                    {self.config.backend.internal_token_header: self.config.backend.api_key}
                    if self.config.backend.api_key else {}
                ),
                timeout=5,
            )
            if response.status_code != 200:
                return None
            data = response.json()
            principal = data.get("principal")
            return principal if isinstance(principal, dict) else None
        except Exception as exc:
            self.logger.warning("No se pudo validar ticket realtime: %s", exc)
            return None

    def _claim_nonce(self, nonce, ttl):
        ttl = max(ttl, 1)
        if self.redis_client:
            try:
                return bool(self.redis_client.set(f"realtime:ticket:{nonce}", "1", nx=True, ex=ttl))
            except Exception:
                pass
        now = time.time()
        self.local_nonces = {key: exp for key, exp in self.local_nonces.items() if exp > now}
        if nonce in self.local_nonces:
            return False
        self.local_nonces[nonce] = now + ttl
        return True

    async def handle_client(self, websocket):
        parsed = urlparse(websocket.path)
        ticket = parse_qs(parsed.query).get("ticket", [None])[0]
        principal = await asyncio.to_thread(self._consume_ticket, ticket)
        if not principal:
            self.stats["rejected"] += 1
            await websocket.close(code=4401, reason="Ticket realtime invalido")
            return
        self.clients[websocket] = principal
        try:
            await websocket.send(json.dumps({"type": "connection.ready", "version": 1}))
            async for message in websocket:
                if message == "ping":
                    await websocket.send(json.dumps({"type": "pong", "version": 1}))
        finally:
            self.clients.pop(websocket, None)

    async def publish(self, event):
        self.stats["published"] += 1
        normalized = {
            "eventId": event.get("eventId") or str(uuid.uuid4()),
            "version": 1,
            "type": event["type"],
            "occurredAt": event.get("occurredAt") or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "empresaId": str(event["empresaId"]) if event.get("empresaId") else None,
            "entityId": str(event["entityId"]) if event.get("entityId") else None,
            "payload": event.get("payload") or {},
        }
        serialized = json.dumps(normalized, ensure_ascii=False, separators=(",", ":"))
        if self.redis_client:
            try:
                receivers = await asyncio.to_thread(
                    self.redis_client.publish, self.CHANNEL, serialized
                )
                if receivers:
                    return
            except Exception as exc:
                self.logger.warning("Redis publish fallo; usando fanout local: %s", exc)
        await self._broadcast(normalized)

    async def _broadcast(self, event):
        if not self.clients:
            return
        empresa_id = event.get("empresaId")
        encoded = json.dumps(event, ensure_ascii=False, separators=(",", ":"))
        stale = []
        for websocket, principal in list(self.clients.items()):
            allowed = principal.get("role") == "super_admin" or (
                empresa_id and str(principal.get("empresa_id")) == str(empresa_id)
            )
            if not allowed:
                continue
            try:
                await websocket.send(encoded)
                self.stats["delivered"] += 1
            except Exception:
                stale.append(websocket)
        for websocket in stale:
            self.clients.pop(websocket, None)

    async def stop(self):
        self.stop_event.set()
        if self.pubsub:
            self.pubsub.close()
        clients = list(self.clients)
        self.clients.clear()
        if clients:
            await asyncio.gather(
                *(client.close(code=1001, reason="Servicio detenido") for client in clients),
                return_exceptions=True,
            )

    def get_statistics(self):
        return {
            **self.stats,
            "connected_clients": len(self.clients),
            "redis_available": self.redis_client is not None,
        }
