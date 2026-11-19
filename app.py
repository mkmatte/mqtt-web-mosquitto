"""Aplicação web FastAPI para visualização e publicação em servidor Mosquitto MQTT."""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import logging
import os
import secrets
import threading
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator

import uvicorn
from dotenv import load_dotenv, set_key
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from mqtt_service import MessageStore, MqttConfig, MqttService

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("mqtt_visualizer.app")

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
SESSION_COOKIE_NAME = "mqtt_vis_session"


class AuthManager:
    """Gerencia autenticação única e alteração persistente de senha no .env."""

    def __init__(
        self, username: str, password: str, env_path: Path | None = None
    ) -> None:
        self.username = username
        self.password = password
        self.env_path = env_path or (BASE_DIR / ".env")
        self._sessions: set[str] = set()
        self._lock = threading.Lock()

    @classmethod
    def from_env(cls, env_path: Path | None = None) -> "AuthManager":
        username = os.getenv("WEB_AUTH_USER", "admin").strip() or "admin"
        password = os.getenv("WEB_AUTH_PASSWORD", "troque_esta_senha")
        return cls(username=username, password=password, env_path=env_path)

    def verify_credentials(self, username: str, password: str) -> bool:
        return secrets.compare_digest(self.username, username) and secrets.compare_digest(
            self.password, password
        )

    def create_session(self) -> str:
        token = secrets.token_urlsafe(32)
        with self._lock:
            self._sessions.add(token)
        return token

    def verify_session(self, token: str | None) -> bool:
        if not token:
            return False
        with self._lock:
            return token in self._sessions

    def revoke_session(self, token: str | None) -> None:
        if not token:
            return
        with self._lock:
            self._sessions.discard(token)

    def change_password(self, current_password: str, new_password: str) -> str:
        with self._lock:
            if not secrets.compare_digest(self.password, current_password):
                raise PermissionError("Senha atual incorreta.")
            if len(new_password.strip()) < 4:
                raise ValueError("A nova senha deve ter pelo menos 4 caracteres.")

            self.env_path.parent.mkdir(parents=True, exist_ok=True)
            if not self.env_path.exists():
                self.env_path.write_text("", encoding="utf-8")
            set_key(str(self.env_path), "WEB_AUTH_PASSWORD", new_password)

            self.password = new_password
            os.environ["WEB_AUTH_PASSWORD"] = new_password
            self._sessions.clear()
            new_token = secrets.token_urlsafe(32)
            self._sessions.add(new_token)
            return new_token


def require_auth(request: Request) -> str:
    """Exige sessão autenticada via cookie ou Basic Auth."""
    auth: AuthManager = request.app.state.auth
    token = request.cookies.get(SESSION_COOKIE_NAME)
    if auth.verify_session(token):
        return auth.username

    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Basic "):
        try:
            decoded = base64.b64decode(auth_header[6:].strip()).decode("utf-8")
            user, pwd = decoded.split(":", 1)
            if auth.verify_credentials(user, pwd):
                return auth.username
        except Exception:
            pass

    raise HTTPException(status_code=401, detail="Não autenticado.")


class LoginRequest(BaseModel):
    """Credenciais para acesso ao visualizador web."""

    username: str = Field(..., min_length=1)
    password: str = Field(..., min_length=1)


class ChangePasswordRequest(BaseModel):
    """Dados para atualização da senha única de acesso."""

    current_password: str = Field(..., min_length=1)
    new_password: str = Field(..., min_length=4)


class PublishRequest(BaseModel):
    """Payload para publicação de mensagem em um tópico MQTT."""

    topic: str = Field(..., min_length=1, description="Tópico de destino")
    payload: str = Field(default="", description="Conteúdo da mensagem")
    qos: int = Field(default=0, ge=0, le=2, description="QoS (0, 1 ou 2)")
    retain: bool = Field(default=False, description="Manter mensagem retida no broker")


def resolve_web_host_port(argv: list[str] | None = None) -> tuple[str, int]:
    """Resolve host e porta do servidor web via variáveis de ambiente e argumentos CLI."""
    default_host = os.getenv("WEB_HOST", "0.0.0.0").strip() or "0.0.0.0"
    default_port = int(os.getenv("WEB_PORT", "1882"))

    parser = argparse.ArgumentParser(
        description="Visualizador Web para servidor MQTT Mosquitto"
    )
    parser.add_argument(
        "--host",
        default=default_host,
        help=f"Endereço de escuta do servidor web (padrão: {default_host})",
    )
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        default=default_port,
        help=f"Porta do servidor web (padrão: {default_port})",
    )
    args = parser.parse_args(argv)
    if not (1 <= args.port <= 65535):
        raise ValueError("A porta web deve estar entre 1 e 65535.")
    return args.host, args.port


async def _periodic_prune(store: MessageStore, interval_seconds: float = 60.0) -> None:
    """Remove periodicamente mensagens que expiraram da janela de retenção."""
    while True:
        try:
            await asyncio.sleep(interval_seconds)
            removed = store.prune_expired()
            if removed > 0:
                logger.info("Limpeza automática: %d mensagens expiradas removidas.", removed)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.warning("Erro na limpeza periódica: %s", exc)


def create_app(
    store: MessageStore | None = None,
    mqtt_service: MqttService | None = None,
    start_mqtt: bool = True,
    auth_manager: AuthManager | None = None,
) -> FastAPI:
    """Cria a instância FastAPI configurada com rotas e ciclo de vida MQTT."""

    @asynccontextmanager
    async def lifespan(fastapi_app: FastAPI) -> AsyncIterator[None]:
        owns_store = store is None
        active_config = MqttConfig.from_env() if store is None or mqtt_service is None else mqtt_service.config
        active_store = store or MessageStore(
            db_path=active_config.db_path,
            retention_hours=active_config.retention_hours,
        )
        active_mqtt = mqtt_service or MqttService(config=active_config, store=active_store)
        active_auth = auth_manager or getattr(fastapi_app.state, "auth", None) or AuthManager.from_env()

        fastapi_app.state.store = active_store
        fastapi_app.state.mqtt = active_mqtt
        fastapi_app.state.auth = active_auth

        active_store.prune_expired()
        loop = asyncio.get_running_loop()
        if start_mqtt:
            active_mqtt.start(loop)

        prune_task = asyncio.create_task(_periodic_prune(active_store, interval_seconds=60.0))
        try:
            yield
        finally:
            prune_task.cancel()
            try:
                await prune_task
            except asyncio.CancelledError:
                pass
            if start_mqtt:
                active_mqtt.stop()
            if owns_store:
                active_store.close()

    fastapi_app = FastAPI(
        title="Visualizador MQTT Mosquitto",
        description="Monitora canais ativos nas últimas 4 horas e envia mensagens MQTT.",
        lifespan=lifespan,
    )
    fastapi_app.state.auth = auth_manager or AuthManager.from_env()

    if STATIC_DIR.exists():
        fastapi_app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    @fastapi_app.get("/", include_in_schema=False)
    async def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    @fastapi_app.post("/api/auth/login")
    async def api_login(
        request: Request, response: Response, body: LoginRequest
    ) -> dict[str, Any]:
        auth: AuthManager = request.app.state.auth
        if not auth.verify_credentials(body.username.strip(), body.password):
            raise HTTPException(status_code=401, detail="Usuário ou senha inválidos.")
        token = auth.create_session()
        response.set_cookie(
            key=SESSION_COOKIE_NAME,
            value=token,
            httponly=True,
            samesite="lax",
            path="/",
        )
        return {"ok": True, "username": auth.username}

    @fastapi_app.post("/api/auth/logout")
    async def api_logout(request: Request, response: Response) -> dict[str, Any]:
        auth: AuthManager = request.app.state.auth
        auth.revoke_session(request.cookies.get(SESSION_COOKIE_NAME))
        response.delete_cookie(key=SESSION_COOKIE_NAME, path="/")
        return {"ok": True}

    @fastapi_app.get("/api/auth/me")
    async def api_me(username: str = Depends(require_auth)) -> dict[str, Any]:
        return {"authenticated": True, "username": username}

    @fastapi_app.post("/api/auth/change-password")
    async def api_change_password(
        request: Request,
        response: Response,
        body: ChangePasswordRequest,
        _user: str = Depends(require_auth),
    ) -> dict[str, Any]:
        auth: AuthManager = request.app.state.auth
        try:
            new_token = auth.change_password(body.current_password, body.new_password)
        except PermissionError as exc:
            raise HTTPException(status_code=401, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        response.set_cookie(
            key=SESSION_COOKIE_NAME,
            value=new_token,
            httponly=True,
            samesite="lax",
            path="/",
        )
        return {"ok": True, "message": "Senha alterada com sucesso."}

    @fastapi_app.get("/api/status")
    async def api_status(
        request: Request, _user: str = Depends(require_auth)
    ) -> dict[str, Any]:
        service: MqttService = request.app.state.mqtt
        msg_store: MessageStore = request.app.state.store
        status = service.get_status()
        status["active_channels_count"] = len(msg_store.get_active_channels())
        return status

    @fastapi_app.get("/api/channels")
    async def api_channels(
        request: Request,
        search: str = Query(default="", max_length=256),
        _user: str = Depends(require_auth),
    ) -> dict[str, Any]:
        service: MqttService = request.app.state.mqtt
        msg_store: MessageStore = request.app.state.store
        channels = msg_store.get_active_channels(search=search)
        return {
            "retention_hours": service.config.retention_hours,
            "channels": channels,
        }

    @fastapi_app.get("/api/messages")
    async def api_messages(
        request: Request,
        topic: str | None = Query(default=None),
        limit: int = Query(default=100, ge=1, le=500),
        _user: str = Depends(require_auth),
    ) -> dict[str, Any]:
        msg_store: MessageStore = request.app.state.store
        messages = msg_store.get_recent_messages(topic=topic, limit=limit)
        return {"messages": messages}

    @fastapi_app.post("/api/publish")
    async def api_publish(
        request: Request,
        body: PublishRequest,
        _user: str = Depends(require_auth),
    ) -> dict[str, Any]:
        service: MqttService = request.app.state.mqtt
        try:
            return service.publish(
                topic=body.topic,
                payload=body.payload,
                qos=body.qos,
                retain=body.retain,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ConnectionError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Erro ao publicar: {exc}") from exc

    @fastapi_app.get("/api/stream")
    async def api_stream(
        request: Request, _user: str = Depends(require_auth)
    ) -> StreamingResponse:
        service: MqttService = request.app.state.mqtt
        queue = service.subscribe_events()

        async def event_generator() -> AsyncIterator[str]:
            try:
                initial_status = json.dumps(service.get_status(), ensure_ascii=False)
                yield f"event: status\ndata: {initial_status}\n\n"

                while True:
                    if await request.is_disconnected():
                        break
                    try:
                        event = await asyncio.wait_for(queue.get(), timeout=15.0)
                        payload_json = json.dumps(event["data"], ensure_ascii=False)
                        yield f"event: {event['type']}\ndata: {payload_json}\n\n"
                    except asyncio.TimeoutError:
                        yield ": keepalive\n\n"
            finally:
                service.unsubscribe_events(queue)

        return StreamingResponse(
            event_generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    return fastapi_app


app = create_app()


def main() -> None:
    host, port = resolve_web_host_port()
    logger.info("Iniciando visualizador web em http://%s:%d", host, port)
    uvicorn.run(app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()
