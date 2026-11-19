"""Serviço MQTT com persistência em SQLite para janela de retenção (padrão 4h)."""

from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
import ssl
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import paho.mqtt.client as mqtt

logger = logging.getLogger("mqtt_visualizer.service")


def _parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "sim", "on"}


@dataclass(frozen=True)
class MqttConfig:
    """Configurações de conexão com o broker Mosquitto e retenção."""

    host: str = "localhost"
    port: int = 1883
    username: str = ""
    password: str = ""
    client_id: str = "mqtt-web-visualizer"
    topics: tuple[str, ...] = ("#",)
    use_tls: bool = False
    tls_insecure: bool = False
    ca_certs: str = ""
    retention_hours: float = 4.0
    max_messages_per_topic: int = 100
    db_path: str = "data/mqtt_history.db"

    @classmethod
    def from_env(cls) -> "MqttConfig":
        raw_topics = os.getenv("MQTT_TOPICS", "#")
        topics = tuple(t.strip() for t in raw_topics.split(",") if t.strip()) or ("#",)
        return cls(
            host=os.getenv("MQTT_HOST", "localhost").strip() or "localhost",
            port=int(os.getenv("MQTT_PORT", "1883")),
            username=os.getenv("MQTT_USERNAME", ""),
            password=os.getenv("MQTT_PASSWORD", ""),
            client_id=os.getenv("MQTT_CLIENT_ID", "mqtt-web-visualizer").strip()
            or "mqtt-web-visualizer",
            topics=topics,
            use_tls=_parse_bool(os.getenv("MQTT_USE_TLS"), False),
            tls_insecure=_parse_bool(os.getenv("MQTT_TLS_INSECURE"), False),
            ca_certs=os.getenv("MQTT_CA_CERTS", "").strip(),
            retention_hours=float(os.getenv("RETENTION_HOURS", "4")),
            max_messages_per_topic=int(os.getenv("MAX_MESSAGES_PER_TOPIC", "100")),
            db_path=os.getenv("DB_PATH", "data/mqtt_history.db").strip()
            or "data/mqtt_history.db",
        )


MAX_PAYLOAD_CHARS = 4096
MAX_TOTAL_MESSAGES = 5000


class MessageStore:
    """Armazenamento SQLite para mensagens e canais ativos na janela configurada."""

    def __init__(
        self,
        db_path: str,
        retention_hours: float = 4.0,
        max_payload_chars: int = MAX_PAYLOAD_CHARS,
        max_total_messages: int = MAX_TOTAL_MESSAGES,
    ) -> None:
        self.db_path = db_path
        self.retention_seconds = max(60.0, retention_hours * 3600.0)
        self.max_payload_chars = max_payload_chars
        self.max_total_messages = max_total_messages
        self._insert_count = 0
        self._lock = threading.Lock()

        if db_path != ":memory:":
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    qos INTEGER NOT NULL DEFAULT 0,
                    retain INTEGER NOT NULL DEFAULT 0,
                    timestamp REAL NOT NULL
                )
                """
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_messages_topic_ts ON messages (topic, timestamp DESC)"
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages (timestamp DESC)"
            )
            self._conn.commit()

    def add_message(
        self,
        topic: str,
        payload: str,
        qos: int = 0,
        retain: bool = False,
        timestamp: float | None = None,
    ) -> dict[str, Any]:
        ts = timestamp if timestamp is not None else time.time()
        safe_payload = (
            payload[: self.max_payload_chars]
            if len(payload) > self.max_payload_chars
            else payload
        )
        with self._lock:
            cur = self._conn.execute(
                """
                INSERT INTO messages (topic, payload, qos, retain, timestamp)
                VALUES (?, ?, ?, ?, ?)
                """,
                (topic, safe_payload, int(qos), 1 if retain else 0, ts),
            )
            msg_id = cur.lastrowid
            self._insert_count += 1
            if self._insert_count >= 100:
                self._insert_count = 0
                self._enforce_row_cap_locked()
            self._conn.commit()
        return {
            "id": msg_id,
            "topic": topic,
            "payload": safe_payload,
            "qos": int(qos),
            "retain": bool(retain),
            "timestamp": ts,
        }

    def _enforce_row_cap_locked(self) -> int:
        cur = self._conn.execute(
            """
            DELETE FROM messages
            WHERE id NOT IN (
                SELECT id FROM messages ORDER BY id DESC LIMIT ?
            )
            """,
            (self.max_total_messages,),
        )
        return cur.rowcount

    def prune_expired(self, now: float | None = None) -> int:
        cutoff = (now if now is not None else time.time()) - self.retention_seconds
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM messages WHERE timestamp < ?", (cutoff,)
            )
            deleted = cur.rowcount + self._enforce_row_cap_locked()
            self._conn.commit()
        return deleted

    def get_active_channels(
        self, search: str = "", now: float | None = None
    ) -> list[dict[str, Any]]:
        """Retorna canais com atividade dentro da janela de retenção (padrão 4h)."""
        cutoff = (now if now is not None else time.time()) - self.retention_seconds
        like_filter = f"%{search.strip()}%" if search.strip() else "%"
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT
                    m.topic,
                    COUNT(*) AS message_count,
                    MAX(m.timestamp) AS last_seen,
                    (
                        SELECT m2.payload
                        FROM messages m2
                        WHERE m2.topic = m.topic AND m2.timestamp >= ?
                        ORDER BY m2.timestamp DESC, m2.id DESC
                        LIMIT 1
                    ) AS last_payload,
                    (
                        SELECT m2.retain
                        FROM messages m2
                        WHERE m2.topic = m.topic AND m2.timestamp >= ?
                        ORDER BY m2.timestamp DESC, m2.id DESC
                        LIMIT 1
                    ) AS last_retain
                FROM messages m
                WHERE m.timestamp >= ? AND m.topic LIKE ?
                GROUP BY m.topic
                ORDER BY last_seen DESC
                """,
                (cutoff, cutoff, cutoff, like_filter),
            ).fetchall()

        return [
            {
                "topic": row["topic"],
                "message_count": row["message_count"],
                "last_seen": row["last_seen"],
                "last_payload": row["last_payload"] or "",
                "last_retain": bool(row["last_retain"]),
            }
            for row in rows
        ]

    def get_recent_messages(
        self, topic: str | None = None, limit: int = 100, now: float | None = None
    ) -> list[dict[str, Any]]:
        """Retorna mensagens recentes dentro da janela de retenção."""
        cutoff = (now if now is not None else time.time()) - self.retention_seconds
        safe_limit = max(1, min(limit, 500))
        with self._lock:
            if topic:
                rows = self._conn.execute(
                    """
                    SELECT id, topic, payload, qos, retain, timestamp
                    FROM messages
                    WHERE timestamp >= ? AND topic = ?
                    ORDER BY timestamp DESC, id DESC
                    LIMIT ?
                    """,
                    (cutoff, topic, safe_limit),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    """
                    SELECT id, topic, payload, qos, retain, timestamp
                    FROM messages
                    WHERE timestamp >= ?
                    ORDER BY timestamp DESC, id DESC
                    LIMIT ?
                    """,
                    (cutoff, safe_limit),
                ).fetchall()

        return [
            {
                "id": row["id"],
                "topic": row["topic"],
                "payload": row["payload"],
                "qos": row["qos"],
                "retain": bool(row["retain"]),
                "timestamp": row["timestamp"],
            }
            for row in rows
        ]

    def close(self) -> None:
        with self._lock:
            self._conn.close()


class MqttService:
    """Gerencia a conexão com o broker Mosquitto e distribui eventos para a UI."""

    def __init__(self, config: MqttConfig, store: MessageStore) -> None:
        self.config = config
        self.store = store
        self.connected = False
        self.last_error: str | None = None
        self.connected_since: float | None = None

        self._loop: asyncio.AbstractEventLoop | None = None
        self._subscribers: set[asyncio.Queue[dict[str, Any]]] = set()
        self._sub_lock = threading.Lock()
        self._client = self._build_client()

    def _build_client(self) -> mqtt.Client:
        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=self.config.client_id,
            protocol=mqtt.MQTTv311,
        )
        if self.config.username:
            client.username_pw_set(self.config.username, self.config.password or None)

        if self.config.use_tls:
            ca_certs = self.config.ca_certs or None
            client.tls_set(ca_certs=ca_certs, cert_reqs=ssl.CERT_REQUIRED)
            if self.config.tls_insecure:
                client.tls_insecure_set(True)

        client.reconnect_delay_set(min_delay=1, max_delay=30)
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message
        return client

    def start(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        try:
            self._client.connect_async(self.config.host, self.config.port, keepalive=60)
            self._client.loop_start()
            logger.info(
                "Iniciando conexão MQTT com %s:%s", self.config.host, self.config.port
            )
        except Exception as exc:
            self.last_error = str(exc)
            logger.error("Erro ao iniciar cliente MQTT: %s", exc)

    def stop(self) -> None:
        try:
            self._client.loop_stop()
            self._client.disconnect()
        except Exception:
            pass

    def _broadcast(self, event_type: str, data: dict[str, Any]) -> None:
        if self._loop is None or self._loop.is_closed():
            return
        event = {"type": event_type, "data": data}
        with self._sub_lock:
            queues = list(self._subscribers)
        for q in queues:
            try:
                self._loop.call_soon_threadsafe(self._enqueue_drop_oldest, q, event)
            except RuntimeError:
                pass

    @staticmethod
    def _enqueue_drop_oldest(
        q: asyncio.Queue[dict[str, Any]], event: dict[str, Any]
    ) -> None:
        if q.full():
            try:
                q.get_nowait()
            except asyncio.QueueEmpty:
                pass
        try:
            q.put_nowait(event)
        except asyncio.QueueFull:
            pass

    def subscribe_events(self) -> asyncio.Queue[dict[str, Any]]:
        q: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=200)
        with self._sub_lock:
            self._subscribers.add(q)
        return q

    def unsubscribe_events(self, q: asyncio.Queue[dict[str, Any]]) -> None:
        with self._sub_lock:
            self._subscribers.discard(q)

    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: Any,
    ) -> None:
        if reason_code.is_failure:
            self.connected = False
            self.last_error = f"Falha na conexão: {reason_code}"
            logger.warning(self.last_error)
        else:
            self.connected = True
            self.last_error = None
            self.connected_since = time.time()
            for topic in self.config.topics:
                client.subscribe(topic, qos=0)
            logger.info(
                "Conectado ao Mosquitto (%s:%s). Inscrito em: %s",
                self.config.host,
                self.config.port,
                ", ".join(self.config.topics),
            )
        self._broadcast("status", self.get_status())

    def _on_disconnect(
        self,
        client: mqtt.Client,
        userdata: Any,
        disconnect_flags: mqtt.DisconnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: Any,
    ) -> None:
        self.connected = False
        self.connected_since = None
        if reason_code != 0:
            self.last_error = f"Desconectado ({reason_code})"
        logger.warning("Desconectado do broker MQTT: %s", reason_code)
        self._broadcast("status", self.get_status())

    def _on_message(
        self, client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage
    ) -> None:
        try:
            payload_text = msg.payload.decode("utf-8")
        except UnicodeDecodeError:
            payload_text = msg.payload.hex()

        saved = self.store.add_message(
            topic=msg.topic,
            payload=payload_text,
            qos=msg.qos,
            retain=bool(msg.retain),
        )
        self._broadcast("message", saved)

    def publish(
        self, topic: str, payload: str, qos: int = 0, retain: bool = False
    ) -> dict[str, Any]:
        clean_topic = topic.strip()
        if not clean_topic:
            raise ValueError("O tópico não pode estar vazio.")
        if any(ch in clean_topic for ch in ("#", "+")):
            raise ValueError("O tópico de publicação não pode conter curingas (# ou +).")
        if qos not in (0, 1, 2):
            raise ValueError("QoS deve ser 0, 1 ou 2.")
        if not self.connected:
            raise ConnectionError("Não conectado ao broker Mosquitto.")

        info = self._client.publish(
            clean_topic, payload=payload, qos=qos, retain=retain
        )
        info.wait_for_publish(timeout=5.0)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"Falha ao publicar mensagem (código {info.rc}).")

        return {
            "ok": True,
            "topic": clean_topic,
            "qos": qos,
            "retain": retain,
            "mid": info.mid,
        }

    def get_status(self) -> dict[str, Any]:
        return {
            "connected": self.connected,
            "broker_host": self.config.host,
            "broker_port": self.config.port,
            "topics_subscribed": list(self.config.topics),
            "retention_hours": self.config.retention_hours,
            "connected_since": self.connected_since,
            "last_error": self.last_error,
        }
