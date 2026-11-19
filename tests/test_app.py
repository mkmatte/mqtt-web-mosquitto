"""Testes automatizados para o visualizador web MQTT Mosquitto."""

from __future__ import annotations

import sys
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import AuthManager, create_app, resolve_web_host_port
from mqtt_service import MessageStore, MqttConfig, MqttService


def test_message_store_4h_window_and_prune(tmp_path: Path) -> None:
    db_file = tmp_path / "test_mqtt.db"
    store = MessageStore(str(db_file), retention_hours=4.0)
    try:
        now = 1_700_000_000.0
        # Mensagem antiga (4h + 10s atrás) -> deve expirar
        store.add_message(
            "sensores/antigo", "old", qos=0, retain=False, timestamp=now - (4 * 3600 + 10)
        )
        # Mensagens dentro das últimas 4h
        store.add_message(
            "sensores/sala", '{"temp": 22.5}', qos=0, retain=False, timestamp=now - 3600
        )
        store.add_message(
            "sensores/sala", '{"temp": 23.1}', qos=1, retain=True, timestamp=now - 10
        )
        store.add_message(
            "atuadores/luz", "ON", qos=0, retain=False, timestamp=now - 120
        )

        channels = store.get_active_channels(now=now)
        topics = [c["topic"] for c in channels]
        assert topics == ["sensores/sala", "atuadores/luz"]
        assert channels[0]["message_count"] == 2
        assert channels[0]["last_payload"] == '{"temp": 23.1}'
        assert channels[0]["last_retain"] is True

        filtered = store.get_active_channels(search="atuadores", now=now)
        assert [c["topic"] for c in filtered] == ["atuadores/luz"]

        recent_all = store.get_recent_messages(now=now)
        assert len(recent_all) == 3

        recent_sala = store.get_recent_messages(topic="sensores/sala", now=now)
        assert len(recent_sala) == 2

        removed = store.prune_expired(now=now)
        assert removed == 1
    finally:
        store.close()


def test_resolve_web_host_port_defaults_and_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("WEB_HOST", raising=False)
    monkeypatch.delenv("WEB_PORT", raising=False)

    assert resolve_web_host_port([]) == ("0.0.0.0", 1882)

    monkeypatch.setenv("WEB_HOST", "127.0.0.1")
    monkeypatch.setenv("WEB_PORT", "1900")
    assert resolve_web_host_port([]) == ("127.0.0.1", 1900)

    assert resolve_web_host_port(["--host", "0.0.0.0", "--port", "8085"]) == (
        "0.0.0.0",
        8085,
    )
    assert resolve_web_host_port(["-p", "1885"]) == ("127.0.0.1", 1885)


def test_api_routes_and_publish(tmp_path: Path) -> None:
    db_file = tmp_path / "api_test.db"
    store = MessageStore(str(db_file), retention_hours=4.0)
    config = MqttConfig(host="127.0.0.1", port=1883, db_path=str(db_file))
    service = MqttService(config=config, store=store)

    now = time.time()
    store.add_message("casa/cozinha/temp", "24.0", timestamp=now - 30)
    store.add_message("casa/sala/temp", "21.5", timestamp=now - 10)

    # Simula cliente paho conectado para testar publicação
    fake_info = MagicMock()
    fake_info.rc = 0
    fake_info.mid = 42
    service._client.publish = MagicMock(return_value=fake_info)
    service.connected = True

    auth = AuthManager(
        username="test_admin",
        password="test_pass_123",
        env_path=tmp_path / ".env",
    )
    test_app = create_app(
        store=store, mqtt_service=service, start_mqtt=False, auth_manager=auth
    )

    try:
        with TestClient(test_app) as client:
            home = client.get("/")
            assert home.status_code == 200
            assert "Visualizador MQTT" in home.text

            login = client.post(
                "/api/auth/login",
                json={"username": "test_admin", "password": "test_pass_123"},
            )
            assert login.status_code == 200
            assert login.json()["ok"] is True

            status = client.get("/api/status")
            assert status.status_code == 200
            status_data = status.json()
            assert status_data["connected"] is True
            assert status_data["broker_port"] == 1883
            assert status_data["active_channels_count"] == 2

            channels = client.get("/api/channels")
            assert channels.status_code == 200
            ch_data = channels.json()
            assert len(ch_data["channels"]) == 2
            assert ch_data["channels"][0]["topic"] == "casa/sala/temp"

            messages = client.get("/api/messages", params={"topic": "casa/cozinha/temp"})
            assert messages.status_code == 200
            assert len(messages.json()["messages"]) == 1

            pub_ok = client.post(
                "/api/publish",
                json={
                    "topic": "casa/sala/cmd",
                    "payload": '{"ligar": true}',
                    "qos": 1,
                    "retain": False,
                },
            )
            assert pub_ok.status_code == 200
            assert pub_ok.json()["ok"] is True
            assert pub_ok.json()["mid"] == 42
            service._client.publish.assert_called_once_with(
                "casa/sala/cmd", payload='{"ligar": true}', qos=1, retain=False
            )

            # Tópico inválido com curinga
            pub_bad = client.post(
                "/api/publish",
                json={"topic": "casa/#", "payload": "teste"},
            )
            assert pub_bad.status_code == 400

            # Broker desconectado -> 503
            service.connected = False
            pub_offline = client.post(
                "/api/publish",
                json={"topic": "casa/sala/cmd", "payload": "teste"},
            )
            assert pub_offline.status_code == 503
    finally:
        store.close()


def test_auth_login_change_password_and_logout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("WEB_AUTH_PASSWORD", raising=False)
    db_file = tmp_path / "auth_test.db"
    env_file = tmp_path / ".env"
    env_file.write_text("WEB_AUTH_USER=test_admin\nWEB_AUTH_PASSWORD=initial_pass\n", encoding="utf-8")

    store = MessageStore(str(db_file), retention_hours=4.0)
    config = MqttConfig(host="127.0.0.1", port=1883, db_path=str(db_file))
    service = MqttService(config=config, store=store)
    auth = AuthManager(
        username="test_admin", password="initial_pass", env_path=env_file
    )

    test_app = create_app(
        store=store, mqtt_service=service, start_mqtt=False, auth_manager=auth
    )

    try:
        with TestClient(test_app) as client:
            # Rotas protegidas exigem autenticação
            assert client.get("/api/status").status_code == 401
            assert client.get("/api/channels").status_code == 401
            assert client.get("/api/messages").status_code == 401
            assert (
                client.post(
                    "/api/publish", json={"topic": "casa/luz", "payload": "ON"}
                ).status_code
                == 401
            )

            # Login inválido -> 401
            bad_login = client.post(
                "/api/auth/login",
                json={"username": "test_admin", "password": "wrong_password"},
            )
            assert bad_login.status_code == 401

            # Login válido -> 200 e cookie de sessão
            ok_login = client.post(
                "/api/auth/login",
                json={"username": "test_admin", "password": "initial_pass"},
            )
            assert ok_login.status_code == 200
            assert "mqtt_vis_session" in client.cookies
            assert client.get("/api/auth/me").status_code == 200
            assert client.get("/api/status").status_code == 200

            # Alterar senha com senha atual errada -> 401
            bad_change = client.post(
                "/api/auth/change-password",
                json={
                    "current_password": "wrong_password",
                    "new_password": "new_secure_pass",
                },
            )
            assert bad_change.status_code == 401

            # Alterar senha com sucesso -> persiste no .env
            ok_change = client.post(
                "/api/auth/change-password",
                json={
                    "current_password": "initial_pass",
                    "new_password": "new_secure_pass",
                },
            )
            assert ok_change.status_code == 200
            assert "new_secure_pass" in env_file.read_text(encoding="utf-8")

            # Logout encerra a sessão
            assert client.post("/api/auth/logout").status_code == 200
            assert client.get("/api/status").status_code == 401

            # Senha antiga não funciona mais; nova senha funciona
            old_login = client.post(
                "/api/auth/login",
                json={"username": "test_admin", "password": "initial_pass"},
            )
            assert old_login.status_code == 401

            new_login = client.post(
                "/api/auth/login",
                json={"username": "test_admin", "password": "new_secure_pass"},
            )
            assert new_login.status_code == 200
            assert client.get("/api/status").status_code == 200
    finally:
        store.close()
