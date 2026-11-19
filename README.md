# Visualizador Web MQTT (Mosquitto)

Aplicação web leve para monitorar os canais (tópicos) ativos nas **últimas 4 horas** de um servidor Mosquitto e enviar mensagens diretamente pela interface web.

## Funcionalidades

- **Canais ativos (últimas 4h):** lista automaticamente todos os tópicos que receberam mensagens nas últimas 4 horas, com contagem de mensagens, horário da última atividade e prévia do último payload.
- **Limpeza automática:** mensagens com mais de 4 horas são removidas automaticamente da janela ativa e do banco SQLite local.
- **Envio de mensagens:** publique mensagens em qualquer tópico com seleção de `QoS` (`0`, `1`, `2`), `Retain` e formatador de JSON.
- **Tempo real:** atualização instantânea via Server-Sent Events (SSE) assim que o Mosquitto recebe novas mensagens.
- **Porta configurável:** roda por padrão na porta **`1882`** (evitando conflito com a porta `1883` do Mosquitto), podendo ser alterada via `.env` ou argumento `--port`.
- **Acesso protegido por senha:** login único configurável via `.env` (`WEB_AUTH_USER` / `WEB_AUTH_PASSWORD`), com opção de alterar a senha diretamente pela interface web (botão **Alterar Senha**) a qualquer momento.
- **Segurança no Git:** o arquivo `.gitignore` bloqueia `.env`, credenciais, certificados, bancos locais (`data/`, `*.db`) e ambiente virtual (`venv/`).

## Instalação

```bash
python -m venv venv
```

Ative o ambiente virtual:
- **Linux / macOS:** `source venv/bin/activate`
- **Windows (PowerShell):** `.\venv\Scripts\Activate.ps1`

Instale as dependências:

```bash
pip install -r requirements.txt
```

## Configuração (sem enviar senhas ao Git)

Copie o arquivo modelo `.env.example` para `.env`:

```bash
cp .env.example .env
```

Edite o arquivo `.env` com os dados do seu servidor Mosquitto:

```ini
# Porta do visualizador web (padrão 1882)
WEB_HOST=0.0.0.0
WEB_PORT=1882

# Acesso ao visualizador web
WEB_AUTH_USER=admin
WEB_AUTH_PASSWORD=troque_esta_senha

# Servidor Mosquitto já instalado
MQTT_HOST=localhost
MQTT_PORT=1883
MQTT_USERNAME=seu_usuario
MQTT_PASSWORD=sua_senha
MQTT_TOPICS=#
RETENTION_HOURS=4
```

> **Importante:** o arquivo `.env` está no `.gitignore` e nunca deve ser adicionado ao Git. A senha de acesso ao visualizador pode ser alterada a qualquer momento pela interface web (botão **Alterar Senha**, que atualiza o `.env` automaticamente) ou editando `WEB_AUTH_PASSWORD` diretamente no `.env`.

## Execução

Rodando na porta padrão configurada no `.env` (`1882`):

```bash
python app.py
```

Alterando a porta do visualizador diretamente na execução:

```bash
python app.py --port 8080
```

Ou via variável de ambiente:

```bash
WEB_PORT=8080 python app.py
```

Acesse no navegador: `http://localhost:1882` (ou `http://IP_DO_SERVIDOR:1882`).

## Testes

```bash
pytest -v
```
