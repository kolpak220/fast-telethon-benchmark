# Fast-Telethon Benchmark

Standalone Docker Compose benchmark for fast Telethon media download/upload tests.

## Setup

```bash
cp .env.example .env
mkdir -p sessions downloads logs
```

Fill `.env` with `TG_API_ID`, `TG_API_HASH`, source, target, and benchmark settings.

Place the prepared upload file at:

```bash
downloads/record.mp4
```

## Create Session

```bash
docker compose run --rm login
```

This creates `sessions/userbot.session` by default.

## Run Benchmark

```bash
docker compose run --rm benchmark
```

The benchmark writes:

```text
logs/results.json
```

When `BENCH_SEND_REPORT=1`, it also sends an HTML summary message and the `results.json` file to `BENCH_TARGET`.

## Proxy

VPS default is direct Telegram access:

```env
BENCH_PROXY_ENABLED=0
```

To use a proxy:

```env
BENCH_PROXY_ENABLED=1
BENCH_PROXY_URL=socks5://host.docker.internal:8897
```

Supported schemes: `socks4://`, `socks5://`, `socks5h://`, `http://`.
