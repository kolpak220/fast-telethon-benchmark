# Fast-Telethon Benchmark

Standalone Docker Compose benchmark for fast Telethon media download/upload tests.

```bash
cp .env.example .env
mkdir -p sessions downloads logs
```

Fill `.env`, put the prepared upload file at `downloads/record.mp4`, then create a session:

```bash
docker compose run --rm login
```

Run:

```bash
docker compose run --rm benchmark
```

Results are written to `logs/results.json`. With `BENCH_SEND_REPORT=1`, the script also sends an HTML summary and `results.json` to `BENCH_TARGET`.

On a VPS, keep direct mode:

```env
BENCH_PROXY_ENABLED=0
```

If Telegram starts logging `GetFileRequest flood wait`, reduce request pressure:

```env
BENCH_FAST_CONFIGS=4:512:512,8:512:512,12:512:512,20:512:512,4:1024:512,8:1024:512,12:1024:512,20:1024:512
BENCH_DOWNLOAD_REQUEST_DELAY_MS=100
```

Increase the delay to `200` or `300` if flood waits continue. The fastest config is the one with the best completed MB/s, not the highest connection count.

`BENCH_FAST_CONFIGS` supports either `workers:part_kb` for legacy same-size download/upload parts, or `workers:download_part_kb:upload_part_kb` to keep uploads at Telegram-safe sizes while testing larger download parts. When uploads are enabled, keep `upload_part_kb` at `512` or lower.
