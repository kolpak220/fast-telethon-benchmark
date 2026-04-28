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

## Practical VPS integration

Your VPS results show that download speed is mostly capped around `5.0-5.7 MB/s`, while upload improves up to about 20-24 connections and then stops gaining much. For a normal download-and-upload pipeline, use `24:512:512` as the default. It gave the best upload result in this run:

```env
BENCH_PROXY_ENABLED=0
BENCH_FAST_CONFIGS=24:512:512
BENCH_DOWNLOAD_REQUEST_DELAY_MS=0
```

If you want a slightly more conservative setting with almost the same result, use:

```env
BENCH_FAST_CONFIGS=20:512:512
```

For download-heavy jobs where upload speed matters less, `4:1024:512` had the best download result on this VPS:

```env
BENCH_FAST_CONFIGS=4:1024:512
```

Recommended integration pattern:

1. Keep `BENCH_PROXY_ENABLED=0` on the VPS.
2. Start production runs with `BENCH_FAST_CONFIGS=24:512:512`.
3. Keep upload parts at `512 KB`; larger upload parts are intentionally blocked when `BENCH_UPLOAD=1`.
4. If Telegram flood waits appear, set `BENCH_DOWNLOAD_REQUEST_DELAY_MS=100`, then retry with `200` if needed.
5. Re-run the benchmark after changing VPS region, provider, file size, or Telegram account, because the best connection count is network-dependent.

## Integrate Fast Uploads

To use this upload method in another Telethon project, copy `app/parallel_transfer.py` into your project and import it near your existing `TelegramClient` code.

```python
from pathlib import Path

from parallel_transfer import InputFile, InputFileBig, ParallelTransferrer, random_file_id


async def fast_upload_file(client, path: str | Path, connections: int = 24, part_size_kb: int = 512):
    path = Path(path)
    file_id = random_file_id()
    size = path.stat().st_size
    uploader = ParallelTransferrer(client)
    part_size, part_count, is_large = await uploader.init_upload(
        file_id=file_id,
        file_size=size,
        part_size_kb=part_size_kb,
        connection_count=connections,
    )

    try:
        with path.open("rb") as file:
            for part in iter(lambda: file.read(part_size), b""):
                await uploader.upload(part)
        await uploader.finish_upload()
    except Exception:
        await uploader._cleanup()
        raise

    if is_large:
        return InputFileBig(file_id, part_count, path.name)
    return InputFile(file_id, part_count, path.name, "")
```

Then pass the returned input file to `send_file`:

```python
uploaded = await fast_upload_file(client, "record.mp4")
await client.send_file(target_chat, uploaded, caption="Uploaded with parallel transfer")
```

For your VPS, start with `connections=24` and `part_size_kb=512`. Use `connections=20` if you want a slightly safer default with nearly the same measured upload speed.
