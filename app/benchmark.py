from __future__ import annotations

import asyncio
import html
import json
import logging
import os
import re
import shutil
import time
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from python_socks import ProxyType
from telethon import TelegramClient

from parallel_transfer import InputFile, InputFileBig, ParallelTransferrer, get_input_location, random_file_id

SESSION_DIR = Path(os.getenv("TELETHON_SESSION_DIR", "/sessions"))
DOWNLOAD_DIR = Path(os.getenv("BENCH_DOWNLOAD_DIR", "/downloads/bench-runs"))
RESULT_PATH = Path(os.getenv("BENCH_RESULT_PATH", "/logs/results.json"))
DEFAULT_SOURCE_URL = "https://t.me/c/3369127007/14"


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise SystemExit(f"{name} is required")
    return value


def enabled(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


def parse_source(value: str) -> tuple[int | str, int]:
    value = value.strip()
    match = re.search(r"t\.me/c/(\d+)/(\d+)", value)
    if match:
        return int(f"-100{match.group(1)}"), int(match.group(2))
    if ":" in value:
        chat_id, message_id = value.rsplit(":", 1)
        chat: int | str = int(chat_id) if re.fullmatch(r"-?\d+", chat_id) else chat_id
        return chat, int(message_id)
    raise SystemExit("BENCH_SOURCE must be a t.me/c/<internal_id>/<message_id> URL or chat:message")


def parse_chat(value: str) -> int | str:
    value = value.strip()
    if not value:
        raise SystemExit("BENCH_TARGET is required")
    return int(value) if re.fullmatch(r"-?\d+", value) else value


def find_session_path() -> Path:
    session_name = os.getenv("TELETHON_SESSION", "").strip()
    if session_name:
        path = Path(session_name)
        if not path.is_absolute():
            path = SESSION_DIR / path
        return path.with_suffix(".session")

    sessions = sorted(SESSION_DIR.glob("*.session"))
    if len(sessions) == 1:
        return sessions[0]
    if not sessions:
        raise SystemExit(f"No .session files found in {SESSION_DIR}; run docker compose run --rm login")
    raise SystemExit("Multiple sessions found; set TELETHON_SESSION")


def parse_fast_configs() -> list[dict[str, int | str]]:
    raw = os.getenv("BENCH_FAST_CONFIGS", "4:512,8:512,12:512,20:512").strip()
    configs: list[dict[str, int | str]] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        if ":" in item:
            connections_raw, part_size_raw = item.split(":", 1)
        else:
            connections_raw = item
            part_size_raw = "512"
        connections = int(connections_raw)
        part_size_kb = int(part_size_raw)
        if connections <= 0 or part_size_kb <= 0:
            raise SystemExit("BENCH_FAST_CONFIGS values must be positive")
        configs.append(
            {
                "connections": connections,
                "part_size_kb": part_size_kb,
                "label": f"c{connections}-p{part_size_kb}",
            }
        )
    if not configs:
        raise SystemExit("BENCH_FAST_CONFIGS must contain at least one config")
    return configs


def proxy_config() -> dict[str, Any] | None:
    if not enabled("BENCH_PROXY_ENABLED"):
        return None
    proxy_url = os.getenv("BENCH_PROXY_URL", "").strip()
    parsed = urlparse(proxy_url)
    scheme = parsed.scheme.lower()
    proxy_types = {
        "socks4": ProxyType.SOCKS4,
        "socks5": ProxyType.SOCKS5,
        "socks5h": ProxyType.SOCKS5,
        "http": ProxyType.HTTP,
    }
    if scheme not in proxy_types:
        raise SystemExit("BENCH_PROXY_URL must use socks4://, socks5://, socks5h://, or http://")
    if not parsed.hostname or not parsed.port:
        raise SystemExit("BENCH_PROXY_URL must include host and port")
    proxy: dict[str, Any] = {
        "proxy_type": proxy_types[scheme],
        "addr": parsed.hostname,
        "port": parsed.port,
        "rdns": scheme.endswith("h"),
    }
    if parsed.username:
        proxy["username"] = parsed.username
    if parsed.password:
        proxy["password"] = parsed.password
    return proxy


def redacted_proxy_url() -> str | None:
    if not enabled("BENCH_PROXY_ENABLED"):
        return None
    proxy_url = os.getenv("BENCH_PROXY_URL", "").strip()
    parsed = urlparse(proxy_url)
    if not parsed.username:
        return proxy_url
    host = parsed.hostname or ""
    port = f":{parsed.port}" if parsed.port else ""
    return f"{parsed.scheme}://{parsed.username}:***@{host}{port}"


def upload_file_path() -> Path | None:
    raw = os.getenv("BENCH_UPLOAD_FILE", "").strip()
    if not raw:
        return None
    path = Path(raw)
    if not path.exists() or not path.is_file():
        raise SystemExit(f"BENCH_UPLOAD_FILE does not exist or is not a file: {path}")
    return path


class ProgressTracker:
    def __init__(self, label: str) -> None:
        self.label = label
        self.started = time.monotonic()
        self.first_at: float | None = None
        self.last_current = 0
        self.last_total: int | None = None
        self.last_percent = 0.0
        self.events: list[dict[str, Any]] = []
        self._last_event_at: float | None = None

    def __call__(self, current: int, total: int) -> None:
        now = time.monotonic()
        if self.first_at is None and current > 0:
            self.first_at = now
        self.last_current = current
        self.last_total = total
        self.last_percent = (current * 100 / total) if total else 0.0
        if self._last_event_at is None or now - self._last_event_at >= 5 or current == total:
            self._last_event_at = now
            elapsed = round(now - self.started, 1)
            print(
                f"{self.label}: {current}/{total} ({self.last_percent:.2f}%) after {elapsed}s",
                flush=True,
            )
            self.events.append(
                {
                    "at_seconds": round(now - self.started, 3),
                    "current": current,
                    "total": total,
                    "percent": round(self.last_percent, 4),
                }
            )

    def summary(self, prefix: str) -> dict[str, Any]:
        return {
            f"{prefix}_time_to_first_progress_seconds": (
                round(self.first_at - self.started, 3) if self.first_at is not None else None
            ),
            f"{prefix}_seconds": round(self.events[-1]["at_seconds"], 3) if self.events else None,
            f"{prefix}_percent": round(self.last_percent, 4),
            f"{prefix}_bytes": self.last_current,
            f"{prefix}_total_bytes": self.last_total,
            f"{prefix}_progress_events": self.events,
            f"{prefix}_mbps_decimal": (
                round((self.last_current / 1_000_000) / max(0.001, self.events[-1]["at_seconds"]), 4)
                if self.events
                else 0.0
            ),
        }


def write_report(report: dict[str, Any]) -> None:
    RESULT_PATH.parent.mkdir(parents=True, exist_ok=True)
    RESULT_PATH.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")


async def fast_download(
    client: TelegramClient,
    message: Any,
    download_dir: Path,
    connections: int,
    part_size_kb: int,
    tracker: ProgressTracker,
) -> str:
    if not message.document:
        downloaded = await client.download_media(message, file=str(download_dir) + os.sep, progress_callback=tracker)
        if not downloaded:
            raise RuntimeError("download_media returned no path")
        return str(downloaded)

    name = getattr(message.file, "name", None) or "download.bin"
    path = download_dir / name
    size = message.document.size
    dc_id, location = get_input_location(message.document)
    downloader = ParallelTransferrer(client, dc_id)
    with path.open("wb") as out:
        async for chunk in downloader.download(location, size, part_size_kb=part_size_kb, connection_count=connections):
            out.write(chunk)
            tracker(out.tell(), size)
    if path.stat().st_size <= 0:
        raise RuntimeError(f"fast download produced empty file: {path}")
    return str(path)


async def fast_upload_input_file(
    client: TelegramClient,
    path: Path,
    connections: int,
    part_size_kb: int,
    tracker: ProgressTracker,
) -> Any:
    file_id = random_file_id()
    file_size = path.stat().st_size
    uploader = ParallelTransferrer(client)
    part_size, part_count, is_large = await uploader.init_upload(
        file_id,
        file_size,
        part_size_kb=part_size_kb,
        connection_count=connections,
    )
    uploaded_bytes = 0
    try:
        with path.open("rb") as file:
            for part in iter(lambda: file.read(part_size), b""):
                await uploader.upload(part)
                uploaded_bytes += len(part)
                tracker(uploaded_bytes, file_size)
        await uploader.finish_upload()
    except Exception:
        await uploader._cleanup()
        raise

    if is_large:
        return InputFileBig(file_id, part_count, path.name)
    return InputFile(file_id, part_count, path.name, "")


async def run_config(
    client: TelegramClient,
    config: dict[str, int | str],
    source_chat: int | str,
    source_message_id: int,
    target_chat: int | str,
    upload_path: Path | None,
    base: dict[str, Any],
) -> dict[str, Any]:
    label = str(config["label"])
    connections = int(config["connections"])
    part_size_kb = int(config["part_size_kb"])
    run_id = f"{label}-{uuid.uuid4().hex[:8]}"
    download_dir = DOWNLOAD_DIR / run_id
    download_dir.mkdir(parents=True, exist_ok=True)
    started_at = time.time()

    result: dict[str, Any] = {
        "label": label,
        "connections": connections,
        "part_size_kb": part_size_kb,
        "download_dir": str(download_dir),
        "upload_file": str(upload_path) if upload_path else None,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(started_at)),
        "duration_seconds": None,
        "downloaded": False,
        "uploaded": False,
        "download_path": None,
        "download_size": 0,
        "upload_message_id": None,
        "error": None,
    }

    print(f"Benchmarking fast-telethon {label}", flush=True)
    download_tracker = ProgressTracker(f"{label} download")
    upload_tracker = ProgressTracker(f"{label} upload")

    try:
        message = await client.get_messages(source_chat, ids=source_message_id)
        if not message or not message.media:
            raise RuntimeError("source message has no downloadable media")

        downloaded: str | None = None
        if base["download"]:
            downloaded = await asyncio.wait_for(
                fast_download(client, message, download_dir, connections, part_size_kb, download_tracker),
                timeout=base["download_timeout"],
            )
            result["downloaded"] = True
            result["download_path"] = downloaded
            result["download_size"] = Path(downloaded).stat().st_size
        else:
            result["download_skipped_reason"] = "BENCH_DOWNLOAD=0"

        if base["upload"]:
            effective_upload_path = upload_path or (Path(downloaded) if downloaded else None)
            if effective_upload_path is None:
                raise RuntimeError("upload requested, but no BENCH_UPLOAD_FILE and no completed download")
            uploaded_file = await asyncio.wait_for(
                fast_upload_input_file(client, effective_upload_path, connections, part_size_kb, upload_tracker),
                timeout=base["upload_timeout"],
            )
            sent = await client.send_file(target_chat, uploaded_file, caption=message.message or None)
            result["uploaded"] = True
            result["upload_message_id"] = sent.id
        else:
            result["upload_skipped_reason"] = "BENCH_UPLOAD=0"

    except asyncio.TimeoutError:
        result["error"] = (
            f"timed out after download={base['download_timeout']}s upload={base['upload_timeout']}s"
        )
    except Exception as exc:
        result["error"] = repr(exc)
    finally:
        result.update(download_tracker.summary("download"))
        result.update(upload_tracker.summary("upload"))
        result["duration_seconds"] = round(time.time() - started_at, 3)
        if not base["keep_downloads"]:
            shutil.rmtree(download_dir, ignore_errors=True)

    return result


def status_for(result: dict[str, Any], upload_enabled: bool) -> str:
    if result.get("error"):
        return "ERR"
    if not result.get("downloaded"):
        return "NO-DL"
    if upload_enabled and not result.get("uploaded"):
        return "NO-UP"
    return "OK"


def best_by(results: list[dict[str, Any]], metric: str) -> str:
    candidates = [item for item in results if item.get(metric) is not None and not item.get("error")]
    if not candidates:
        return "n/a"
    return str(min(candidates, key=lambda item: item[metric])["label"])


def best_balanced(results: list[dict[str, Any]], upload_enabled: bool) -> str:
    candidates = [item for item in results if not item.get("error") and item.get("download_seconds") is not None]
    if upload_enabled:
        candidates = [item for item in candidates if item.get("upload_seconds") is not None]
    if not candidates:
        return "n/a"
    if not upload_enabled:
        return str(min(candidates, key=lambda item: item["download_seconds"])["label"])
    return str(min(candidates, key=lambda item: item["download_seconds"] + item["upload_seconds"])["label"])


def html_summary(report: dict[str, Any]) -> str:
    results = report["results"]
    upload_enabled = bool(report["upload_enabled"])
    lines = [
        f"{'Config':<12} {'Download':>10} {'MB/s':>7} {'Upload':>10} {'MB/s':>7} {'Status':>8}",
    ]
    for item in results:
        download = f"{item['download_seconds']:.1f}s" if item.get("download_seconds") is not None else "-"
        upload = f"{item['upload_seconds']:.1f}s" if item.get("upload_seconds") is not None else "-"
        lines.append(
            f"{item['label']:<12} "
            f"{download:>10} "
            f"{item.get('download_mbps_decimal', 0):>7.2f} "
            f"{upload:>10} "
            f"{item.get('upload_mbps_decimal', 0):>7.2f} "
            f"{status_for(item, upload_enabled):>8}"
        )

    best_download = best_by(results, "download_seconds")
    best_upload = best_by(results, "upload_seconds") if upload_enabled else "n/a"
    balanced = best_balanced(results, upload_enabled)

    return (
        "<b>Fast-Telethon Benchmark Results</b>\n\n"
        f"<pre>{html.escape(chr(10).join(lines))}</pre>\n\n"
        f"<b>Best download:</b> {html.escape(best_download)}\n"
        f"<b>Best upload:</b> {html.escape(best_upload)}\n"
        f"<b>Best balanced:</b> {html.escape(balanced)}"
    )


async def send_report(client: TelegramClient, target_chat: int | str, report: dict[str, Any]) -> None:
    summary = html_summary(report)
    await client.send_message(target_chat, summary, parse_mode="html")
    await client.send_file(target_chat, str(RESULT_PATH), caption="results.json")


async def main() -> None:
    logging.basicConfig(
        level=getattr(logging, os.getenv("BENCH_LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s.%(msecs)03d  %(levelname)-5s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    RESULT_PATH.parent.mkdir(parents=True, exist_ok=True)

    source_chat, source_message_id = parse_source(os.getenv("BENCH_SOURCE", DEFAULT_SOURCE_URL))
    target_chat = parse_chat(os.getenv("BENCH_TARGET", "-1003966203647"))
    session_path = find_session_path()
    upload_enabled = enabled("BENCH_UPLOAD", "1")
    upload_path = upload_file_path() if upload_enabled and os.getenv("BENCH_UPLOAD_FILE", "").strip() else None
    proxy = proxy_config()

    base = {
        "download": enabled("BENCH_DOWNLOAD", "1"),
        "upload": upload_enabled,
        "send_report": enabled("BENCH_SEND_REPORT", "1"),
        "keep_downloads": enabled("BENCH_KEEP_DOWNLOADS", "1"),
        "download_timeout": float(os.getenv("BENCH_DOWNLOAD_TIMEOUT_SECONDS", "6000")),
        "upload_timeout": float(os.getenv("BENCH_UPLOAD_TIMEOUT_SECONDS", "6000")),
    }

    report: dict[str, Any] = {
        "source": f"{source_chat}:{source_message_id}",
        "target": target_chat,
        "session": str(session_path),
        "proxy_enabled": proxy is not None,
        "proxy": redacted_proxy_url(),
        "download_enabled": base["download"],
        "upload_enabled": base["upload"],
        "upload_file": str(upload_path) if upload_path else None,
        "download_timeout_seconds": base["download_timeout"],
        "upload_timeout_seconds": base["upload_timeout"],
        "fast_configs": parse_fast_configs(),
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "results": [],
    }
    write_report(report)

    client = TelegramClient(
        str(session_path.with_suffix("")),
        int(required_env("TG_API_ID")),
        required_env("TG_API_HASH"),
        proxy=proxy,
    )

    async with client:
        if not await client.is_user_authorized():
            raise SystemExit("Telethon session is not authorized; run docker compose run --rm login")

        for config in report["fast_configs"]:
            result = await run_config(
                client,
                config,
                source_chat,
                source_message_id,
                target_chat,
                upload_path,
                base,
            )
            report["results"].append(result)
            write_report(report)

        report["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        write_report(report)

        if base["send_report"]:
            await send_report(client, target_chat, report)

    print(json.dumps(report, indent=2, ensure_ascii=False), flush=True)
    print(f"Wrote {RESULT_PATH}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
