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


def env_required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise SystemExit(f"{name} is required")
    return value


def enabled(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


def parse_source(value: str) -> tuple[int | str, int]:
    match = re.search(r"t\.me/c/(\d+)/(\d+)", value.strip())
    if match:
        return int(f"-100{match.group(1)}"), int(match.group(2))
    if ":" in value:
        chat_id, message_id = value.rsplit(":", 1)
        return (int(chat_id) if re.fullmatch(r"-?\d+", chat_id) else chat_id), int(message_id)
    raise SystemExit("BENCH_SOURCE must be t.me/c/<chat>/<message> or chat:message")


def parse_chat(value: str) -> int | str:
    value = value.strip()
    return int(value) if re.fullmatch(r"-?\d+", value) else value


def session_path() -> Path:
    name = os.getenv("TELETHON_SESSION", "userbot").strip() or "userbot"
    path = Path(name)
    if not path.is_absolute():
        path = SESSION_DIR / path
    path = path.with_suffix(".session")
    if not path.exists():
        raise SystemExit(f"No session at {path}; run docker compose run --rm login")
    return path


def fast_configs() -> list[dict[str, int | str]]:
    raw = os.getenv("BENCH_FAST_CONFIGS", "2:512,3:512,4:512,8:512")
    configs = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        connections, part = item.split(":", 1) if ":" in item else (item, "512")
        c = int(connections)
        p = int(part)
        if c <= 0 or p <= 0:
            raise SystemExit("BENCH_FAST_CONFIGS values must be positive")
        configs.append({"label": f"c{c}-p{p}", "connections": c, "part_size_kb": p})
    if not configs:
        raise SystemExit("BENCH_FAST_CONFIGS is empty")
    return configs


def proxy_config() -> dict[str, Any] | None:
    if not enabled("BENCH_PROXY_ENABLED"):
        return None
    parsed = urlparse(os.getenv("BENCH_PROXY_URL", "").strip())
    schemes = {"socks4": ProxyType.SOCKS4, "socks5": ProxyType.SOCKS5, "socks5h": ProxyType.SOCKS5, "http": ProxyType.HTTP}
    if parsed.scheme.lower() not in schemes or not parsed.hostname or not parsed.port:
        raise SystemExit("BENCH_PROXY_URL must be socks4://, socks5://, socks5h://, or http:// with host and port")
    proxy = {"proxy_type": schemes[parsed.scheme.lower()], "addr": parsed.hostname, "port": parsed.port, "rdns": parsed.scheme.endswith("h")}
    if parsed.username:
        proxy["username"] = parsed.username
    if parsed.password:
        proxy["password"] = parsed.password
    return proxy


def redacted_proxy() -> str | None:
    if not enabled("BENCH_PROXY_ENABLED"):
        return None
    parsed = urlparse(os.getenv("BENCH_PROXY_URL", "").strip())
    if not parsed.username:
        return os.getenv("BENCH_PROXY_URL", "").strip()
    return f"{parsed.scheme}://{parsed.username}:***@{parsed.hostname}:{parsed.port}"


def write_report(report: dict[str, Any]) -> None:
    RESULT_PATH.parent.mkdir(parents=True, exist_ok=True)
    RESULT_PATH.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")


class Tracker:
    def __init__(self, label: str) -> None:
        self.label = label
        self.started = time.monotonic()
        self.first: float | None = None
        self.current = 0
        self.total: int | None = None
        self.events: list[dict[str, Any]] = []
        self.last_log = 0.0

    def __call__(self, current: int, total: int) -> None:
        now = time.monotonic()
        if self.first is None and current:
            self.first = now
        self.current = current
        self.total = total
        if now - self.last_log >= 5 or current == total:
            self.last_log = now
            percent = current * 100 / total if total else 0.0
            elapsed = now - self.started
            print(f"{self.label}: {current}/{total} ({percent:.2f}%) after {elapsed:.1f}s", flush=True)
            self.events.append({"at_seconds": round(elapsed, 3), "current": current, "total": total, "percent": round(percent, 4)})

    def summary(self, prefix: str) -> dict[str, Any]:
        seconds = self.events[-1]["at_seconds"] if self.events else None
        return {
            f"{prefix}_seconds": seconds,
            f"{prefix}_time_to_first_progress_seconds": round(self.first - self.started, 3) if self.first else None,
            f"{prefix}_bytes": self.current,
            f"{prefix}_total_bytes": self.total,
            f"{prefix}_percent": round(self.current * 100 / self.total, 4) if self.total else 0.0,
            f"{prefix}_mbps_decimal": round((self.current / 1_000_000) / max(seconds or 0.001, 0.001), 4),
            f"{prefix}_progress_events": self.events,
        }


async def download_media(client: TelegramClient, message: Any, out_dir: Path, cfg: dict[str, Any], delay: float, tracker: Tracker) -> str:
    if not message.document:
        path = await client.download_media(message, file=str(out_dir) + os.sep, progress_callback=tracker)
        if not path:
            raise RuntimeError("download_media returned no path")
        return str(path)
    name = getattr(message.file, "name", None) or "download.bin"
    path = out_dir / name
    dc_id, location = get_input_location(message.document)
    downloader = ParallelTransferrer(client, dc_id)
    with path.open("wb") as out:
        async for chunk in downloader.download(
            location,
            message.document.size,
            part_size_kb=int(cfg["part_size_kb"]),
            connection_count=int(cfg["connections"]),
            request_delay_seconds=delay,
        ):
            out.write(chunk)
            tracker(out.tell(), message.document.size)
    return str(path)


async def upload_input_file(client: TelegramClient, path: Path, cfg: dict[str, Any], tracker: Tracker) -> Any:
    file_id = random_file_id()
    size = path.stat().st_size
    uploader = ParallelTransferrer(client)
    part_size, part_count, is_large = await uploader.init_upload(file_id, size, int(cfg["part_size_kb"]), int(cfg["connections"]))
    sent = 0
    try:
        with path.open("rb") as file:
            for part in iter(lambda: file.read(part_size), b""):
                await uploader.upload(part)
                sent += len(part)
                tracker(sent, size)
        await uploader.finish_upload()
    except Exception:
        await uploader._cleanup()
        raise
    return InputFileBig(file_id, part_count, path.name) if is_large else InputFile(file_id, part_count, path.name, "")


async def run_one(client: TelegramClient, cfg: dict[str, Any], base: dict[str, Any]) -> dict[str, Any]:
    label = str(cfg["label"])
    out_dir = DOWNLOAD_DIR / f"{label}-{uuid.uuid4().hex[:8]}"
    out_dir.mkdir(parents=True, exist_ok=True)
    started = time.time()
    result = {"label": label, "connections": cfg["connections"], "part_size_kb": cfg["part_size_kb"], "downloaded": False, "uploaded": False, "error": None}
    download_tracker = Tracker(f"{label} download")
    upload_tracker = Tracker(f"{label} upload")
    try:
        message = await client.get_messages(base["source_chat"], ids=base["source_message_id"])
        if not message or not message.media:
            raise RuntimeError("source message has no downloadable media")
        downloaded = None
        if base["download"]:
            downloaded = await asyncio.wait_for(download_media(client, message, out_dir, cfg, base["download_delay"], download_tracker), timeout=base["download_timeout"])
            result.update({"downloaded": True, "download_path": downloaded, "download_size": Path(downloaded).stat().st_size})
        if base["upload"]:
            upload_path = base["upload_path"] or (Path(downloaded) if downloaded else None)
            if upload_path is None:
                raise RuntimeError("upload requested, but no upload file and no completed download")
            input_file = await asyncio.wait_for(upload_input_file(client, upload_path, cfg, upload_tracker), timeout=base["upload_timeout"])
            sent = await client.send_file(base["target_chat"], input_file, caption=message.message or None)
            result.update({"uploaded": True, "upload_message_id": sent.id})
    except Exception as exc:
        result["error"] = repr(exc)
    finally:
        result.update(download_tracker.summary("download"))
        result.update(upload_tracker.summary("upload"))
        result["duration_seconds"] = round(time.time() - started, 3)
        if not base["keep_downloads"]:
            shutil.rmtree(out_dir, ignore_errors=True)
    return result


def status(row: dict[str, Any], upload: bool) -> str:
    if row.get("error"):
        return "ERR"
    if not row.get("downloaded"):
        return "NO-DL"
    if upload and not row.get("uploaded"):
        return "NO-UP"
    return "OK"


def best(rows: list[dict[str, Any]], key: str) -> str:
    ok = [r for r in rows if not r.get("error") and r.get(key) is not None]
    return str(min(ok, key=lambda r: r[key])["label"]) if ok else "n/a"


def summary_html(report: dict[str, Any]) -> str:
    rows = report["results"]
    lines = [f"{'Config':<12} {'Download':>10} {'MB/s':>7} {'Upload':>10} {'MB/s':>7} {'Status':>8}"]
    for row in rows:
        dl = f"{row['download_seconds']:.1f}s" if row.get("download_seconds") is not None else "-"
        up = f"{row['upload_seconds']:.1f}s" if row.get("upload_seconds") is not None else "-"
        lines.append(f"{row['label']:<12} {dl:>10} {row.get('download_mbps_decimal', 0):>7.2f} {up:>10} {row.get('upload_mbps_decimal', 0):>7.2f} {status(row, report['upload_enabled']):>8}")
    balanced_rows = [r for r in rows if not r.get("error") and r.get("download_seconds") is not None and (not report["upload_enabled"] or r.get("upload_seconds") is not None)]
    balanced = min(balanced_rows, key=lambda r: r["download_seconds"] + (r.get("upload_seconds") or 0))["label"] if balanced_rows else "n/a"
    return (
        "<b>Fast-Telethon Benchmark Results</b>\n\n"
        f"<pre>{html.escape(chr(10).join(lines))}</pre>\n\n"
        f"<b>Best download:</b> {html.escape(best(rows, 'download_seconds'))}\n"
        f"<b>Best upload:</b> {html.escape(best(rows, 'upload_seconds'))}\n"
        f"<b>Best balanced:</b> {html.escape(str(balanced))}"
    )


async def main() -> None:
    logging.basicConfig(level=getattr(logging, os.getenv("BENCH_LOG_LEVEL", "INFO").upper(), logging.INFO), format="%(asctime)s.%(msecs)03d  %(levelname)-5s  %(name)s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    source_chat, source_message_id = parse_source(os.getenv("BENCH_SOURCE", "https://t.me/c/3369127007/14"))
    upload_file = Path(os.getenv("BENCH_UPLOAD_FILE", "/downloads/record.mp4"))
    upload_enabled = enabled("BENCH_UPLOAD", "1")
    if upload_enabled and not upload_file.exists():
        raise SystemExit(f"BENCH_UPLOAD_FILE does not exist: {upload_file}")
    base = {
        "source_chat": source_chat,
        "source_message_id": source_message_id,
        "target_chat": parse_chat(os.getenv("BENCH_TARGET", "-1003966203647")),
        "download": enabled("BENCH_DOWNLOAD", "1"),
        "upload": upload_enabled,
        "upload_path": upload_file if upload_enabled else None,
        "download_timeout": float(os.getenv("BENCH_DOWNLOAD_TIMEOUT_SECONDS", "6000")),
        "upload_timeout": float(os.getenv("BENCH_UPLOAD_TIMEOUT_SECONDS", "6000")),
        "download_delay": float(os.getenv("BENCH_DOWNLOAD_REQUEST_DELAY_MS", "0")) / 1000,
        "keep_downloads": enabled("BENCH_KEEP_DOWNLOADS", "1"),
    }
    report = {
        "source": f"{source_chat}:{source_message_id}",
        "target": base["target_chat"],
        "proxy_enabled": enabled("BENCH_PROXY_ENABLED"),
        "proxy": redacted_proxy(),
        "download_request_delay_ms": int(base["download_delay"] * 1000),
        "upload_enabled": upload_enabled,
        "upload_file": str(upload_file) if upload_enabled else None,
        "fast_configs": fast_configs(),
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "results": [],
    }
    write_report(report)
    client = TelegramClient(str(session_path().with_suffix("")), int(env_required("TG_API_ID")), env_required("TG_API_HASH"), proxy=proxy_config())
    async with client:
        if not await client.is_user_authorized():
            raise SystemExit("Telethon session is not authorized; run docker compose run --rm login")
        for cfg in report["fast_configs"]:
            print(f"Benchmarking {cfg['label']} delay={report['download_request_delay_ms']}ms", flush=True)
            report["results"].append(await run_one(client, cfg, base))
            write_report(report)
        report["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        write_report(report)
        if enabled("BENCH_SEND_REPORT", "1"):
            await client.send_message(base["target_chat"], summary_html(report), parse_mode="html")
            await client.send_file(base["target_chat"], str(RESULT_PATH), caption="results.json")
    print(json.dumps(report, indent=2, ensure_ascii=False), flush=True)
    print(f"Wrote {RESULT_PATH}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
