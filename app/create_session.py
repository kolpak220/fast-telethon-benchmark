from __future__ import annotations

import asyncio
import getpass
import logging
import os
from pathlib import Path
from urllib.parse import urlparse

from python_socks import ProxyType
from telethon import TelegramClient

SESSION_DIR = Path(os.getenv("TELETHON_SESSION_DIR", "/sessions"))


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise SystemExit(f"{name} is required")
    return value


def enabled(name: str) -> bool:
    return os.getenv(name, "0").strip().lower() in {"1", "true", "yes", "on"}


def proxy_config():
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


async def main() -> None:
    logging.basicConfig(
        level=getattr(logging, os.getenv("BENCH_LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s.%(msecs)03d  %(levelname)-5s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    log = logging.getLogger("fast-telethon-login")
    SESSION_DIR.mkdir(parents=True, exist_ok=True)
    session_name = os.getenv("TELETHON_SESSION", "userbot").strip() or "userbot"
    session_path = SESSION_DIR / session_name
    client = TelegramClient(str(session_path), int(required_env("TG_API_ID")), required_env("TG_API_HASH"), proxy=proxy_config())
    await client.connect()
    try:
        if await client.is_user_authorized():
            me = await client.get_me()
            log.info("Session already authorized as %s @%s", me.id, me.username or "")
            return
        phone = os.getenv("TELETHON_PHONE", "").strip() or input("Phone number, with country code: ").strip()
        await client.send_code_request(phone)
        code = input("Telegram login code: ").strip().replace(" ", "")
        try:
            await client.sign_in(phone=phone, code=code)
        except Exception as exc:
            if exc.__class__.__name__ != "SessionPasswordNeededError":
                raise
            await client.sign_in(password=getpass.getpass("Two-step verification password: "))
        me = await client.get_me()
        log.info("Created session %s as %s @%s", session_path.with_suffix(".session"), me.id, me.username or "")
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
