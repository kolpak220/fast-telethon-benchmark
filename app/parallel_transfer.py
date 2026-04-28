from __future__ import annotations

import asyncio
import math
from typing import AsyncGenerator, Optional, Union

from telethon import TelegramClient, helpers, utils
from telethon.crypto import AuthKey
from telethon.network import MTProtoSender
from telethon.tl.alltlobjects import LAYER
from telethon.tl.functions import InvokeWithLayerRequest
from telethon.tl.functions.auth import ExportAuthorizationRequest, ImportAuthorizationRequest
from telethon.tl.functions.upload import GetFileRequest, SaveBigFilePartRequest, SaveFilePartRequest
from telethon.tl.types import (
    Document,
    InputDocumentFileLocation,
    InputFile,
    InputFileBig,
    InputFileLocation,
    InputPeerPhotoFileLocation,
    InputPhotoFileLocation,
)

TypeLocation = Union[
    Document,
    InputDocumentFileLocation,
    InputPeerPhotoFileLocation,
    InputFileLocation,
    InputPhotoFileLocation,
]


class DownloadSender:
    def __init__(
        self,
        client: TelegramClient,
        sender: MTProtoSender,
        file: TypeLocation,
        offset: int,
        limit: int,
        stride: int,
        count: int,
    ) -> None:
        self.client = client
        self.sender = sender
        self.request = GetFileRequest(file, offset=offset, limit=limit)
        self.stride = stride
        self.remaining = count

    async def next(self) -> Optional[bytes]:
        if not self.remaining:
            return None
        result = await self.client._call(self.sender, self.request)
        self.remaining -= 1
        self.request.offset += self.stride
        return result.bytes

    async def disconnect(self) -> None:
        await self.sender.disconnect()


class UploadSender:
    def __init__(
        self,
        client: TelegramClient,
        sender: MTProtoSender,
        file_id: int,
        part_count: int,
        big: bool,
        index: int,
        stride: int,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self.client = client
        self.sender = sender
        self.request = (
            SaveBigFilePartRequest(file_id, index, part_count, b"")
            if big
            else SaveFilePartRequest(file_id, index, b"")
        )
        self.stride = stride
        self.previous: Optional[asyncio.Task] = None
        self.loop = loop

    async def next(self, data: bytes) -> None:
        if self.previous:
            await self.previous
        self.previous = self.loop.create_task(self._next(data))

    async def _next(self, data: bytes) -> None:
        self.request.bytes = data
        await self.client._call(self.sender, self.request)
        self.request.file_part += self.stride

    async def disconnect(self) -> None:
        if self.previous:
            await self.previous
        await self.sender.disconnect()


class ParallelTransferrer:
    def __init__(self, client: TelegramClient, dc_id: Optional[int] = None) -> None:
        self.client = client
        self.loop = self.client.loop
        self.dc_id = dc_id or self.client.session.dc_id
        self.auth_key: Optional[AuthKey] = (
            None if dc_id and self.client.session.dc_id != dc_id else self.client.session.auth_key
        )
        self.senders: list[Union[DownloadSender, UploadSender]] = []
        self.upload_ticker = 0

    async def _cleanup(self) -> None:
        if self.senders:
            await asyncio.gather(*[sender.disconnect() for sender in self.senders], return_exceptions=True)
        self.senders = []

    async def _create_sender(self) -> MTProtoSender:
        dc = await self.client._get_dc(self.dc_id)
        sender = MTProtoSender(self.auth_key, loggers=self.client._log)
        await sender.connect(
            self.client._connection(
                dc.ip_address,
                dc.port,
                dc.id,
                loggers=self.client._log,
                proxy=self.client._proxy,
            )
        )
        if not self.auth_key:
            auth = await self.client(ExportAuthorizationRequest(self.dc_id))
            self.client._init_request.query = ImportAuthorizationRequest(id=auth.id, bytes=auth.bytes)
            await sender.send(InvokeWithLayerRequest(LAYER, self.client._init_request))
            self.auth_key = sender.auth_key
        return sender

    async def download(
        self,
        file: TypeLocation,
        file_size: int,
        part_size_kb: int,
        connection_count: int,
        request_delay_seconds: float = 0.0,
    ) -> AsyncGenerator[bytes, None]:
        part_size = part_size_kb * 1024
        part_count = math.ceil(file_size / part_size)
        minimum, remainder = divmod(part_count, connection_count)

        def count_for_sender() -> int:
            nonlocal remainder
            if remainder > 0:
                remainder -= 1
                return minimum + 1
            return minimum

        self.senders = [
            DownloadSender(
                self.client,
                await self._create_sender(),
                file,
                i * part_size,
                part_size,
                connection_count * part_size,
                count_for_sender(),
            )
            for i in range(connection_count)
        ]
        try:
            part = 0
            while part < part_count:
                tasks = [self.loop.create_task(sender.next()) for sender in self.senders]
                for task in tasks:
                    data = await task
                    if not data:
                        break
                    yield data
                    part += 1
                if request_delay_seconds > 0:
                    await asyncio.sleep(request_delay_seconds)
        finally:
            await self._cleanup()

    async def init_upload(
        self,
        file_id: int,
        file_size: int,
        part_size_kb: int,
        connection_count: int,
    ) -> tuple[int, int, bool]:
        part_size = part_size_kb * 1024
        part_count = (file_size + part_size - 1) // part_size
        is_large = file_size > 10 * 1024 * 1024
        self.senders = [
            UploadSender(
                self.client,
                await self._create_sender(),
                file_id,
                part_count,
                is_large,
                i,
                connection_count,
                self.loop,
            )
            for i in range(connection_count)
        ]
        return part_size, part_count, is_large

    async def upload(self, part: bytes) -> None:
        await self.senders[self.upload_ticker].next(part)
        self.upload_ticker = (self.upload_ticker + 1) % len(self.senders)

    async def finish_upload(self) -> None:
        await self._cleanup()


def get_input_location(document: Document):
    return utils.get_input_location(document)


def random_file_id() -> int:
    return helpers.generate_random_long()


__all__ = ["InputFile", "InputFileBig", "ParallelTransferrer", "get_input_location", "random_file_id"]
