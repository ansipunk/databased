from types import TracebackType
from typing import Optional, Type

from based.backends import Backend, Session


class Database:
    _backend: Backend
    _force_rollback: bool

    def __init__(self, url: str, *, force_rollback: bool = False) -> None:
        url_parts = url.split("://")
        if len(url_parts) != 2:
            raise ValueError("Invalid database URL")
        schema = url_parts[0]

        if schema == "sqlite":
            from based.backends.sqlite import SqliteBackend
            sqlite_url = url_parts[1][1:]
            self._backend = SqliteBackend(
                sqlite_url, force_rollback=force_rollback,
            )

    async def connect(self) -> "Database":
        await self._backend.connect()
        return self

    async def disconnect(self) -> None:
        await self._backend.disconnect()

    def session(self) -> Session:
        return self._backend.session()

    async def __aenter__(self) -> "Database":
        return await self.connect()

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        await self.disconnect()

        if exc_val is not None:
            raise exc_val
