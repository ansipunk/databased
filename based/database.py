from contextlib import asynccontextmanager
from types import TracebackType
from typing import AsyncGenerator, Optional, Type

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
            from based.backends.sqlite import SQLite
            sqlite_url = url_parts[1][1:]
            self._backend = SQLite(
                sqlite_url, force_rollback=force_rollback,
            )
        elif schema == "postgresql":
            from based.backends.postgres import PostgreSQL
            self._backend = PostgreSQL(url, force_rollback=force_rollback)
        else:
            raise ValueError(f"Unknown database schema: {schema}")

    async def connect(self) -> None:
        await self._backend.connect()

    async def disconnect(self) -> None:
        await self._backend.disconnect()

    @asynccontextmanager
    async def session(self) -> AsyncGenerator[Session, None]:
        async with self._backend.session() as session:
            yield session

    async def __aenter__(self) -> "Database":
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        await self.disconnect()

        if exc_val is not None:
            raise exc_val
