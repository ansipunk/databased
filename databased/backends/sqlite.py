from typing import Any, Optional, Union

import aiosqlite
from sqlalchemy.sql import ClauseElement
from sqlalchemy.dialects import sqlite

from databased.backends import DatabaseBackend, SessionBackend
from databased.errors import DatabaseAlreadyConnectedError, DatabaseNotConnectedError


class SqliteDatabaseBackend(DatabaseBackend):
    _url: str
    _force_rollback: bool
    _force_rollback_session: Optional["SessionBackend"] = None
    _conn: aiosqlite.Connection
    _connected: bool = False

    def __init__(self, url: str, *, force_rollback: bool = False) -> None:
        self._url = url
        self._force_rollback = force_rollback
        self._conn = aiosqlite.connect(url, isolation_level=None)

    async def _connect(self) -> None:
        if self._connected:
            raise DatabaseAlreadyConnectedError

        self._connected = True

    async def _disconnect(self) -> None:
        if not self._connected:
            raise DatabaseNotConnectedError

        self._connected = False

    def _get_session(self) -> "SqliteSessionBackend":
        if not self._connected:
            raise DatabaseNotConnectedError

        return SqliteSessionBackend(
            self._conn,
            is_root=True,
            force_rollback=self._force_rollback,
        )


class SqliteSessionBackend(SessionBackend):
    _conn: aiosqlite.Connection
    _is_root: bool
    _force_rollback: bool

    def __init__(
        self,
        conn: aiosqlite.Connection,
        *args: list[Any],
        is_root: bool = False,
        force_rollback: bool = False,
        **kwargs: dict[str, Any],
    ) -> None:
        self._conn = conn
        self._is_root = is_root
        self._force_rollback = force_rollback

    def _compile_query(
        self, query: ClauseElement,
    ) -> tuple[str, Optional[Union[dict[str, Any], list[Any]]]]:
        compiled_query = query.compile(dialect=sqlite.dialect())  # type: ignore
        str_query = str(compiled_query)

        if not compiled_query.params:
            return str_query, None

        params = [
            compiled_query.params[key]
            for key in compiled_query.positiontup  # type: ignore
        ]
        return str_query, params

    def _cast_row(
        self, cursor: aiosqlite.Cursor, row: aiosqlite.Row,
    ) -> dict[str, Any]:
        fields = [column[0] for column in cursor.description]
        return {key: value for key, value in zip(fields, row)}

    async def _execute(
        self,
        query: str,
        parameters: Optional[Union[dict[str, Any], list[Any]]] = None,
    ) -> None:
        await self._conn.execute(query, parameters)

    async def _fetch_one(
        self,
        query: str,
        parameters: Optional[Union[dict[str, Any], list[Any]]] = None,
    ) -> Optional[dict[str, Any]]:
        cursor = await self._conn.execute(query, parameters)
        row = await cursor.fetchone()
        if not row:
            return None
        return self._cast_row(cursor, row)

    async def _fetch_all(
        self,
        query: str,
        parameters: Optional[Union[dict[str, Any], list[Any]]] = None,
    ) -> list[dict[str, Any]]:
        cursor = await self._conn.execute(query, parameters)
        rows = await cursor.fetchall()
        return [self._cast_row(cursor, row) for row in rows]

    async def _create_transaction(self, transaction_name: str) -> None:
        query = "SAVEPOINT :transaction_name;"
        parameters = {"transaction_name": transaction_name}
        await self._execute(query, parameters)

    async def _commit_transaction(self, transaction_name: str) -> None:
        query = "RELEASE :transaction_name;"
        parameters = {"transaction_name": transaction_name}
        await self._execute(query, parameters)

    async def _cancel_transaction(self, transaction_name: str) -> None:
        query = "ROLLBACK TO :transaction_name;"
        parameters = {"transaction_name": transaction_name}
        await self._execute(query, parameters)

    async def _open(self) -> None:
        await self._conn

    async def _close(self) -> None:
        return

    def transaction(self) -> "SqliteSessionBackend":
        return SqliteSessionBackend(
            self._conn,
            is_root=self._is_root,
            force_rollback=self._force_rollback,
        )
