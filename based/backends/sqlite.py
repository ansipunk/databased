from typing import Any, Dict, List, Optional, Tuple, Union

import aiosqlite
from sqlalchemy.dialects import sqlite
from sqlalchemy.sql import ClauseElement

from based.backends import Backend, Session


class SqliteBackend(Backend):
    _url: str
    _force_rollback: bool
    _force_rollback_session: Optional["Session"] = None
    _conn: aiosqlite.Connection
    _connected: bool = False

    def __init__(self, url: str, *, force_rollback: bool = False) -> None:
        self._url = url
        self._force_rollback = force_rollback
        self._conn = aiosqlite.connect(url, isolation_level=None)

    async def _connect(self) -> None:
        self._connected = True

    async def _disconnect(self) -> None:
        self._connected = False

    def _get_session(self) -> "SqliteSession":
        return SqliteSession(
            self._conn,
            is_root=True,
            force_rollback=self._force_rollback,
        )


class SqliteSession(Session):
    _conn: aiosqlite.Connection
    _is_root: bool
    _force_rollback: bool

    def __init__(
        self,
        conn: aiosqlite.Connection,
        *args: List[Any],
        is_root: bool = False,
        force_rollback: bool = False,
        **kwargs: Dict[str, Any],
    ) -> None:
        self._conn = conn
        self._is_root = is_root
        self._force_rollback = force_rollback

    def _compile_query(
        self, query: ClauseElement,
    ) -> Tuple[str, Optional[Union[Dict[str, Any], List[Any]]]]:
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
    ) -> Dict[str, Any]:
        fields = [column[0] for column in cursor.description]
        return {key: value for key, value in zip(fields, row)}

    async def _execute(
        self,
        query: str,
        parameters: Optional[Union[Dict[str, Any], List[Any]]] = None,
    ) -> None:
        await self._conn.execute(query, parameters)

    async def _fetch_one(
        self,
        query: str,
        parameters: Optional[Union[Dict[str, Any], List[Any]]] = None,
    ) -> Optional[Dict[str, Any]]:
        cursor = await self._conn.execute(query, parameters)
        row = await cursor.fetchone()
        if not row:
            return None
        return self._cast_row(cursor, row)

    async def _fetch_all(
        self,
        query: str,
        parameters: Optional[Union[Dict[str, Any], List[Any]]] = None,
    ) -> List[Dict[str, Any]]:
        cursor = await self._conn.execute(query, parameters)
        rows = await cursor.fetchall()
        return [self._cast_row(cursor, row) for row in rows]

    async def _create_transaction(self, transaction_name: str) -> None:
        query = f"SAVEPOINT '{transaction_name}';"
        await self._execute(query)

    async def _commit_transaction(self, transaction_name: str) -> None:
        query = f"RELEASE '{transaction_name}';"
        await self._execute(query)

    async def _cancel_transaction(self, transaction_name: str) -> None:
        query = f"ROLLBACK TO '{transaction_name}'"
        await self._execute(query)

    async def _open(self) -> None:
        await self._conn

    async def _close(self) -> None:
        await self._conn.close()

    def transaction(self) -> "SqliteSession":
        return SqliteSession(
            self._conn,
            is_root=False,
            force_rollback=self._force_rollback,
        )
