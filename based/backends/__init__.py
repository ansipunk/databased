from secrets import token_urlsafe
from types import TracebackType
from typing import Any, Dict, List, Optional, Tuple, Type, Union

from sqlalchemy.sql import ClauseElement

from based import errors


class Backend:
    _url: str
    _force_rollback: bool
    _force_rollback_session: Optional["Session"] = None
    _connected: bool = False

    def __init__(self, url: str, *, force_rollback: bool = False) -> None:
        self._url = url
        self._force_rollback = force_rollback

    async def _connect(self) -> None:
        raise NotImplementedError

    async def _disconnect(self) -> None:
        raise NotImplementedError

    def _get_session(self) -> "Session":
        """Return a new Session object with connection from this pool.

        It is crucial that returned Session objects have their
        `_is_root` field set to `True`.

        If there's asynchronous initialization to be done, it should be performed
        in session's `_open` method.
        """
        raise NotImplementedError

    def session(self) -> "Session":
        if not self._connected:
            raise errors.DatabaseNotConnectedError

        if self._force_rollback:
            if self._force_rollback_session is None:
                self._force_rollback_session = self._get_session()

            return self._force_rollback_session

        return self._get_session()

    async def connect(self) -> None:
        if self._connected:
            raise errors.DatabaseAlreadyConnectedError

        await self._connect()

    async def disconnect(self) -> None:
        if not self._connected:
            raise errors.DatabaseNotConnectedError

        if self._force_rollback and self._force_rollback_session:
            await self._force_rollback_session.cancel()
            await self._force_rollback_session.close(force=True)

        await self._disconnect()


class Session:
    _is_root: bool
    _force_rollback: bool
    _transaction: Optional[str] = None

    def __init__(
        self,
        *args: List[Any],
        is_root: bool = False,
        force_rollback: bool = False,
        **kwargs: Dict[str, Any],
    ) -> None:
        self._is_root = is_root
        self._force_rollback = force_rollback

    async def _execute(
        self,
        query: str,
        parameters: Optional[Union[Dict[str, Any], List[Any]]] = None,
    ) -> None:
        raise NotImplementedError

    async def _fetch_one(
        self,
        query: str,
        parameters: Optional[Union[Dict[str, Any], List[Any]]] = None,
    ) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    async def _fetch_all(
        self,
        query: str,
        parameters: Optional[Union[Dict[str, Any], List[Any]]] = None,
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError

    async def _create_transaction(self, transaction_name: str) -> None:
        raise NotImplementedError

    async def _commit_transaction(self, transaction_name: str) -> None:
        raise NotImplementedError

    async def _cancel_transaction(self, transaction_name: str) -> None:
        raise NotImplementedError

    async def _open(self) -> None:
        raise NotImplementedError

    async def _close(self) -> None:
        raise NotImplementedError

    def transaction(self) -> "Session":
        """Return a new Session object with the same connection.

        It is crucial that returned Session objects have their
        `_is_root` field set to `False`.

        This method is named `transaction` so it can be used as following:

            async with session.transaction() as transaction:
                await transaction.execute(query, parameters)
        """
        raise NotImplementedError

    def _compile_query(
        self, query: ClauseElement,
    ) -> Tuple[str, Optional[Union[Dict[str, Any], List[Any]]]]:
        raise NotImplementedError

    async def open(self) -> None:
        if self._transaction:
            raise errors.SessionAlreadyOpenError

        if self._is_root:
            await self._open()

        self._transaction = token_urlsafe(16)
        await self._create_transaction(self._transaction)

    async def commit(self) -> None:
        if not self._transaction:
            raise errors.SessionNotOpenError

        if self._force_rollback and self._is_root:
            await self._cancel_transaction(self._transaction)
        else:
            await self._commit_transaction(self._transaction)

    async def cancel(self) -> None:
        if self._transaction:
            await self._cancel_transaction(self._transaction)

    async def close(self, *, force: bool = False) -> None:
        if not self._transaction:
            return

        if force or (self._is_root and not self._force_rollback):
            # Only close connections if it's a root session.
            # Sessions with enabled `force_rollback` mode must be
            # closed from `DatabaseBackend` object.
            await self._close()
            self._transaction = None

    async def execute(
        self,
        query: Union[ClauseElement, str],
        parameters: Optional[Union[Dict[str, Any], List[Any]]] = None,
    ) -> None:
        if isinstance(query, ClauseElement):
            query, parameters = self._compile_query(query)
        await self._execute(query, parameters)

    async def fetch_one(
        self,
        query: Union[ClauseElement, str],
        parameters: Optional[Union[Dict[str, Any], List[Any]]] = None,
    ) -> Optional[Dict[str, Any]]:
        if isinstance(query, ClauseElement):
            query, parameters = self._compile_query(query)
        return await self._fetch_one(query, parameters)

    async def fetch_all(
        self,
        query: Union[ClauseElement, str],
        parameters: Optional[Union[Dict[str, Any], List[Any]]] = None,
    ) -> List[Dict[str, Any]]:
        if isinstance(query, ClauseElement):
            query, parameters = self._compile_query(query)
        return await self._fetch_all(query, parameters)

    async def __aenter__(self) -> "Session":
        await self.open()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        if exc_val is not None:
            await self.cancel()
            await self.close()
            raise exc_val

        await self.commit()
        await self.close()
