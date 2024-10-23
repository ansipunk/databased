from databased.backends import DatabaseBackend, SessionBackend


class Database:
    _backend: DatabaseBackend
    _force_rollback: bool

    def __init__(self, url: str, force_rollback: bool = False) -> None:
        url_parts = url.split("://")
        if len(url_parts) != 2:
            raise ValueError("Invalid database URL")
        schema = url_parts[0]

        if schema == "sqlite":
            from databased.backends.sqlite import SqliteDatabaseBackend
            sqlite_url = url_parts[1][1:]
            self._backend = SqliteDatabaseBackend(
                sqlite_url, force_rollback=force_rollback,
            )

    async def connect(self) -> None:
        await self._backend.connect()

    async def disconnect(self) -> None:
        await self._backend.disconnect()

    def session(self) -> SessionBackend:
        return self._backend.session()
