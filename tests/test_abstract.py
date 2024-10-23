import pytest
import sqlalchemy

import databased.backends


async def test_abstract_backend(database_url: str):
    backend = databased.backends.DatabaseBackend(database_url)

    with pytest.raises(NotImplementedError):
        await backend.connect()

    backend._connected = True

    with pytest.raises(NotImplementedError):
        await backend.disconnect()

    with pytest.raises(NotImplementedError):
        backend.session()


async def test_abstract_session(table: sqlalchemy.Table):
    session = databased.backends.SessionBackend(is_root = True)
    raw_query = "SELECT 1;"
    transaction = "transaction"

    with pytest.raises(NotImplementedError):
        await session._execute(raw_query)

    with pytest.raises(NotImplementedError):
        await session._fetch_one(raw_query)

    with pytest.raises(NotImplementedError):
        await session._fetch_all(raw_query)

    with pytest.raises(NotImplementedError):
        await session._create_transaction(transaction)

    with pytest.raises(NotImplementedError):
        await session._commit_transaction(transaction)

    with pytest.raises(NotImplementedError):
        await session._cancel_transaction(transaction)

    with pytest.raises(NotImplementedError):
        await session._open()

    with pytest.raises(NotImplementedError):
        await session._close()

    with pytest.raises(NotImplementedError):
        session.transaction()

    with pytest.raises(NotImplementedError):
        session._compile_query(table.select())
