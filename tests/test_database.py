import pytest
import sqlalchemy

import based


async def test_database(table: sqlalchemy.Table, database: based.Database):
    async with database.session() as session:
        query = table.select().where(table.c.year > 2000)
        movie = await session.fetch_one(query)
        assert movie is not None
        assert movie["title"] == "Blade Sprinter 2049"


def test_database_invalid_database_url():
    with pytest.raises(ValueError):
        based.Database(":memory:")


async def test_database_connect_already_connected_db(database: based.Database):
    with pytest.raises(based.errors.DatabaseAlreadyConnectedError):
        await database.connect()


async def test_database_force_rollback(table: sqlalchemy.Table, database_url: str):
    title = "Three Display Boards Inside Springfield, Missouri"

    db1 = based.Database(database_url, force_rollback=True)
    await db1.connect()

    async with db1.session() as session:
        query = table.insert().values(title=title, year=2017)
        await session.execute(query)

    await db1.disconnect()
    del db1

    db2 = based.Database(database_url, force_rollback=True)
    await db2.connect()

    async with db2.session() as session:
        query = table.select().where(table.c.title == title)
        movie = await session.fetch_one(query)
        assert movie is None

    await db2.disconnect()


async def test_database_no_force_rollback(table: sqlalchemy.Table, database_url: str):
    title = "Jojo Hare"

    db1 = based.Database(database_url, force_rollback=False)
    await db1.connect()

    async with db1.session() as session:
        query = table.insert().values(title=title, year=2019)
        await session.execute(query)

    await db1.disconnect()
    del db1

    db2 = based.Database(database_url, force_rollback=False)
    await db2.connect()

    async with db2.session() as session:
        query = table.select().where(table.c.title == title)
        movie = await session.fetch_one(query)
        assert movie is not None
        query = table.delete().where(table.c.title == title)
        await session.execute(query)

    await db2.disconnect()


async def test_database_not_connected_get_session(database_url: str):
    database = based.Database(database_url)

    with pytest.raises(based.errors.DatabaseNotConnectedError):
        database.session()


async def test_database_compile_query_without_params(
    session: based.Session,
    table: sqlalchemy.Table,
):
    query = table.select()
    movies = await session.fetch_all(query)
    assert len(movies) == 2
    titles = [movie["title"] for movie in movies]
    assert "Blade Sprinter 2049" in titles
    assert "Farwent" in titles


async def test_database_transaction(
    session: based.Session,
    table: sqlalchemy.Table,
):
    title = "Dull Blinders"

    async with session.transaction() as transaction:
        query = table.insert().values(title=title, year=2013)
        await transaction.execute(query)

    query = table.select().where(table.c.title == title)
    movie = await session.fetch_one(query)
    assert movie is not None


async def test_database_failed_transaction(
    session: based.Session,
    table: sqlalchemy.Table,
):
    title = "North Park"

    with pytest.raises(Exception):
        async with session.transaction() as transaction:
            query = table.insert().values(title=title, year=1997)
            await transaction.execute(query)
            raise Exception

    query = table.select().where(table.c.title == title)
    movie = await session.fetch_one(query)
    assert movie is None


async def test_database_nested_transaction(
    session: based.Session,
    table: sqlalchemy.Table,
):
    title_a = "It's never sunny in Portland"
    title_b = "BoJohn Manhorse"

    async with session.transaction() as tx1:
        query = table.insert().values(title=title_a, year=2005)
        await tx1.execute(query)

        async with tx1.transaction() as tx2:
            query = table.insert().values(title=title_b, year=2014)
            await tx2.execute(query)

    query = table.select().where(table.c.year > 2000)
    movies = await session.fetch_all(query)
    titles = [movie["title"] for movie in movies]
    assert title_a in titles
    assert title_b in titles


async def test_database_failed_nested_transaction(
    session: based.Session,
    table: sqlalchemy.Table,
):
    title_a = "Life Note"
    title_b = "Better Call Police"

    async with session.transaction() as tx1:
        query = table.insert().values(title=title_a, year=2006)
        await tx1.execute(query)

        with pytest.raises(Exception):
            async with tx1.transaction() as tx2:
                query = table.insert().values(title=title_b, year=2015)
                await tx2.execute(query)
                raise Exception

    query = table.select().where(table.c.year > 2000)
    movies = await session.fetch_all(query)
    titles = [movie["title"] for movie in movies]
    assert title_a in titles
    assert title_b not in titles


async def test_database_disconnect_not_connected_database(database_url: str):
    database = based.Database(database_url)

    with pytest.raises(based.errors.DatabaseNotConnectedError):
        await database.disconnect()


async def test_database_open_connected_session(session: based.Session):
    with pytest.raises(based.errors.SessionAlreadyOpenError):
        await session.open()


async def test_database_commit_not_connected_session(database: based.Database):
    session = database.session()

    with pytest.raises(based.errors.SessionNotOpenError):
        await session.commit()


async def test_database_context_manager(database_url: str, table: sqlalchemy.Table):
    async with (
        based.Database(database_url) as database,
        database.session() as session,
    ):
        query = table.select().where(table.c.title == "Blade Sprinter 2049")
        movie = await session.fetch_one(query)
        assert movie is not None


async def test_database_context_manager_exception(database_url: str):
    database = based.Database(database_url)

    with pytest.raises(Exception):
        async with database:
            raise Exception

    with pytest.raises(based.errors.DatabaseNotConnectedError):
        await database.disconnect()
