import pytest
import sqlalchemy

import databased


async def test_database(table: sqlalchemy.Table, database: databased.Database):
    async with database.session() as session:
        query = table.select().where(table.c.year > 2000)
        movie = await session.fetch_one(query)
        assert movie is not None
        assert movie["title"] == "Blade Sprinter 2049"


def test_database_invalid_database_url():
    with pytest.raises(ValueError):
        databased.Database(":memory:")


async def test_database_connect_already_connected_db(database: databased.Database):
    with pytest.raises(databased.errors.DatabaseAlreadyConnectedError):
        await database.connect()


async def test_database_force_rollback(table: sqlalchemy.Table, database_url: str):
    title = "Three Display Boards Inside Springfield, Missouri"

    db1 = databased.Database(database_url, force_rollback=True)
    await db1.connect()

    async with db1.session() as session:
        query = table.insert().values(title=title, year=2017)
        await session.execute(query)

    await db1.disconnect()
    del db1

    db2 = databased.Database(database_url, force_rollback=True)
    await db2.connect()

    async with db2.session() as session:
        query = table.select().where(table.c.title == title)
        movie = await session.fetch_one(query)
        assert movie is None

    await db2.disconnect()


async def test_database_no_force_rollback(table: sqlalchemy.Table, database_url: str):
    title = "Jojo Hare"

    db1 = databased.Database(database_url, force_rollback=False)
    await db1.connect()

    async with db1.session() as session:
        query = table.insert().values(title=title, year=2019)
        await session.execute(query)

    await db1.disconnect()
    del db1

    db2 = databased.Database(database_url, force_rollback=False)
    await db2.connect()

    async with db2.session() as session:
        query = table.select().where(table.c.title == title)
        movie = await session.fetch_one(query)
        assert movie is not None
        query = table.delete().where(table.c.title == title)
        await session.execute(query)

    await db2.disconnect()


async def test_database_not_connected_get_session(database_url: str):
    database = databased.Database(database_url)

    with pytest.raises(databased.errors.DatabaseNotConnectedError):
        database.session()


async def test_database_compile_query_without_params(
    session: databased.Session,
    table: sqlalchemy.Table,
):
    query = table.select()
    movies = await session.fetch_all(query)
    assert len(movies) == 2
    titles = [movie["title"] for movie in movies]
    assert "Blade Sprinter 2049" in titles
    assert "Farwent" in titles


async def test_database_transaction(
    session: databased.Session,
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
    session: databased.Session,
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
    session: databased.Session,
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
    session: databased.Session,
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
    database = databased.Database(database_url)
    
    with pytest.raises(databased.errors.DatabaseNotConnectedError):
        await database.disconnect()


async def test_database_open_connected_session(session: databased.Session):
    with pytest.raises(databased.errors.SessionAlreadyOpenError):
        await session.open()


async def test_database_commit_not_connected_session(database: databased.Database):
    session = database.session()
    
    with pytest.raises(databased.errors.SessionNotOpenError):
        await session.commit()
