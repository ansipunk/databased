import pytest
import sqlalchemy

import databased


async def test_database(table: sqlalchemy.Table, database: databased.Database):
    async with database.session() as session:
        query = table.select().where(table.c.year > 2000)
        movie = await session.fetch_one(query)
        assert movie is not None
        assert movie["title"] == "Blade Runner 2049"


def test_database_invalid_database_url():
    with pytest.raises(ValueError):
        databased.Database(":memory:")


async def test_database_connect_already_connected_db(database: databased.Database):
    with pytest.raises(databased.errors.DatabaseAlreadyConnectedError):
        await database.connect()


async def test_database_force_rollback(table: sqlalchemy.Table, database_url: str):
    db1 = databased.Database(database_url, force_rollback=True)
    await db1.connect()

    async with db1.session() as session:
        query = table.insert().values(title="Joker", year=2019)
        await session.execute(query)

        query = table.select().where(table.c.title == "Joker")
        await session.execute(query)

    await db1.disconnect()
    del db1

    db2 = databased.Database(database_url, force_rollback=True)
    await db2.connect()

    async with db2.session() as session:
        query = table.select().where(table.c.title == "Joker")
        movie = await session.fetch_one(query)
        assert movie is None

    await db2.disconnect()


async def test_database_no_force_rollback(table: sqlalchemy.Table, database_url: str):
    db1 = databased.Database(database_url, force_rollback=False)
    await db1.connect()

    async with db1.session() as session:
        query = table.insert().values(title="Joker", year=2019)
        await session.execute(query)

    await db1.disconnect()
    del db1

    db2 = databased.Database(database_url, force_rollback=True)
    await db2.connect()

    async with db2.session() as session:
        query = table.select().where(table.c.title == "Joker")
        movie = await session.fetch_one(query)
        assert movie is not None
        assert movie["title"] == "Joker"

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
    titles = ["Blade Runner 2049", "Fargo"]
    assert all(movie["title"] in titles for movie in movies)


async def test_database_transaction(
    session: databased.Session,
    table: sqlalchemy.Table,
):
    async with session.transaction() as transaction:
        query = table.insert().values(title="Joker", year="2019")
        await transaction.execute(query)

    query = table.select().where(table.c.title == "Joker")
    movie = await session.fetch_one(query)
    assert movie is not None


async def test_database_failed_transaction(
    session: databased.Session,
    table: sqlalchemy.Table,
):
    with pytest.raises(Exception):
        async with session.transaction() as transaction:
            query = table.insert().values(title="Joker", year="2019")
            await transaction.execute(query)
            raise Exception

    query = table.select().where(table.c.title == "Joker")
    movie = await session.fetch_one(query)
    assert movie is None


async def test_database_nested_transaction(
    session: databased.Session,
    table: sqlalchemy.Table,
):
    async with session.transaction() as tx1:
        query = table.insert().values(title="Bees", year="2030")
        await tx1.execute(query)

        async with tx1.transaction() as tx2:
            query = table.insert().values(title="Flowers", year="2040")
            await tx2.execute(query)

    query = table.select().where(table.c.year > 2025)
    movies = await session.fetch_all(query)
    assert len(movies) == 2


async def test_database_failed_nested_transaction(
    session: databased.Session,
    table: sqlalchemy.Table,
):
    async with session.transaction() as tx1:
        query = table.insert().values(title="Bees", year="2030")
        await tx1.execute(query)

        with pytest.raises(Exception):
            async with tx1.transaction() as tx2:
                query = table.insert().values(title="Flowers", year="2040")
                await tx2.execute(query)
                raise Exception

    query = table.select().where(table.c.year > 2025)
    movies = await session.fetch_all(query)
    assert len(movies) == 1
