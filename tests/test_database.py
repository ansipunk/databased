import sqlalchemy

import databased


async def test_database(table: sqlalchemy.Table, database_url: str):
    database = databased.Database(database_url, force_rollback=True)
    await database.connect()

    async with database.session() as session:
        query = table.select().where(table.c.year > 2000)
        movie = await session.fetch_one(query)
        assert movie is not None
        assert movie["title"] == "Blade Runner 2049"

    await database.disconnect()
