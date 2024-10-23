import sqlalchemy
import pytest

import databased

DATABASE_URLS = (
    "sqlite:///test.sqlite",
)


@pytest.fixture(scope="session")
def metadata() -> sqlalchemy.MetaData:
    return sqlalchemy.MetaData()


@pytest.fixture(scope="session")
def table(metadata: sqlalchemy.MetaData) -> sqlalchemy.Table:
    return sqlalchemy.Table(
        "movies",
        metadata,
        sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column("title", sqlalchemy.Text, nullable=False),
        sqlalchemy.Column("year", sqlalchemy.Integer, nullable=False),
    )


@pytest.fixture(autouse=True, scope="session")
def _context(
    metadata: sqlalchemy.MetaData,
    table: sqlalchemy.Table,
    database_url: str,
):
    engine = sqlalchemy.create_engine(database_url)
    metadata.create_all(engine)

    conn = engine.connect()
    queries = [
        table.insert().values(title="Blade Runner 2049", year=2017),
        table.insert().values(title="Fargo", year=1996),
    ]
    for query in queries:
        conn.execute(query)
    conn.commit()
    conn.close()

    engine.dispose()

    yield

    engine = sqlalchemy.create_engine(database_url)
    metadata.drop_all(engine)
    engine.dispose()


@pytest.fixture(scope="function")
async def database(database_url: str):
    database = databased.Database(database_url, force_rollback=True)
    await database.connect()
    yield database
    await database.disconnect()


@pytest.fixture()
async def session(database: databased.Database):
    async with database.session() as session:
        yield session


def pytest_generate_tests(metafunc):
    if "database_url" in metafunc.fixturenames:
        metafunc.parametrize("database_url", DATABASE_URLS, scope="session")
