import os
import tempfile

import pytest
import sqlalchemy

import based

RAW_DATABASE_URLS = os.environ.get("BASED_TEST_DB_URLS", "")
DATABASE_URLS = RAW_DATABASE_URLS.split(",") if RAW_DATABASE_URLS else []
DATABASE_URLS.append("sqlite")


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
        table.insert().values(title="Blade Sprinter 2049", year=2017),
        table.insert().values(title="Farwent", year=1996),
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


@pytest.fixture
async def database(database_url: str):
    database = based.Database(database_url, force_rollback=True)
    await database.connect()
    yield database
    await database.disconnect()


@pytest.fixture
async def session(database: based.Database):
    async with database.session() as session:
        yield session


@pytest.fixture(scope="session")
def database_url(raw_database_url: str, worker_id: str) -> str:
    if raw_database_url != "sqlite":
        yield raw_database_url
    else:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = f"{tmpdir}/{worker_id}.sqlite"
            yield f"sqlite:///{db_path!s}"


def pytest_generate_tests(metafunc):
    if "raw_database_url" in metafunc.fixturenames:
        metafunc.parametrize("raw_database_url", DATABASE_URLS, scope="session")
