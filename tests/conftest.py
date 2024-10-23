import sqlalchemy
import pytest

DATABASE_URLS = (
    "sqlite:///test.db",
)


@pytest.fixture()
def metadata() -> sqlalchemy.MetaData:
    return sqlalchemy.MetaData()


@pytest.fixture()
def table(metadata: sqlalchemy.MetaData) -> sqlalchemy.Table:
    return sqlalchemy.Table(
        "movies",
        metadata,
        sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column("title", sqlalchemy.Text, nullable=False),
        sqlalchemy.Column("year", sqlalchemy.Integer, nullable=False),
    )


@pytest.fixture(autouse=True, scope="function")
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


def pytest_generate_tests(metafunc):
    if "database_url" in metafunc.fixturenames:
        metafunc.parametrize("database_url", DATABASE_URLS)
