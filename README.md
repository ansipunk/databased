# Based

A based asynchronous database connection manager.

Currently, the only supported backend is `aiosqlite`. Support for `psycopg` is
on the way. It's fairly simple to add a new backend, should you need one. Work
in progress - any contributions - issues or pull requests - are very welcome. It
is designed to be used with SQLAlchemy Core queries.

This library is inspired by [databases](https://github.com/encode/databases).

## Usage

```bash
pip install based[sqlite]
```

```python
import based

database = based.Database("sqlite:///database.sqlite")
await database.connect()

async with database.session() as session:
    query = Movies.select().where(Movies.c.year >= 2010)
    movies = await session.fetch_all(query)

    if movies:
        async with session.transaction() as transaction:
            query = "DELETE FROM movies WHERE year >= :year;"
            params = {"year": 2010}
            await transaction.execute(query, params)

            async with transaction.transaction() as nested:
                for movie in movies:
                    query = "INSERT INTO movies (title, year) VALUES (?, ?);"
                    params = [movie["title"], movie["year"] - 1000]
                    await transaction.execute(query, params)

await database.disconnect()
```

As you can see in the example above, both SQLAlchemy and raw SQL queries are
supported, however, using SQLAlchemy Core is the preferred way to interact with
based. It also supports nested transactions. Instead of keeping track of
transaction stack, based just creates child Session objects that only manage
themselves. Should you need to manually control Session's lifecycle for any
reason, it is also possible, however, you must explicitly open, close, commit
and cancel sessions:

```python
def sqlite(func):
    async def wrapper(*args, **kwargs):
        session = database.session()
        await session.open()

        try:
            await func(session, *args, **kwargs)
        except Exception:
            await session.cancel()
        else:
            await session.commit()
        finally:
            await session.close()

    return wrapper


@sqlite
async def get_movie(
    session: based.Session, title: str,
) -> dict[str, Any] | None:
    query = Movies.select().where(Movies.c.title == title)
    return await session.fetch_one(query)


await get_movie("Jokist")
```

## `force_rollback`

Databases can be initialized in `force_rollback=True` mode. When it's enabled,
everything will work as it usually does, but all the changes to the database
will be discarded upon disconneciton. It can be useful for testing purposes,
where you don't want to manually clean up made changes after each test.

To make it possible, `Database` object will only operate with one single session
and each new requested session will be a nested transaction of it.

```python
async with Database("sqlite:///test.db", force_rollback=True) as database:
	async with database.session() as session:
		query = Movies.insert().values(title="Newboy", year=2004)
		await session.execute(query)

		query = Movies.select().where(Movies.c.title == "Newboy")
		movie = await session.execute(query)
		assert movie is not None

async with Database("sqlite:///test.db", force_rollback=True) as database:
	async with database.session() as session:
		query = Movies.select().where(Movies.c.title == "Newboy")
		movie = await session.execute(query)
		assert movie is None
```

## Design choices

As you can see, database backends are split into two classes - `BasedBackend`
and `Session`. This design choice might be not very clear with SQLite, however,
it is bound to be handy with alternative backends that support connection pools
like psycopg and psycopg_pool.

## Contributing

This library was designed to make adding new backends as simple as possible. You
need to implement `BasedBackend` and `Session` classes and add their
initialization to the `Database` class. Just follow SQLite's example. You only
need to implement methods that raise `NotImplementedError` in base classes,
adding private helpers as needed.

## TODO

- [ ] CI/CD
  - [x] Testing with multiple Python versions
  - [ ] Building and uploading packages to PyPi
- [ ] Psycopg backend with psycopg_pool support
- [ ] Database URL parsing and building
- [ ] Replace nested sessions with transaction stack
