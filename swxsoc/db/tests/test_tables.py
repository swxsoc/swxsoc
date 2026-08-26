import pytest

pytest.importorskip("sqlalchemy")

from sqlalchemy import Column, Integer
from sqlalchemy.orm import declarative_base

import swxsoc
from swxsoc.db import create_engine, create_session
from swxsoc.db.tables import (
    base_table,
    create_table,
    create_tables,
    file_level_table,
    file_type_table,
    get_columns,
    get_tables,
    instrument_configuration_table,
    instrument_table,
    remove_tables,
    science_file_table,
    science_product_table,
    status_table,
    table_exists,
)


def test_get_tables() -> None:
    # Create engine and session
    engine = create_engine("sqlite://")

    # Get mission name at runtime
    MISSION_NAME = swxsoc.config["mission"]["mission_name"]
    assert isinstance(MISSION_NAME, str)

    # Create Base
    Base = declarative_base()

    # Create dummy table class
    class TestTable(Base):  # type: ignore[misc, valid-type]
        __tablename__ = "test_table"
        test_id = Column(Integer, primary_key=True)

    # Set up tables
    create_table(engine=engine, table_class=TestTable)

    # Get tables
    tables = get_tables(engine=engine)

    assert "test_table" in tables


def test_get_columns() -> None:
    # Create engine and session
    engine = create_engine("sqlite://")

    # Create Base
    Base = declarative_base()

    # Create dummy table class
    class TestTable(Base):  # type: ignore[misc, valid-type]
        __tablename__ = "test_table"
        test_id = Column(Integer, primary_key=True)

    # Set up tables
    create_table(engine=engine, table_class=TestTable)

    # Get columns
    columns = get_columns(engine=engine, table_name="test_table")

    for column in columns:
        assert column["name"] == "test_id"
        assert column["primary_key"] == 1


def test_create_table() -> None:
    # Create engine and session
    engine = create_engine("sqlite://")

    # Create Base
    Base = declarative_base()

    # Create dummy table class
    class TestTable(Base):  # type: ignore[misc, valid-type]
        __tablename__ = "test_table"
        test_id = Column(Integer, primary_key=True)

    # Set up tables
    create_table(engine=engine, table_class=TestTable)

    # Get tables
    created_tables = get_tables(engine=engine)

    assert "test_table" in created_tables


def test_table_exists() -> None:
    # Create engine and session
    engine = create_engine("sqlite://")

    # Create Base
    Base = declarative_base()

    # Create dummy table class
    class TestTable(Base):  # type: ignore[misc, valid-type]
        __tablename__ = "test_table"
        test_id = Column(Integer, primary_key=True)

    # Set up tables
    create_table(engine=engine, table_class=TestTable)

    # Get tables
    created_tables = get_tables(engine=engine)

    assert "test_table" in created_tables


def test_create_tables() -> None:
    # Get mission name at runtime
    MISSION_NAME = swxsoc.config["mission"]["mission_name"]

    # Create engine and session
    engine = create_engine("sqlite://")
    create_session(engine)

    # Set up tables
    create_tables(engine=engine)

    # Expected tables
    table_names = [
        f"{MISSION_NAME}_file_level",
        f"{MISSION_NAME}_instrument_configuration",
        f"{MISSION_NAME}_instrument",
        f"{MISSION_NAME}_file_type",
        f"{MISSION_NAME}_science_file",
        f"{MISSION_NAME}_science_product",
        f"{MISSION_NAME}_status",
        f"{MISSION_NAME}_status_origin_association",
    ]

    # Get tables
    created_tables = get_tables(engine=engine)

    # Sort the tables
    table_names.sort()
    created_tables.sort()

    # Test expected tables and the returned tables are the same
    assert created_tables == table_names


def test_create_tables_existing() -> None:
    # Get mission name at runtime
    MISSION_NAME = swxsoc.config["mission"]["mission_name"]

    # Create engine and session
    engine = create_engine("sqlite://")
    create_session(engine)

    # Set up tables
    create_tables(engine=engine)

    # Expected tables
    table_names = [
        f"{MISSION_NAME}_file_level",
        f"{MISSION_NAME}_instrument_configuration",
        f"{MISSION_NAME}_instrument",
        f"{MISSION_NAME}_file_type",
        f"{MISSION_NAME}_science_file",
        f"{MISSION_NAME}_science_product",
        f"{MISSION_NAME}_status",
        f"{MISSION_NAME}_status_origin_association",
    ]

    # Get tables
    created_tables = get_tables(engine=engine)

    # Sort the tables
    table_names.sort()
    created_tables.sort()

    # Test expected tables and the returned tables are the same
    assert created_tables == table_names

    # Retry without error
    create_tables(engine=engine)


def test_remove_tables() -> None:
    # Get mission name at runtime
    MISSION_NAME = swxsoc.config["mission"]["mission_name"]

    # Create engine and session
    engine = create_engine("sqlite://")

    # Set up tables
    create_tables(engine=engine)

    # Expected tables
    table_names = [
        f"{MISSION_NAME}_file_level",
        f"{MISSION_NAME}_instrument_configuration",
        f"{MISSION_NAME}_instrument",
        f"{MISSION_NAME}_file_type",
        f"{MISSION_NAME}_science_file",
        f"{MISSION_NAME}_science_product",
        f"{MISSION_NAME}_status",
        f"{MISSION_NAME}_status_origin_association",
    ]

    # Get tables
    created_tables = get_tables(engine=engine)

    # Sort the tables
    table_names.sort()
    created_tables.sort()

    # Test expected tables and the returned tables are the same
    assert created_tables == table_names

    # Remove tables
    remove_tables(engine=engine)

    # Get tables
    assert not table_exists(engine=engine, table_name=f"{MISSION_NAME}_file_level")


def _reset_to_cold_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Simulate a process that has never called ``swxsoc.db.reconfigure()``.

    Every test in this suite runs behind the ``default_test_mission`` autouse
    fixture, which calls ``swxsoc.db.reconfigure()`` and therefore builds
    *every* mission table class together before the test body ever runs.
    Production never does this -- each table class is only built lazily,
    the first time something calls ``.return_class()`` on it -- so tests
    that rely on the autouse fixture's eager rebuild can't observe bugs that
    only occur when classes are built lazily and independently, in
    whatever order application code happens to touch them.

    This resets every table module's cached class to ``None`` and swaps in
    a brand new, empty ``Base`` registry, undoing the autouse fixture's
    eager build so a test can exercise the real, lazy ``return_class()``
    code path from a clean slate. All changes are undone automatically via
    ``monkeypatch`` at the end of the test.
    """
    for module in (
        file_level_table,
        file_type_table,
        instrument_table,
        instrument_configuration_table,
        science_product_table,
        science_file_table,
        status_table,
    ):
        monkeypatch.setattr(module, "_current_class", None)
    monkeypatch.setattr(base_table, "Base", declarative_base())


@pytest.mark.parametrize("query_order", ["product_first", "file_first"])
def test_create_tables_avoids_reentrant_mapper_configuration_bug(
    monkeypatch: pytest.MonkeyPatch, query_order: str
) -> None:
    """
    Regression test for a SQLAlchemy mapper-configuration bug (see #116 and
    its follow-up fix) that only manifests when ORM classes are built
    lazily and independently, as they are in production.

    ``ScienceProductTable.children``/``ScienceFileTable.parent`` (and
    ``StatusTable.origin_files``) declare their relationship targets as
    lambdas (``lambda: <other module>.return_class()``) so the target class
    can be built on demand rather than resolved by name. If
    ``configure_mappers()`` first runs while only one of
    ``ScienceProductTable``/``ScienceFileTable`` has been built, resolving
    that lambda reentrantly builds and registers the *other* class in the
    middle of the same configure pass. That reentrant class gets marked
    "configured" as a side effect, without its own relationship property
    ever having its ``.strategy`` assigned. The next time that class is
    queried directly, SQLAlchemy assumes it's already configured, skips it,
    and accessing the half-initialized property raises
    ``AttributeError: strategy`` -- regardless of which of the two classes
    is queried first.

    ``create_tables()`` avoids this by building every table class up front
    (``get_table_classes(get_table_modules())``) before any query can
    trigger ``configure_mappers()``. This test starts from a cold state
    (nothing built yet, matching a fresh production process) and asserts
    that querying ``ScienceProductTable``/``ScienceFileTable`` afterwards,
    in either order, does not raise.
    """
    _reset_to_cold_state(monkeypatch)

    engine = create_engine("sqlite://")
    create_tables(engine=engine)

    ProductCls = science_product_table.return_class()
    FileCls = science_file_table.return_class()

    if query_order == "product_first":
        query_classes = [ProductCls, FileCls]
    else:
        query_classes = [FileCls, ProductCls]

    session = create_session(engine)
    for table_class in query_classes:
        with session() as sql_session:
            # Before the fix, whichever of these two classes is queried
            # *second* raises AttributeError: strategy.
            sql_session.query(table_class).first()


def test_create_tables_builds_all_classes_before_any_query(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    ``create_tables()`` should build every table class -- including
    ``ScienceProductTable``, ``ScienceFileTable``, and ``StatusTable``,
    which nothing else in ``create_tables()`` otherwise touches -- before
    it runs any query or calls ``Base.metadata.create_all()``. Otherwise
    those classes' tables would never be registered in ``Base.metadata`` in
    time to be created on a genuinely fresh database.
    """
    _reset_to_cold_state(monkeypatch)

    engine = create_engine("sqlite://")
    create_tables(engine=engine)

    for module in (
        science_product_table,
        science_file_table,
        status_table,
    ):
        assert module._current_class is not None

    MISSION_NAME = swxsoc.config["mission"]["mission_name"]
    created_tables = get_tables(engine=engine)
    assert f"{MISSION_NAME}_science_product" in created_tables
    assert f"{MISSION_NAME}_science_file" in created_tables
    assert f"{MISSION_NAME}_status" in created_tables
