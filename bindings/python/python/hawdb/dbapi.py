# Copyright 2026 Nowledge
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""PEP 249 (DB-API 2.0) connections over HawDB's SQL surface.

``hawdb.connect(path=None, read_only=False, *, autocommit=False, ...)`` opens
a database like ``hawdb.open``, with the same options, and returns a
``Connection`` that owns it. ``connect`` is another name for ``Connection``.
Cursor statements run through ``execute_sql``; Cypher, named parameters and
graph values stay on ``Database.execute``. Like any handle, the connection
keeps other handles off its path until it closes, so close it before opening
the same path with ``hawdb.open``.

Parameters are a sequence bound to ``$1``, ``$2``, ... in order
(``paramstyle = "numeric_dollar"``). Statements are sent unchanged: ``?``,
``%s`` and ``:name`` are not rewritten and fail like any other invalid SQL.
Parameter values follow the binding's rules and keep its ``TypeError``,
``OverflowError`` and ``ValueError``.

Autocommit is off, as PEP 249 requires. The first ``execute`` or
``executemany`` after ``connect``, ``commit()`` or ``rollback()`` begins a
transaction, even for a query; ``commit()`` publishes it, ``rollback()``
discards it, and ``close()`` rolls back whatever was not committed.
``connect(..., autocommit=True)`` instead runs each ``execute`` as its own
transaction and each ``executemany`` as one atomic batch, and ``commit()``
and ``rollback()`` do nothing. Statements the engine refuses inside a
transaction, such as ``CREATE BRANCH`` and ``USE BRANCH``, need such a
connection. A ``read_only=True`` connection always works this way, because a
read-only database cannot commit even an empty transaction.

A failed ``execute`` leaves the transaction open with the earlier statements
still staged. A failed ``executemany`` does not: there are no savepoints, so
the batch takes the whole transaction with it, including the statements
before it. The connection is then aborted: ``execute``, ``executemany`` and
``commit()`` raise ``OperationalError`` until ``rollback()`` acknowledges the
lost work. ``close()`` still succeeds.

Rows are tuples in ``description`` order. ``description`` holds one
seven-item tuple per column: the name, a type code and five ``None``. The
type code is the Python type of the column's first non-null value, or
``None`` when every value is null. ``STRING`` equals ``str``, ``BINARY``
equals ``bytes`` and ``NUMBER`` equals ``int`` and ``float``; booleans and
vectors match none of them. DDL and every INSERT, UPDATE and DELETE leave
``description`` as ``None`` and ``rowcount`` as -1, since the binding does
not report affected rows, and fetching from them returns no rows.

Two engine gaps show through. Key lookups, joins, ORDER BY, DISTINCT,
aggregates and GROUP BY return their columns sorted by name rather than in
SELECT order, so read a row by the names in ``description``, not by the
positions of the SELECT list. When nothing matches, the same shapes report
no columns at all: ``description`` is ``None``, and ``pandas.read_sql``
cannot build a frame from it.

Engine errors keep their ``hawdb.exceptions`` types. ``Error`` and
``DatabaseError`` are both ``hawdb.exceptions.Error``, so ``DatabaseError``
also catches ``InterfaceError``. A constraint violation such as a duplicate
primary key is an ``ExecutionError``: the engine does not single those out,
so this module's ``IntegrityError`` is declared but not raised. It is not
``hawdb.exceptions.IntegrityError``, which reports damaged storage rather
than a violated constraint. Using a closed connection or cursor raises
``InterfaceError``; fetching before a successful ``execute`` raises
``ProgrammingError``.

``threadsafety`` is 1: threads may share the module but not a connection. A
connection must stay on the thread that opened it, and other threads get
``ProgrammingError``. Close it before that thread ends, because no other
thread can release the database, which then keeps its path busy until the
process exits. There is no pool.

Deliberate departures from PEP 249: ``paramstyle`` is ``"numeric_dollar"``,
SQLAlchemy's name for ``$1`` placeholders, which the PEP does not list; and
fetching after a statement without a result set returns no rows instead of
raising, because a query that matches nothing can look the same. Not
provided: ``callproc``, ``nextset``, scrollable cursors, and the ``Date``,
``Time``, ``Timestamp`` and ``*FromTicks`` constructors, because the binding
takes no temporal parameter values.
"""

from __future__ import annotations

import datetime
import operator
import os
import sys
import threading
from collections.abc import Iterator, Sequence
from itertools import islice
from types import TracebackType
from typing import Any

from hawdb._hawdb import Database, QueryResult, Transaction, exceptions
from hawdb._hawdb import open as _open

__all__ = [
    "BINARY",
    "Binary",
    "Connection",
    "Cursor",
    "DATETIME",
    "DataError",
    "DatabaseError",
    "Error",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "NUMBER",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
    "ROWID",
    "STRING",
    "Warning",
    "apilevel",
    "connect",
    "paramstyle",
    "threadsafety",
]

apilevel = "2.0"
threadsafety = 1
paramstyle = "numeric_dollar"

_ABORTED = (
    "a failed executemany rolled back the whole transaction; "
    "call rollback() before using the connection again"
)


class Warning(Exception):
    """Declared by PEP 249; HawDB does not raise it."""


Error = exceptions.Error
DatabaseError = Error


class InterfaceError(Error):
    """A closed connection or cursor was used."""


class ProgrammingError(Error):
    """A cursor was fetched before a successful execute, or used from another thread."""


class OperationalError(Error):
    """A failed ``executemany`` aborted the transaction; call ``rollback()``."""


class IntegrityError(Error):
    """Declared by PEP 249; constraint violations raise ``ExecutionError``."""


class DataError(Error):
    """Declared by PEP 249; invalid parameter values keep the binding's types."""


class InternalError(Error):
    """Declared by PEP 249; HawDB does not raise it."""


class NotSupportedError(Error):
    """Declared by PEP 249; HawDB does not raise it."""


class _TypeObject:
    """A PEP 249 type object, equal to each Python type it groups."""

    def __init__(self, name: str, *types: type) -> None:
        self._name = name
        self._types = frozenset(types)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, type):
            return NotImplemented
        return other in self._types

    def __repr__(self) -> str:
        return f"hawdb.dbapi.{self._name}"


STRING = _TypeObject("STRING", str)
BINARY = _TypeObject("BINARY", bytes)
NUMBER = _TypeObject("NUMBER", int, float)
# The binding returns no temporal values and HawDB exposes no row ids, so
# these two never match; they exist for code that maps description types.
DATETIME = _TypeObject("DATETIME", datetime.date, datetime.datetime, datetime.time)
ROWID = _TypeObject("ROWID")

Binary = bytes

_Column = tuple[str, "type | None", None, None, None, None, None]


class Connection:
    """A DB-API connection that owns one ``hawdb.Database``.

    ``Connection(path=None, read_only=False, *, autocommit=False, ...)``, also
    spelled ``connect``, opens the database like ``hawdb.open``, with the same
    options. Closing the connection closes the database. As a context manager
    it commits on a clean exit, rolls back on an exception, and closes either
    way.
    """

    Warning = Warning
    Error = Error
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    DataError = DataError
    OperationalError = OperationalError
    IntegrityError = IntegrityError
    InternalError = InternalError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError

    def __init__(
        self,
        path: str | os.PathLike[str] | None = None,
        read_only: bool = False,
        *,
        autocommit: bool = False,
        max_read_result_rows: int | None = None,
        max_read_result_payload_bytes: int | None = None,
    ) -> None:
        self._database: Database | None = _open(
            path,
            read_only,
            max_read_result_rows=max_read_result_rows,
            max_read_result_payload_bytes=max_read_result_payload_bytes,
        )
        # A read-only database cannot commit even an empty transaction.
        self._autocommit = bool(autocommit or read_only)
        # Thread-local, so a later thread that reuses this thread's ident
        # does not pass for the owner.
        self._owner = threading.local()
        self._owner.opened_here = True
        self._transaction: Transaction | None = None
        self._aborted = False  # a failed executemany ended the transaction

    @property
    def autocommit(self) -> bool:
        """Whether each statement commits on its own; fixed when connecting."""
        return self._autocommit

    def cursor(self) -> Cursor:
        self._open()
        return Cursor(self)

    def commit(self) -> None:
        """Publish the open transaction; without one, do nothing."""
        self._usable()
        transaction, self._transaction = self._transaction, None
        if transaction is not None:
            transaction.commit()

    def rollback(self) -> None:
        """Discard the open transaction and clear an aborted state."""
        self._open()
        self._aborted = False
        transaction, self._transaction = self._transaction, None
        if transaction is not None:
            transaction.rollback()

    def close(self) -> None:
        """Roll back uncommitted work and close the database; repeats do nothing."""
        if self._database is None:
            return
        database, self._database = self._open(), None
        transaction, self._transaction = self._transaction, None
        try:
            if transaction is not None:
                transaction.rollback()
        finally:
            database.close()

    def __enter__(self) -> Connection:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        try:
            if exc_type is None and self._database is not None:
                self.commit()
        finally:
            self.close()

    def _open(self) -> Database:
        # The database is unsendable: another thread would hit a Rust panic.
        if not getattr(self._owner, "opened_here", False):
            raise ProgrammingError("a connection must stay on the thread that opened it")
        if self._database is None:
            raise InterfaceError("connection is closed")
        return self._database

    def _usable(self) -> Database:
        database = self._open()
        if self._aborted:
            raise OperationalError(_ABORTED)
        return database

    def _begin(self) -> Transaction:
        database = self._usable()
        if self._transaction is None:
            # Holding a transaction for queries blocks no other reader today:
            # the first handle to run a statement keeps others off its path
            # until it closes. Revisit this if handles ever share a path.
            self._transaction = database.transaction()
        return self._transaction

    def _execute(self, operation: str, parameters: list[Any] | None) -> QueryResult:
        if self._autocommit:
            return self._open().execute_sql(operation, parameters)
        return self._begin().execute_sql(operation, parameters)

    def _executemany(
        self, operation: str, seq_of_parameters: Sequence[Sequence[Any]]
    ) -> None:
        if self._autocommit:
            self._open().executemany_sql(operation, seq_of_parameters)
            return
        transaction = self._begin()
        try:
            transaction.executemany_sql(operation, seq_of_parameters)
        except BaseException:
            # A failed batch has already rolled back the whole transaction.
            # Roll back here too, for a failure such as an interrupt that
            # left it open.
            self._transaction, self._aborted = None, True
            transaction.rollback()
            raise


connect = Connection


class Cursor:
    """A DB-API cursor; rows are tuples in ``description`` order.

    ``execute`` materializes the whole result, as ``Database.execute_sql``
    does, and the fetch methods consume it.
    """

    _rows: Iterator[tuple[Any, ...]] | None  # None: nothing to fetch
    _description: tuple[_Column, ...] | None
    _rowcount: int

    def __init__(self, connection: Connection) -> None:
        self.connection = connection
        self.arraysize = 1
        self._closed = False
        self._reset()

    @property
    def description(self) -> tuple[_Column, ...] | None:
        return self._description

    @property
    def rowcount(self) -> int:
        return self._rowcount

    def execute(self, operation: str, parameters: Sequence[Any] | None = None) -> Cursor:
        """Run one SQL statement with ``parameters`` bound to ``$1``, ``$2``, ..."""
        self._check_open()
        self._reset()
        bound = None if parameters is None else list(_sequence(parameters, "parameters"))
        result = self.connection._execute(operation, bound)
        columns = result.columns
        # The binding fills each row dict in column order.
        rows = [tuple(row.values()) for row in result.fetchall()]
        if columns:
            self._description = tuple(
                (name, _type_code(rows, position), None, None, None, None, None)
                for position, name in enumerate(columns)
            )
            self._rowcount = len(rows)
        self._rows = iter(rows)
        return self

    def executemany(
        self, operation: str, seq_of_parameters: Sequence[Sequence[Any]]
    ) -> Cursor:
        """Run one SQL statement for each parameter sequence as one batch.

        The batch delegates to ``executemany_sql`` and produces no result set.
        In autocommit it is its own transaction; otherwise it joins the
        connection's transaction, which a failed batch aborts.
        """
        self._check_open()
        self._reset()
        # Wrong arguments must fail here: a failed batch aborts the transaction.
        if not isinstance(operation, str):
            raise TypeError(f"operation must be a str, not {type(operation).__name__}")
        _sequence(seq_of_parameters, "seq_of_parameters")
        self.connection._executemany(operation, seq_of_parameters)
        self._rows = iter(())
        return self

    def fetchone(self) -> tuple[Any, ...] | None:
        return next(self._result(), None)

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Return up to ``size`` rows; ``size`` defaults to ``arraysize``."""
        rows = self._result()
        size = operator.index(self.arraysize if size is None else size)
        if size < 0:
            raise ValueError("fetchmany size must not be negative")
        return list(islice(rows, min(size, sys.maxsize)))

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._result())

    def close(self) -> None:
        """Release the result; repeats do nothing."""
        self._closed = True
        self._reset()

    def setinputsizes(self, sizes: object) -> None:
        """Accepted and ignored, as PEP 249 allows."""

    def setoutputsize(self, size: object, column: object = None) -> None:
        """Accepted and ignored, as PEP 249 allows."""

    def __iter__(self) -> Cursor:
        return self

    def __next__(self) -> tuple[Any, ...]:
        return next(self._result())

    def __enter__(self) -> Cursor:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def _check_open(self) -> None:
        if self._closed:
            raise InterfaceError("cursor is closed")
        self.connection._open()

    def _reset(self) -> None:
        self._rows = None
        self._description = None
        self._rowcount = -1

    def _result(self) -> Iterator[tuple[Any, ...]]:
        self._check_open()
        if self._rows is None:
            raise ProgrammingError("no result to fetch; execute a statement first")
        return self._rows


def _sequence(value: Any, name: str) -> Sequence[Any]:
    # The binding's rule for parameter containers, checked here so that a
    # wrong container fails before it reaches the connection's transaction.
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Sequence):
        raise TypeError(
            f"{name} must be a sequence such as a list or tuple, not "
            f"{type(value).__name__}; statements bind $1, $2, ... by position"
        )
    return value


def _type_code(rows: list[tuple[Any, ...]], position: int) -> type | None:
    for row in rows:
        if row[position] is not None:
            return type(row[position])
    return None
