from __future__ import annotations

import sys
from typing import Callable

from ._dependencies import Connection, Table, func, select


ProgressCallback = Callable[..., None]
# Raised from 2000: with SQLAlchemy 2's insertmanyvalues path over psycopg3,
# larger batches cut round-trips ~5x for the publish phase (124k rows).
BATCH_SIZE = 10000


def root_module():
    return sys.modules[__package__]


def _table_key(table: Table) -> str:
    if table.schema:
        return f"{table.schema}.{table.name}"
    return table.name


def _count_import_rows(connection: Connection, table: Table, import_fingerprint: str) -> int:
    return int(
        connection.execute(
            select(func.count()).select_from(table)
            .where(table.c.import_fingerprint == import_fingerprint)
        ).scalar_one()
    )
