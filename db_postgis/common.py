from __future__ import annotations

import sys
from typing import Callable

from ._dependencies import Connection, Table, func, select


ProgressCallback = Callable[..., None]
BATCH_SIZE = 2000


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
