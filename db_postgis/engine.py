from __future__ import annotations

from config import database_url

from ._dependencies import Engine, create_engine


def build_engine() -> Engine:
    # insertmanyvalues_page_size raised from default (1000) so SQLAlchemy's
    # multi-row INSERT path matches our 10k BATCH_SIZE and keeps round-trips low.
    # pool_size + max_overflow widened so background phases (e.g. Tier-4 noise
    # dispatch) don't starve the main publish connection.
    return create_engine(
        database_url(),
        future=True,
        pool_pre_ping=True,
        pool_size=8,
        max_overflow=8,
        insertmanyvalues_page_size=10000,
    )
