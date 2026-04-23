from __future__ import annotations

try:
    from geoalchemy2 import Geometry
    from geoalchemy2.shape import from_shape, to_shape
    from sqlalchemy import (
        BigInteger,
        Boolean,
        Date,
        Column,
        DateTime,
        Float,
        Integer,
        MetaData,
        Table,
        Text,
        case,
        create_engine,
        delete,
        func,
        inspect,
        insert,
        select,
        text,
        update,
    )
    from sqlalchemy.dialects.postgresql import JSONB, insert as pg_insert
    from sqlalchemy.engine import Connection, Engine
except ImportError as exc:  # pragma: no cover - depends on installed dependencies
    raise RuntimeError(
        "Missing PostgreSQL/PostGIS dependencies. Install requirements.txt "
        "before running the DB-backed pipeline."
    ) from exc
