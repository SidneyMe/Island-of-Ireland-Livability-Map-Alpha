from __future__ import annotations

import sys


def _install_windows_platform_shim() -> None:
    if sys.platform != "win32":
        return
    # On some Windows setups, platform.system()/uname() can block inside WMI.
    # SQLAlchemy imports platform during startup, so avoid hanging the CLI before
    # it can print progress.
    import platform
    from collections import namedtuple

    uname_result = namedtuple(
        "uname_result",
        "system node release version machine processor",
    )
    cached = uname_result("Windows", "", "", "", "AMD64", "")
    platform.uname = lambda: cached
    platform.system = lambda: "Windows"
    platform.machine = lambda: "AMD64"
    platform.processor = lambda: ""


_install_windows_platform_shim()

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
