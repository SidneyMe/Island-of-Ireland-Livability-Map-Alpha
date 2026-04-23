from __future__ import annotations

import os
import queue
import subprocess
import threading
import time
from collections import deque
from datetime import datetime, timezone

from sqlalchemy.engine.url import make_url


def detect_importer_version_impl(importer_bin: str, *, subprocess_module=subprocess) -> str:
    try:
        completed = subprocess_module.run(
            [importer_bin, "--version"],
            check=True,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(
            "osm2pgsql is required for local OSM import, but it was not found on PATH. "
            "Install osm2pgsql or set OSM2PGSQL_BIN before running --precompute."
        ) from exc
    except subprocess_module.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()
        raise RuntimeError(
            "Unable to determine osm2pgsql version before import. "
            f"Original error: {stderr or exc}"
        ) from exc

    version_line = (completed.stdout or completed.stderr or "").strip().splitlines()
    if not version_line:
        return "osm2pgsql-unknown"
    return version_line[0].strip()


def resolve_source_state_impl(
    *,
    build_source_state_fn,
    detect_importer_version_fn,
    progress_cb=None,
    emit_detail_fn=None,
):
    if emit_detail_fn is not None:
        emit_detail_fn(progress_cb, "probing osm2pgsql --version")
    importer_version = detect_importer_version_fn()
    if emit_detail_fn is not None:
        emit_detail_fn(progress_cb, "resolving OSM source state")
    return build_source_state_fn(importer_version, progress_cb=progress_cb)


def query_value_impl(url, key: str) -> str | None:
    value = url.query.get(key)
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        if not value:
            return None
        value = value[0]
    text = str(value).strip()
    return text or None


def connection_arguments_impl(*, database_url_fn, make_url_fn=make_url) -> tuple[list[str], dict[str, str]]:
    url = make_url_fn(database_url_fn())
    if not url.host or not url.username or not url.database:
        raise RuntimeError(
            "DATABASE_URL or POSTGRES_* must resolve to host, database, and user for osm2pgsql import."
        )
    if not url.password and str(url.host).lower() not in {"localhost", "127.0.0.1"}:
        raise RuntimeError(
            "osm2pgsql import requires a database password for non-local PostgreSQL hosts. "
            "Set DATABASE_URL with a password or provide POSTGRES_PASSWORD."
        )

    command = [
        "-H", url.host,
        "-P", str(url.port or 5432),
        "-d", url.database,
        "-U", url.username,
    ]
    env: dict[str, str] = {"PGCONNECT_TIMEOUT": "15"}
    if url.password:
        env["PGPASSWORD"] = url.password
    query_env = {
        "sslmode": "PGSSLMODE",
        "sslrootcert": "PGSSLROOTCERT",
        "sslcert": "PGSSLCERT",
        "sslkey": "PGSSLKEY",
    }
    for query_key, env_key in query_env.items():
        value = query_value_impl(url, query_key)
        if value is not None:
            env[env_key] = value
    return command, env


def stream_subprocess_lines_impl(
    process: subprocess.Popen[str],
    *,
    emit_detail_fn,
    progress_cb=None,
    queue_module=queue,
    threading_module=threading,
    time_module=time,
) -> list[str]:
    if process.stdout is None:
        return []

    line_queue: queue.Queue[str | object] = queue_module.Queue()
    sentinel = object()
    recent_lines: deque[str] = deque(maxlen=30)
    reader_errors: list[BaseException] = []

    def _reader() -> None:
        try:
            for raw_line in process.stdout:
                line_queue.put(raw_line)
        except BaseException as exc:  # pragma: no cover - defensive subprocess plumbing
            reader_errors.append(exc)
        finally:
            line_queue.put(sentinel)

    reader_thread = threading_module.Thread(target=_reader, daemon=True)
    reader_thread.start()

    last_output_at = time_module.monotonic()
    last_heartbeat_at: float | None = None
    reader_finished = False

    while True:
        try:
            item = line_queue.get(timeout=0.25)
        except queue_module.Empty:
            now = time_module.monotonic()
            if (
                (now - last_output_at) >= 15.0
                and (last_heartbeat_at is None or (now - last_heartbeat_at) >= 15.0)
            ):
                emit_detail_fn(progress_cb, "osm2pgsql still running; waiting for new output...")
                last_heartbeat_at = now
            if reader_finished and process.poll() is not None:
                break
            continue

        if item is sentinel:
            reader_finished = True
            if process.poll() is not None and line_queue.empty():
                break
            continue

        line = str(item).rstrip("\r\n")
        last_output_at = time_module.monotonic()
        if not line:
            continue
        recent_lines.append(line)
        emit_detail_fn(progress_cb, f"osm2pgsql: {line}")

    reader_thread.join(timeout=1.0)
    if reader_errors:
        raise RuntimeError(
            "Failed while streaming osm2pgsql output. "
            f"Original error: {reader_errors[0]}"
        ) from reader_errors[0]
    return list(recent_lines)


def run_osm2pgsql_import_impl(
    source_state,
    *,
    importer_bin: str,
    importer_config,
    import_schema: str,
    connection_arguments_fn,
    emit_detail_fn,
    progress_cb=None,
    cache_mb: int = 0,
    flat_nodes_path: str = "",
    number_processes: int | None = None,
    subprocess_module=subprocess,
    stream_subprocess_lines_fn=stream_subprocess_lines_impl,
    os_module=os,
    datetime_cls=datetime,
    timezone_cls=timezone,
) -> None:
    if not importer_config.exists():
        raise RuntimeError(
            f"Required osm2pgsql flex config is missing at '{importer_config}'."
        )

    db_args, connection_env = connection_arguments_fn()
    command = [
        importer_bin,
        "--output=flex",
        "--slim",
        "--style",
        str(importer_config),
        "--schema",
        import_schema,
        "--middle-schema",
        import_schema,
        "--create",
        "--cache",
        str(int(cache_mb)),
    ]
    if flat_nodes_path:
        flat_nodes_abs = os_module.path.abspath(flat_nodes_path)
        flat_nodes_dir = os_module.path.dirname(flat_nodes_abs)
        if flat_nodes_dir:
            os_module.makedirs(flat_nodes_dir, exist_ok=True)
        command.extend(["--flat-nodes", flat_nodes_abs])
    if number_processes is not None:
        command.extend(["--number-processes", str(int(number_processes))])
    command.extend([*db_args, str(source_state.extract_path)])
    env = os_module.environ.copy()
    env.update(connection_env)
    # Relax Postgres durability just for the bulk load. These options apply
    # per-connection via libpq; they do NOT change the server config. We skip
    # them if the caller already set PGOPTIONS so operators keep the final say.
    if not env.get("PGOPTIONS"):
        env["PGOPTIONS"] = (
            "-c synchronous_commit=off "
            "-c maintenance_work_mem=1GB "
            "-c work_mem=64MB"
        )
    env["LIVABILITY_IMPORT_FINGERPRINT"] = source_state.import_fingerprint
    env["LIVABILITY_IMPORT_CREATED_AT"] = datetime_cls.now(timezone_cls.utc).isoformat()
    env["LIVABILITY_IMPORT_SCHEMA"] = import_schema

    emit_detail_fn(progress_cb, "running osm2pgsql --create")

    try:
        process = subprocess_module.Popen(
            command,
            env=env,
            stdin=subprocess_module.DEVNULL,
            stdout=subprocess_module.PIPE,
            stderr=subprocess_module.STDOUT,
            text=True,
            bufsize=1,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(
            "osm2pgsql is required for local OSM import, but it was not found on PATH. "
            "Install osm2pgsql or set OSM2PGSQL_BIN before running --precompute."
        ) from exc

    recent_lines: list[str] = []
    try:
        recent_lines = stream_subprocess_lines_fn(process, progress_cb=progress_cb)
        return_code = process.wait()
    finally:
        if process.stdout is not None and hasattr(process.stdout, "close"):
            process.stdout.close()

    if return_code == 0:
        return

    recent_output = "\n".join(recent_lines) if recent_lines else "no recent output captured"
    raise RuntimeError(
        "osm2pgsql import failed for the configured local .osm.pbf extract. "
        f"Exit status: {return_code}. Recent output:\n{recent_output}"
    )
