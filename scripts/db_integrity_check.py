from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy.exc import SQLAlchemyError

from db_postgis import build_engine, table_exists
from db_postgis._dependencies import Engine, func, select
from db_postgis.tables import (
    amenities,
    build_manifest,
    grid_walk,
    service_deserts,
    transit_gtfs_stop_reality,
    transit_gtfs_stop_service_summary,
    transit_service_classification,
    transit_service_desert_cells,
)


DEFAULT_SAMPLE_LIMIT = 5


@dataclass(frozen=True)
class DuplicateCheckSpec:
    label: str
    table: Any
    key_columns: tuple[str, ...]
    ambiguous: bool = False
    ambiguity_reason: str | None = None

    @property
    def display_name(self) -> str:
        return self.label


def build_duplicate_check_specs() -> tuple[DuplicateCheckSpec, ...]:
    return (
        DuplicateCheckSpec(
            label="grid_walk",
            table=grid_walk,
            key_columns=("build_key", "resolution_m", "cell_id"),
        ),
        DuplicateCheckSpec(
            label="service_deserts",
            table=service_deserts,
            key_columns=("build_key", "resolution_m", "cell_id"),
        ),
        DuplicateCheckSpec(
            label="build_manifest",
            table=build_manifest,
            key_columns=("build_key",),
        ),
        DuplicateCheckSpec(
            label="transit_derived.service_classification",
            table=transit_service_classification,
            key_columns=("reality_fingerprint", "feed_id", "service_id"),
        ),
        DuplicateCheckSpec(
            label="transit_derived.gtfs_stop_service_summary",
            table=transit_gtfs_stop_service_summary,
            key_columns=("reality_fingerprint", "feed_id", "stop_id"),
        ),
        DuplicateCheckSpec(
            label="transit_derived.gtfs_stop_reality",
            table=transit_gtfs_stop_reality,
            key_columns=("reality_fingerprint", "source_ref"),
        ),
        DuplicateCheckSpec(
            label="transit_derived.service_desert_cells",
            table=transit_service_desert_cells,
            key_columns=("build_key", "resolution_m", "cell_id"),
        ),
        DuplicateCheckSpec(
            label="amenities",
            table=amenities,
            key_columns=(),
            ambiguous=True,
            ambiguity_reason=(
                "no safe logical key could be derived from the current schema, indexes, "
                "and write paths"
            ),
        ),
    )


def _column_exprs(spec: DuplicateCheckSpec) -> list[Any]:
    return [spec.table.c[column_name] for column_name in spec.key_columns]


def build_duplicate_group_count_query(spec: DuplicateCheckSpec):
    columns = _column_exprs(spec)
    grouped = (
        select(*columns)
        .select_from(spec.table)
        .group_by(*columns)
        .having(func.count() > 1)
        .subquery()
    )
    return select(func.count()).select_from(grouped)


def build_duplicate_sample_query(spec: DuplicateCheckSpec, *, sample_limit: int = DEFAULT_SAMPLE_LIMIT):
    columns = _column_exprs(spec)
    duplicate_rows = func.count().label("duplicate_rows")
    return (
        select(*columns, duplicate_rows)
        .select_from(spec.table)
        .group_by(*columns)
        .having(func.count() > 1)
        .order_by(*columns)
        .limit(sample_limit)
    )


def _format_key_row(spec: DuplicateCheckSpec, row: dict[str, Any]) -> str:
    parts = [f"{column}={row[column]!r}" for column in spec.key_columns]
    duplicate_rows = row.get("duplicate_rows")
    suffix = "" if duplicate_rows is None else f" (duplicate rows: {duplicate_rows})"
    return "  - " + ", ".join(parts) + suffix


def _evaluate_spec(
    connection,
    spec: DuplicateCheckSpec,
    *,
    sample_limit: int = DEFAULT_SAMPLE_LIMIT,
) -> tuple[str, int, list[dict[str, Any]], str | None]:
    if spec.ambiguous:
        return "skipped", 0, [], spec.ambiguity_reason

    duplicate_group_count = int(connection.execute(build_duplicate_group_count_query(spec)).scalar_one())
    if duplicate_group_count == 0:
        return "ok", 0, [], None

    sample_rows = [
        dict(row)
        for row in connection.execute(build_duplicate_sample_query(spec, sample_limit=sample_limit)).mappings().all()
    ]
    return "fail", duplicate_group_count, sample_rows, None


def _build_engine(database_url: str | None) -> Engine:
    if database_url is None:
        return build_engine()

    previous_database_url = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = database_url
    try:
        return build_engine()
    finally:
        if previous_database_url is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = previous_database_url


def run_integrity_check(
    *,
    database_url: str | None = None,
    checks: Iterable[DuplicateCheckSpec] | None = None,
    sample_limit: int = DEFAULT_SAMPLE_LIMIT,
) -> int:
    try:
        engine = _build_engine(database_url)
    except (OSError, RuntimeError, SQLAlchemyError) as exc:
        print(f"Error: {exc}")
        return 1

    failures = 0
    checks_to_run = tuple(checks or build_duplicate_check_specs())
    for spec in checks_to_run:
        try:
            exists = True
            if not spec.ambiguous:
                exists = table_exists(engine, spec.table.name, schema=spec.table.schema or "public")
            if not exists:
                print(f"MISSING {spec.display_name}: table not found")
                failures += 1
                continue

            if spec.ambiguous:
                print(f"SKIP {spec.display_name}: {spec.ambiguity_reason}")
                continue

            with engine.connect() as connection:
                status, duplicate_group_count, sample_rows, note = _evaluate_spec(
                    connection,
                    spec,
                    sample_limit=sample_limit,
                )

            if status == "ok":
                columns = ", ".join(spec.key_columns)
                print(f"OK {spec.display_name}: no duplicates for ({columns})")
            elif status == "fail":
                columns = ", ".join(spec.key_columns)
                print(
                    f"FAIL {spec.display_name}: {duplicate_group_count} duplicate groups for ({columns})"
                )
                if sample_rows:
                    print("Sample keys:")
                    for row in sample_rows:
                        print(_format_key_row(spec, row))
                failures += 1
            elif status == "skipped":
                print(f"SKIP {spec.display_name}: {note}")
            else:
                print(f"ERROR {spec.display_name}: unexpected status {status!r}")
                failures += 1
        except (OSError, RuntimeError, SQLAlchemyError) as exc:
            print(f"ERROR {spec.display_name}: {exc}")
            failures += 1

    return 1 if failures else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only duplicate detection for logical database keys before adding "
            "future primary/unique/foreign-key constraints."
        ),
    )
    parser.add_argument(
        "--database-url",
        help=(
            "Override DATABASE_URL for this run. If omitted, the script uses the "
            "same database configuration as the rest of the project."
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return run_integrity_check(database_url=args.database_url)


if __name__ == "__main__":
    raise SystemExit(main())
