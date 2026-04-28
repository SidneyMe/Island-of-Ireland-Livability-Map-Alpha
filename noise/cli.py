from __future__ import annotations

from pathlib import Path

from .signature import NOISE_DATA_DIR
from .source_discovery import ni_round1_class_snapshot


def _print_ni_round1_class_snapshot(data_dir: Path) -> None:
    rows = ni_round1_class_snapshot(data_dir=data_dir)
    if not rows:
        print("No NI Round 1 class rows found.", flush=True)
        return
    print(
        "source_layer\tmetric\tsource_type\traw_gridcode\tnoise_class_label\trow_count",
        flush=True,
    )
    for row in rows:
        print(
            f"{row['source_layer']}\t{row['metric']}\t{row['source_type']}\t"
            f"{row['raw_gridcode']}\t{row['noise_class_label']}\t{row['row_count']}",
            flush=True,
        )


def _main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Noise loader diagnostics")
    parser.add_argument(
        "--dump-ni-round1-classes",
        action="store_true",
        help="Dump NI Round 1 GRIDCODE/Noise_Cl class pairs from raw archive attributes.",
    )
    parser.add_argument(
        "--data-dir",
        default=str(NOISE_DATA_DIR),
        help="Path to noise_datasets directory.",
    )
    args = parser.parse_args(argv)

    if args.dump_ni_round1_classes:
        _print_ni_round1_class_snapshot(Path(args.data_dir))
        return 0

    parser.print_help()
    return 1
