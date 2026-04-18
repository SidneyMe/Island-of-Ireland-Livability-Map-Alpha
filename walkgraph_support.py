from __future__ import annotations

import re
import shutil
import subprocess
from functools import lru_cache
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
WALKGRAPH_PROJECT_DIR = REPO_ROOT / "walkgraph"
WALKGRAPH_TARGET_DIR = WALKGRAPH_PROJECT_DIR / "target"


def walkgraph_runtime_error(detail: str) -> RuntimeError:
    return RuntimeError(
        f"{detail} Rebuild the Rust CLI with `cargo build --release` in `walkgraph/` "
        "or set WALKGRAPH_BIN to a current binary."
    )


def clear_walkgraph_probe_caches() -> None:
    _resolve_walkgraph_binary_path.cache_clear()
    _latest_repo_source_mtime_ns.cache_clear()
    _probe_walkgraph_help.cache_clear()


@lru_cache(maxsize=None)
def _resolve_walkgraph_binary_path(walkgraph_bin: str) -> str | None:
    candidate = Path(walkgraph_bin).expanduser()
    if candidate.is_file():
        return str(candidate.resolve())
    discovered = shutil.which(walkgraph_bin)
    if discovered:
        return str(Path(discovered).resolve())
    return None


def _is_repo_local_binary(binary_path: Path) -> bool:
    return binary_path.is_relative_to(WALKGRAPH_TARGET_DIR)


@lru_cache(maxsize=1)
def _latest_repo_source_mtime_ns() -> int:
    latest_mtime_ns = 0
    candidates = [WALKGRAPH_PROJECT_DIR / "Cargo.toml"]
    candidates.extend(sorted((WALKGRAPH_PROJECT_DIR / "src").rglob("*.rs")))
    for candidate in candidates:
        try:
            latest_mtime_ns = max(latest_mtime_ns, candidate.stat().st_mtime_ns)
        except OSError:
            continue
    return latest_mtime_ns


def _repo_local_binary_is_stale(binary_path: Path) -> bool:
    try:
        binary_mtime_ns = binary_path.stat().st_mtime_ns
    except OSError:
        return False
    latest_source_mtime_ns = _latest_repo_source_mtime_ns()
    return latest_source_mtime_ns > 0 and binary_mtime_ns < latest_source_mtime_ns


@lru_cache(maxsize=None)
def _probe_walkgraph_help(binary_path: str) -> str:
    try:
        completed = subprocess.run(
            [binary_path, "--help"],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError as exc:
        raise walkgraph_runtime_error(f"walkgraph binary '{binary_path}' was not found.") from exc
    output = "\n".join(part for part in (completed.stdout, completed.stderr) if part).strip()
    if completed.returncode != 0:
        detail = (
            f"Unable to inspect walkgraph binary '{binary_path}' via '--help'."
            if not output
            else f"Unable to inspect walkgraph binary '{binary_path}' via '--help': {output}"
        )
        raise walkgraph_runtime_error(detail)
    return output


def _help_includes_subcommand(help_output: str, required_subcommand: str) -> bool:
    return re.search(rf"(?m)^\s*{re.escape(required_subcommand)}\b", help_output) is not None


def ensure_walkgraph_subcommand_available(
    walkgraph_bin: str,
    required_subcommand: str,
) -> Path:
    resolved_binary = _resolve_walkgraph_binary_path(walkgraph_bin)
    if resolved_binary is None:
        raise walkgraph_runtime_error(f"walkgraph binary '{walkgraph_bin}' was not found.")

    binary_path = Path(resolved_binary)
    if _is_repo_local_binary(binary_path) and _repo_local_binary_is_stale(binary_path):
        raise walkgraph_runtime_error(
            f"Configured walkgraph binary '{binary_path}' is older than the current Rust source "
            f"and may be missing required subcommand '{required_subcommand}'."
        )

    help_output = _probe_walkgraph_help(str(binary_path))
    if not _help_includes_subcommand(help_output, required_subcommand):
        raise walkgraph_runtime_error(
            f"Configured walkgraph binary '{binary_path}' does not support required subcommand "
            f"'{required_subcommand}'."
        )

    return binary_path
