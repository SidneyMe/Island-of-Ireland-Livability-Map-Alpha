#!/usr/bin/env bash
# One-command bootstrap for a Linux host: installs every prerequisite (Debian/
# Ubuntu), provisions a local PostgreSQL/PostGIS database, downloads every
# dataset, builds the helpers, and runs the full pipeline up to a servable map.
#
#   ./setup.sh                 install everything and build the published map
#   ./setup.sh --skip-build    install everything, skip the multi-hour pipeline
#   ./setup.sh --refresh-data  re-download OSM and GTFS from official sources
#
# Reruns are safe: finished downloads, installs, and pipeline stages are reused.
set -Eeuo pipefail
IFS=$'\n\t'

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT"

SKIP_BUILD=0
REFRESH_DATA=0
for arg in "$@"; do
    case "$arg" in
        --skip-build) SKIP_BUILD=1 ;;
        --refresh-data) REFRESH_DATA=1 ;;
        -h|--help)
            sed -n '2,10p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *)
            echo "setup.sh: unknown option: $arg (see --help)" >&2
            exit 2
            ;;
    esac
done

DB_HOST="${POSTGRES_HOST:-localhost}"
DB_PORT="${POSTGRES_PORT:-5432}"
DB_NAME="${POSTGRES_DB:-livability}"
DB_USER="${POSTGRES_USER:-livability}"
DB_PASSWORD="${POSTGRES_PASSWORD:-}"
# Dataset sources (mirror, per-dataset *_URL overrides, official fallbacks)
# live in scripts/fetch_datasets.py, shared with setup.ps1.

die() {
    echo "setup.sh: $*" >&2
    exit 1
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"
}

if [[ "$EUID" -eq 0 ]]; then
    ADMIN=()
elif command -v sudo >/dev/null 2>&1; then
    ADMIN=(sudo)
else
    ADMIN=()
fi

require_admin() {
    [[ "$EUID" -eq 0 || ${#ADMIN[@]} -gt 0 ]] || die "$1 Rerun as root or install sudo."
}

install_system_dependencies() {
    local -a missing_commands=()
    local command_name
    for command_name in cargo curl ogr2ogr osm2pgsql pg_isready psql python3 unzip; do
        command -v "$command_name" >/dev/null 2>&1 || missing_commands+=("$command_name")
    done
    python3 -m venv --help >/dev/null 2>&1 || missing_commands+=("python3-venv")

    if [[ ${#missing_commands[@]} -eq 0 ]]; then
        return
    fi
    if ! command -v apt-get >/dev/null 2>&1; then
        die "Missing required commands: ${missing_commands[*]}. Install them with your system package manager, then rerun setup.sh."
    fi
    require_admin "Missing required commands: ${missing_commands[*]}."

    echo "=== 0. Installing missing system dependencies: ${missing_commands[*]} ==="
    "${ADMIN[@]}" apt-get update
    DEBIAN_FRONTEND=noninteractive "${ADMIN[@]}" apt-get install --yes \
        build-essential ca-certificates cargo curl gdal-bin git libgdal-dev libgeos-dev libpq-dev \
        libproj-dev osm2pgsql postgis postgresql postgresql-contrib postgresql-postgis \
        python3 python3-dev python3-venv unzip
}

node_is_supported() {
    # The frontend build (Vite 7, JSON import attributes) needs Node >= 22,
    # matching CI; distro packages on Debian 12 / Ubuntu 24.04 ship Node 18.
    command -v node >/dev/null 2>&1 && command -v npm >/dev/null 2>&1 \
        && node -e 'process.exit(Number(process.versions.node.split(".")[0]) >= 22 ? 0 : 1)'
}

install_node() {
    if node_is_supported; then
        return
    fi
    command -v apt-get >/dev/null 2>&1 || die "Node.js 22+ is required. Install it, then rerun setup.sh."
    require_admin "Node.js 22+ is required for the frontend build."
    echo "=== 0a. Installing Node.js 22 (NodeSource) ==="
    local node_installer
    node_installer="$(mktemp)"
    curl --fail --location --proto '=https' --tlsv1.2 --retry 2 \
        https://deb.nodesource.com/setup_22.x -o "$node_installer"
    "${ADMIN[@]}" bash "$node_installer"
    rm -f "$node_installer"
    DEBIAN_FRONTEND=noninteractive "${ADMIN[@]}" apt-get install --yes nodejs
    node_is_supported || die "Node.js 22+ is still unavailable after installing NodeSource nodejs."
}

install_docker() {
    # The self-hosted basemap build uses `docker compose run --build`, which
    # needs the Compose v2 plugin; distro docker-compose v1 packages are not
    # enough, so use Docker's official installer when Compose v2 is missing.
    if ! command -v docker >/dev/null 2>&1 || ! docker compose version >/dev/null 2>&1; then
        require_admin "Docker with the Compose v2 plugin is required for the basemap."
        echo "=== 0b. Installing Docker Engine + Compose plugin ==="
        local docker_installer
        docker_installer="$(mktemp)"
        curl --fail --location --proto '=https' --tlsv1.2 --retry 2 \
            https://get.docker.com -o "$docker_installer"
        "${ADMIN[@]}" sh "$docker_installer"
        rm -f "$docker_installer"
    fi
    if ! "${ADMIN[@]}" docker info >/dev/null 2>&1; then
        if [[ -d /run/systemd/system ]] && command -v systemctl >/dev/null 2>&1; then
            "${ADMIN[@]}" systemctl enable --now docker || true
        elif command -v service >/dev/null 2>&1; then
            "${ADMIN[@]}" service docker start || true
        fi
    fi
    local current_user="${USER:-$(id -un)}"
    if [[ "$EUID" -ne 0 ]] && ! id -nG "$current_user" | tr ' ' '\n' | grep -qx docker; then
        "${ADMIN[@]}" usermod -aG docker "$current_user"
        echo "Added $current_user to the docker group (takes effect in new login shells)."
    fi
}

# Run a command that talks to the Docker daemon, even when the docker group
# membership added above is not active in this shell yet.
with_docker() {
    if docker info >/dev/null 2>&1 || [[ "$EUID" -eq 0 ]]; then
        "$@"
    elif command -v sg >/dev/null 2>&1; then
        local quoted
        quoted="$(printf '%q ' "$@")"
        sg docker -c "$quoted"
    else
        die "Docker is installed but not usable by ${USER:-$(id -un)}; log out and back in, then rerun setup.sh."
    fi
}

install_stable_rust() {
    echo "=== Installing the current stable Rust toolchain (rustup) ==="
    if ! command -v rustup >/dev/null 2>&1; then
        local rustup_installer
        rustup_installer="$(mktemp)"
        curl --fail --location --proto '=https' --tlsv1.2 --retry 2 \
            https://sh.rustup.rs -o "$rustup_installer"
        sh "$rustup_installer" -y --profile minimal
        rm -f "$rustup_installer"
    fi

    export PATH="$HOME/.cargo/bin:$PATH"
    command -v rustup >/dev/null 2>&1 || die "rustup installation did not provide a usable toolchain."
    rustup toolchain install stable --profile minimal
    rustup default stable
}

build_walkgraph() {
    # Distro Cargo can be too old for walkgraph (Cargo.lock v4 needs >= 1.78,
    # and `cargo metadata --no-deps` does not read the lockfile, so only a real
    # build proves it). Fall back to rustup stable once, then fail loudly.
    [[ -d "$HOME/.cargo/bin" ]] && export PATH="$HOME/.cargo/bin:$PATH"
    if cargo build --release --manifest-path walkgraph/Cargo.toml; then
        return
    fi
    install_stable_rust
    cargo build --release --manifest-path walkgraph/Cargo.toml \
        || die "walkgraph failed to build with the current stable Rust toolchain."
}

install_system_dependencies
install_node
install_docker

if [[ "$DB_HOST" != "localhost" && "$DB_HOST" != "127.0.0.1" && "$DB_HOST" != "::1" ]]; then
    die "This script provisions a local PostgreSQL instance; POSTGRES_HOST must be localhost, 127.0.0.1, or ::1."
fi

for command_name in cargo curl docker node npm ogr2ogr osm2pgsql pg_isready psql python3 unzip; do
    require_command "$command_name"
done

if [[ -z "$DB_PASSWORD" ]]; then
    if [[ -t 0 ]]; then
        while [[ -z "$DB_PASSWORD" ]]; do
            read -r -s -p "Password for PostgreSQL role '$DB_USER': " DB_PASSWORD
            echo
            [[ -n "$DB_PASSWORD" ]] || echo "Password cannot be empty." >&2
        done
    else
        die "Set POSTGRES_PASSWORD before running non-interactively."
    fi
fi

export POSTGRES_HOST="$DB_HOST"
export POSTGRES_PORT="$DB_PORT"
export POSTGRES_DB="$DB_NAME"
export POSTGRES_USER="$DB_USER"
export POSTGRES_PASSWORD="$DB_PASSWORD"
# The split variables above are URL-escaped by config.database_url(). Do not
# let a stale DATABASE_URL silently point migrations at another database.
unset DATABASE_URL

write_runtime_env() {
    if [[ -e .env ]]; then
        echo "Keeping existing .env; ensure it contains the matching POSTGRES_* settings."
        return
    fi

    (
        umask 077
        printf '%s\n' \
            '# Created by setup.sh. Keep this file private.' \
            "POSTGRES_HOST=$DB_HOST" \
            "POSTGRES_PORT=$DB_PORT" \
            "POSTGRES_DB=$DB_NAME" \
            "POSTGRES_USER=$DB_USER" \
            "POSTGRES_PASSWORD=$DB_PASSWORD" \
            "WALKGRAPH_BIN=$WALKGRAPH_BIN" \
            > .env
    )
    echo "Created .env with owner-only permissions for future shells."
}

if [[ "$EUID" -eq 0 ]]; then
    PG_ADMIN=(runuser -u postgres --)
else
    require_admin "The local postgres service must be administered."
    PG_ADMIN=(sudo -u postgres)
fi

echo "=== 1. Starting PostgreSQL ==="
if ! pg_isready -q -h "$DB_HOST" -p "$DB_PORT"; then
    if [[ -d /run/systemd/system ]] && command -v systemctl >/dev/null 2>&1; then
        "${ADMIN[@]}" systemctl start postgresql || true
    fi
    if ! pg_isready -q -h "$DB_HOST" -p "$DB_PORT" && command -v service >/dev/null 2>&1; then
        "${ADMIN[@]}" service postgresql start || true
    fi
fi
pg_isready -q -h "$DB_HOST" -p "$DB_PORT" || die "PostgreSQL is not accepting connections on $DB_HOST:$DB_PORT."

echo "=== 2. Setting up database and extensions ==="
"${PG_ADMIN[@]}" psql --set=ON_ERROR_STOP=1 --set=db_user="$DB_USER" --set=db_password="$DB_PASSWORD" -p "$DB_PORT" -d postgres <<'SQL'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L CREATEDB', :'db_user', :'db_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'db_user')
\gexec
SELECT format('ALTER ROLE %I LOGIN PASSWORD %L CREATEDB', :'db_user', :'db_password')
\gexec
SQL

"${PG_ADMIN[@]}" psql --set=ON_ERROR_STOP=1 --set=db_name="$DB_NAME" --set=db_user="$DB_USER" -p "$DB_PORT" -d postgres <<'SQL'
SELECT format('CREATE DATABASE %I OWNER %I', :'db_name', :'db_user')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = :'db_name')
\gexec
SQL

"${PG_ADMIN[@]}" psql --set=ON_ERROR_STOP=1 -p "$DB_PORT" -d "$DB_NAME" <<'SQL'
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pgcrypto;
SQL

echo "=== 3. Downloading and validating local datasets ==="
fetch_args=()
[[ "$REFRESH_DATA" -eq 1 ]] && fetch_args+=(--refresh)
python3 scripts/fetch_datasets.py "${fetch_args[@]}"

echo "=== 4. Building Rust walkgraph ==="
build_walkgraph
export WALKGRAPH_BIN="$PROJECT_ROOT/walkgraph/target/release/walkgraph"
write_runtime_env

echo "=== 5. Installing Python dependencies and running migrations ==="
if [[ ! -x .venv/bin/python ]]; then
    python3 -m venv .venv
fi
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m alembic upgrade head

echo "=== 6. Building frontend bundle ==="
npm ci --prefix frontend
npm run build --prefix frontend

if [[ "$SKIP_BUILD" -eq 1 ]]; then
    echo "=== Setup complete (pipeline skipped: --skip-build) ==="
    echo "Build the map later with: ./setup.sh   (reruns reuse everything already done)"
    exit 0
fi

echo "=== 7. Importing OSM and building the self-hosted basemap ==="
with_docker .venv/bin/python main.py import

echo "=== 8. Building GTFS transit reality ==="
.venv/bin/python main.py transit

echo "=== 9. Precomputing the full map (accurate noise overlay) ==="
.venv/bin/python main.py precompute --profile full

echo "=== 10. Sanity-checking the published build ==="
.venv/bin/python scripts/sanity_check.py --profile full

echo "=== Setup complete ==="
echo "Start the map with: .venv/bin/python main.py serve --deployment   (then open http://127.0.0.1:8000)"
