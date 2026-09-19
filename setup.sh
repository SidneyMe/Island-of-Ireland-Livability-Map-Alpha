#!/usr/bin/env bash
# Bootstrap a local Linux/PostgreSQL development environment. On Debian/Ubuntu
# it installs missing prerequisites; other distributions must provide them.
set -Eeuo pipefail
IFS=$'\n\t'

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT"

DB_HOST="${POSTGRES_HOST:-localhost}"
DB_PORT="${POSTGRES_PORT:-5432}"
DB_NAME="${POSTGRES_DB:-livability}"
DB_USER="${POSTGRES_USER:-livability}"
DB_PASSWORD="${POSTGRES_PASSWORD:-}"
# The project-maintained release carries the required remote-install inputs.
# Set DATASET_MIRROR_BASE_URL to replace it with another full mirror.
MIRROR_BASE_URL="${DATASET_MIRROR_BASE_URL:-https://github.com/SidneyMe/livability-data/releases/download/v1}"
MIRROR_BASE_URL="${MIRROR_BASE_URL%/}"

if [[ -n "$MIRROR_BASE_URL" ]]; then
    MIRROR_OSM_URL="$MIRROR_BASE_URL/ireland-and-northern-ireland-latest.osm.pbf"
    MIRROR_ROI_BOUNDARY_URL="$MIRROR_BASE_URL/Counties_NationalStatutoryBoundaries_Ungeneralised_2024_-6732842875837866666.geojson"
    MIRROR_NI_BOUNDARY_URL="$MIRROR_BASE_URL/osni_open_data_largescale_boundaries_ni_outline.geojson"
    MIRROR_NTA_GTFS_URL="$MIRROR_BASE_URL/nta_gtfs.zip"
    MIRROR_TRANSLINK_GTFS_URL="$MIRROR_BASE_URL/translink_gtfs.zip"
    MIRROR_OVERTURE_PLACES_URL="$MIRROR_BASE_URL/ireland_places.geoparquet"
    MIRROR_MAIN_ISLAND_BOUNDARY_ARCHIVE_URL="$MIRROR_BASE_URL/ireland_main_island.zip"
    MIRROR_NOISE_ROUND4_URL="$MIRROR_BASE_URL/NOISE_Round4.zip"
    MIRROR_NOISE_ROUND3_URL="$MIRROR_BASE_URL/NOISE_Round3.zip"
    MIRROR_NOISE_ROUND2_URL="$MIRROR_BASE_URL/NOISE_Round2.zip"
    MIRROR_NOISE_NI_ROUND3_URL="$MIRROR_BASE_URL/end_noisedata_round3.zip"
    MIRROR_NOISE_NI_ROUND2_URL="$MIRROR_BASE_URL/end_noisedata_round2.zip"
    MIRROR_NOISE_NI_ROUND1_URL="$MIRROR_BASE_URL/end_noisedata_round1.zip"
fi

OSM_URL="${OSM_URL:-${MIRROR_OSM_URL:-https://download.geofabrik.de/europe/ireland-and-northern-ireland-latest.osm.pbf}}"
OSM_PATH="osm/ireland-and-northern-ireland-latest.osm.pbf"
ROI_BOUNDARY_PATH="boundaries/Counties_NationalStatutoryBoundaries_Ungeneralised_2024_-6732842875837866666.geojson"
NI_BOUNDARY_PATH="boundaries/osni_open_data_largescale_boundaries_ni_outline.geojson"
NTA_GTFS_PATH="${GTFS_NTA_ZIP_PATH:-gtfs/nta_gtfs.zip}"
TRANSLINK_GTFS_PATH="${GTFS_TRANSLINK_ZIP_PATH:-gtfs/translink_gtfs.zip}"
OVERTURE_PATH="overture/ireland_places.geoparquet"
MAIN_ISLAND_SHAPEFILE="ireland_main_island_shp/ireland_main_island.shp"

# The official boundary download endpoints are stable enough to provide
# defaults. Configure the remaining source URLs because portal release links
# change frequently.
ROI_BOUNDARY_URL="${ROI_BOUNDARY_URL:-${MIRROR_ROI_BOUNDARY_URL:-https://data-osi.opendata.arcgis.com/api/download/v1/items/dc24df2a5ce84ee9a38d9afe8431ee9b/geojson?layers=1}}"
# OSNI's portals currently return transient resets and 403s to unattended
# downloads. Use a stable GitHub mirror of the NI coastline/outline instead;
# the pipeline consumes this file's geometry only. Override either URL when an
# official source becomes reliably downloadable again.
NI_BOUNDARY_URL="${NI_BOUNDARY_URL:-${MIRROR_NI_BOUNDARY_URL:-https://github.com/SidneyMe/livability-data/releases/download/v1/osni_open_data_largescale_boundaries_ni_outline.geojson}}"
NI_BOUNDARY_FALLBACK_URL="${NI_BOUNDARY_FALLBACK_URL:-https://hub.arcgis.com/api/v3/datasets/159c80fe1ad54140b429f8799f624962_0/downloads/data?format=geojson&spatialRefId=4326&where=1%3D1}"
NTA_GTFS_URL="${GTFS_NTA_URL:-${MIRROR_NTA_GTFS_URL:-}}"
TRANSLINK_GTFS_URL="${GTFS_TRANSLINK_URL:-${MIRROR_TRANSLINK_GTFS_URL:-}}"
OVERTURE_PLACES_URL="${OVERTURE_PLACES_URL:-${MIRROR_OVERTURE_PLACES_URL:-}}"
MAIN_ISLAND_BOUNDARY_ARCHIVE_URL="${MAIN_ISLAND_BOUNDARY_ARCHIVE_URL:-${MIRROR_MAIN_ISLAND_BOUNDARY_ARCHIVE_URL:-}}"
NOISE_ROUND4_URL="${NOISE_ROUND4_URL:-${MIRROR_NOISE_ROUND4_URL:-}}"
NOISE_ROUND3_URL="${NOISE_ROUND3_URL:-${MIRROR_NOISE_ROUND3_URL:-}}"
NOISE_ROUND2_URL="${NOISE_ROUND2_URL:-${MIRROR_NOISE_ROUND2_URL:-}}"
NOISE_NI_ROUND3_URL="${NOISE_NI_ROUND3_URL:-${MIRROR_NOISE_NI_ROUND3_URL:-}}"
NOISE_NI_ROUND2_URL="${NOISE_NI_ROUND2_URL:-${MIRROR_NOISE_NI_ROUND2_URL:-}}"
NOISE_NI_ROUND1_URL="${NOISE_NI_ROUND1_URL:-${MIRROR_NOISE_NI_ROUND1_URL:-}}"

die() {
    echo "setup.sh: $*" >&2
    exit 1
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"
}

install_system_dependencies() {
    local -a missing_commands=()
    local command_name
    for command_name in cargo curl node npm osm2pgsql pg_isready psql python3 unzip; do
        command -v "$command_name" >/dev/null 2>&1 || missing_commands+=("$command_name")
    done

    if [[ ${#missing_commands[@]} -eq 0 ]]; then
        return
    fi
    if ! command -v apt-get >/dev/null 2>&1; then
        die "Missing required commands: ${missing_commands[*]}. Install them with your system package manager, then rerun setup.sh."
    fi

    local -a apt_admin=()
    if [[ "$EUID" -ne 0 ]]; then
        command -v sudo >/dev/null 2>&1 || die "Missing required commands: ${missing_commands[*]}. Rerun as root or install sudo so setup.sh can install them."
        apt_admin=(sudo)
    fi

    echo "=== 0. Installing missing system dependencies: ${missing_commands[*]} ==="
    "${apt_admin[@]}" apt-get update
    DEBIAN_FRONTEND=noninteractive "${apt_admin[@]}" apt-get install --yes \
        build-essential cargo curl gdal-bin libgdal-dev libgeos-dev libpq-dev libproj-dev \
        nodejs npm osm2pgsql postgis postgresql postgresql-contrib postgresql-postgis \
        python3 python3-dev python3-venv unzip
}

ensure_supported_rust_toolchain() {
    if cargo metadata --manifest-path walkgraph/Cargo.toml --format-version 1 --no-deps >/dev/null 2>&1; then
        return
    fi

    echo "=== Rust toolchain upgrade required for walkgraph/Cargo.lock ==="
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
    cargo metadata --manifest-path walkgraph/Cargo.toml --format-version 1 --no-deps >/dev/null \
        || die "Current stable Rust still cannot read walkgraph/Cargo.lock."
}

download_dataset() {
    local label="$1"
    local target="$2"
    local url="$3"
    local required="$4"
    local fallback_url="${5:-}"

    if [[ -s "$target" ]]; then
        echo "Reusing $label: $target"
        return
    fi
    if [[ -z "$url" ]]; then
        if [[ "$required" == "required" ]]; then
            die "Missing $label at $target. Set the corresponding download URL or place the file there."
        fi
        echo "Skipping optional $label; no download URL was configured."
        return
    fi

    mkdir -p "$(dirname "$target")"
    echo "Downloading $label..."
    if ! curl --fail --location --http1.1 --retry 2 --retry-delay 2 \
        --connect-timeout 30 --output "${target}.part" "$url"; then
        rm -f "${target}.part"
        if [[ -z "$fallback_url" ]]; then
            if [[ "$required" == "required" ]]; then
                die "Download failed for $label."
            fi
            echo "Skipping optional $label; its configured download is unavailable."
            return
        fi
        echo "Primary download failed; trying the fallback source for $label..."
        if ! curl --fail --location --http1.1 --retry 2 --retry-delay 2 \
            --connect-timeout 30 --output "${target}.part" "$fallback_url"; then
            rm -f "${target}.part"
            if [[ "$required" == "required" ]]; then
                die "Download failed for $label from both configured sources."
            fi
            echo "Skipping optional $label; both configured downloads are unavailable."
            return
        fi
    fi
    mv "${target}.part" "$target"
}

install_system_dependencies

if [[ "$DB_HOST" != "localhost" && "$DB_HOST" != "127.0.0.1" && "$DB_HOST" != "::1" ]]; then
    die "This script provisions a local PostgreSQL instance; POSTGRES_HOST must be localhost, 127.0.0.1, or ::1."
fi

for command_name in cargo curl node npm osm2pgsql pg_isready psql python3 unzip; do
    require_command "$command_name"
done
ensure_supported_rust_toolchain

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
    SERVICE_ADMIN=()
elif command -v sudo >/dev/null 2>&1; then
    PG_ADMIN=(sudo -u postgres)
    SERVICE_ADMIN=(sudo)
else
    die "Run as root or install sudo so the local postgres service can be administered."
fi

echo "=== 1. Starting PostgreSQL ==="
if ! pg_isready -q -h "$DB_HOST" -p "$DB_PORT"; then
    if [[ -d /run/systemd/system ]] && command -v systemctl >/dev/null 2>&1; then
        "${SERVICE_ADMIN[@]}" systemctl start postgresql || true
    fi
    if ! pg_isready -q -h "$DB_HOST" -p "$DB_PORT" && command -v service >/dev/null 2>&1; then
        "${SERVICE_ADMIN[@]}" service postgresql start || true
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
mkdir -p osm gtfs boundaries ireland_main_island_shp noise_datasets overture
download_dataset "OSM extract" "$OSM_PATH" "$OSM_URL" required
download_dataset "Republic of Ireland boundary" "$ROI_BOUNDARY_PATH" "$ROI_BOUNDARY_URL" required
download_dataset "Northern Ireland boundary" "$NI_BOUNDARY_PATH" "$NI_BOUNDARY_URL" required "$NI_BOUNDARY_FALLBACK_URL"
download_dataset "NTA GTFS feed" "$NTA_GTFS_PATH" "$NTA_GTFS_URL" required
download_dataset "Translink GTFS feed" "$TRANSLINK_GTFS_PATH" "$TRANSLINK_GTFS_URL" required
download_dataset "Overture places dataset" "$OVERTURE_PATH" "$OVERTURE_PLACES_URL" optional

if [[ ! -s "$MAIN_ISLAND_SHAPEFILE" && -n "$MAIN_ISLAND_BOUNDARY_ARCHIVE_URL" ]]; then
    require_command unzip
    main_island_archive="ireland_main_island_shp/ireland_main_island.zip"
    download_dataset "main-island boundary archive" "$main_island_archive" "$MAIN_ISLAND_BOUNDARY_ARCHIVE_URL" optional
    unzip -o "$main_island_archive" -d ireland_main_island_shp
    [[ -s "$MAIN_ISLAND_SHAPEFILE" ]] || die "Main-island archive did not extract $MAIN_ISLAND_SHAPEFILE."
fi
if [[ -s "$MAIN_ISLAND_SHAPEFILE" ]]; then
    echo "Reusing main-island boundary: $MAIN_ISLAND_SHAPEFILE"
else
    echo "Main-island boundary is optional; the two required GeoJSON boundaries will be merged instead."
fi

download_dataset "ROI Round 4 noise archive" "noise_datasets/NOISE_Round4.zip" "$NOISE_ROUND4_URL" optional
download_dataset "ROI Round 3 noise archive" "noise_datasets/NOISE_Round3.zip" "$NOISE_ROUND3_URL" optional
download_dataset "ROI Round 2 noise archive" "noise_datasets/NOISE_Round2.zip" "$NOISE_ROUND2_URL" optional
download_dataset "NI Round 3 noise archive" "noise_datasets/end_noisedata_round3.zip" "$NOISE_NI_ROUND3_URL" optional
download_dataset "NI Round 2 noise archive" "noise_datasets/end_noisedata_round2.zip" "$NOISE_NI_ROUND2_URL" optional
download_dataset "NI Round 1 noise archive" "noise_datasets/end_noisedata_round1.zip" "$NOISE_NI_ROUND1_URL" optional

echo "=== 4. Building Rust walkgraph ==="
cargo build --release --manifest-path walkgraph/Cargo.toml
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

echo "=== Setup complete ==="
echo "Database settings are available through .env in future shells."
echo "Optional Overture and noise source URLs are documented in .env.example; raw noise files must be converted to an artifact before use."
