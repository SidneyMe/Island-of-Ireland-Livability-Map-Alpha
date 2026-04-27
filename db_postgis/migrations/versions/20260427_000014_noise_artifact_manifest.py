from __future__ import annotations

from alembic import op


revision = "20260427_000014"
down_revision = "20260426_000013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS noise_artifact_manifest (
            artifact_hash   TEXT        NOT NULL PRIMARY KEY,
            artifact_type   TEXT        NOT NULL,
            manifest_json   JSONB       NOT NULL,
            status          TEXT        NOT NULL DEFAULT 'building',
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            completed_at    TIMESTAMPTZ NULL,
            CHECK (artifact_type IN ('source', 'domain', 'resolved', 'tiles', 'exposure')),
            CHECK (status IN ('building', 'complete', 'failed', 'superseded'))
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS noise_artifact_manifest_status_idx
            ON noise_artifact_manifest (status)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS noise_artifact_manifest_type_status_idx
            ON noise_artifact_manifest (artifact_type, status)
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS noise_artifact_lineage (
            artifact_hash   TEXT NOT NULL
                REFERENCES noise_artifact_manifest(artifact_hash) ON DELETE CASCADE,
            parent_hash     TEXT NOT NULL
                REFERENCES noise_artifact_manifest(artifact_hash) ON DELETE RESTRICT,
            PRIMARY KEY (artifact_hash, parent_hash)
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS noise_active_artifact (
            artifact_type   TEXT NOT NULL PRIMARY KEY,
            artifact_hash   TEXT NOT NULL
                REFERENCES noise_artifact_manifest(artifact_hash)
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS noise_active_artifact")
    op.execute("DROP TABLE IF EXISTS noise_artifact_lineage")
    op.execute("DROP INDEX IF EXISTS noise_artifact_manifest_type_status_idx")
    op.execute("DROP INDEX IF EXISTS noise_artifact_manifest_status_idx")
    op.execute("DROP TABLE IF EXISTS noise_artifact_manifest")
