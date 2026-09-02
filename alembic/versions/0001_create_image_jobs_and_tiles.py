"""Create image jobs and tile tasks."""

from alembic import op
import sqlalchemy as sa

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    job_status = sa.Enum("SPLITTING", "PROCESSING", "MERGING", "COMPLETED", "FAILED", name="jobstatus")
    tile_status = sa.Enum("QUEUED", "PROCESSING", "COMPLETED", "FAILED", name="tilestatus")
    op.create_table(
        "image_jobs",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("original_filename", sa.String(255), nullable=False),
        sa.Column("original_path", sa.String(500), nullable=False),
        sa.Column("final_path", sa.String(500)),
        sa.Column("width", sa.Integer(), nullable=False),
        sa.Column("height", sa.Integer(), nullable=False),
        sa.Column("tile_size", sa.Integer(), nullable=False),
        sa.Column("total_tiles", sa.Integer(), nullable=False),
        sa.Column("status", job_status, nullable=False),
        sa.Column("error_message", sa.String(1000)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
    )
    op.create_table(
        "tile_tasks",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("job_id", sa.String(36), sa.ForeignKey("image_jobs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("tile_index", sa.Integer(), nullable=False),
        sa.Column("x", sa.Integer(), nullable=False),
        sa.Column("y", sa.Integer(), nullable=False),
        sa.Column("width", sa.Integer(), nullable=False),
        sa.Column("height", sa.Integer(), nullable=False),
        sa.Column("input_path", sa.String(500), nullable=False),
        sa.Column("output_path", sa.String(500)),
        sa.Column("status", tile_status, nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("worker_id", sa.String(255)),
        sa.Column("processing_time_ms", sa.Integer()),
        sa.Column("error_message", sa.String(1000)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint("job_id", "tile_index"),
    )
    op.create_index("ix_tile_tasks_job_id", "tile_tasks", ["job_id"])


def downgrade() -> None:
    op.drop_table("tile_tasks")
    op.drop_table("image_jobs")
