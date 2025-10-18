"""initial notification schema

Revision ID: a1b2c3d4e5f6
Revises:
Create Date: 2025-10-16 17:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table('scheduled_notifications',
        sa.Column('id', sa.Integer(), nullable=False, primary_key=True, autoincrement=True),
        sa.Column('user_id', sa.Integer(), nullable=False, index=True),
        sa.Column('message', sa.Text(), nullable=True),
        sa.Column('send_at', sa.DateTime(), nullable=False, index=True),
        sa.Column('status', sa.String(), nullable=True, default='pending', index=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
        sa.Column('storage_key', sa.String(), nullable=True),
        sa.Column('document_caption', sa.Text(), nullable=True)
    )


def downgrade() -> None:
    op.drop_table('scheduled_notifications')