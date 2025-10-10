"""initial

Revision ID: 4d5b6c7e8f90
Revises:
Create Date: 2025-10-10 14:02:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = '4d5b6c7e8f90'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table('tickets',
    sa.Column('ticket_id', sa.Integer(), nullable=False),
    sa.Column('requester_user_id', sa.Integer(), nullable=False),
    sa.Column('title', sa.String(), nullable=False),
    sa.Column('status', sa.String(), nullable=False),
    sa.Column('event_date', sa.DateTime(), nullable=True),
    sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
    sa.PrimaryKeyConstraint('ticket_id')
    )
    op.create_table('ticket_comments',
    sa.Column('comment_id', sa.Integer(), nullable=False),
    sa.Column('ticket_id', sa.Integer(), nullable=False),
    sa.Column('author_user_id', sa.Integer(), nullable=False),
    sa.Column('text', sa.String(), nullable=False),
    sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['ticket_id'], ['tickets.ticket_id'], ),
    sa.PrimaryKeyConstraint('comment_id')
    )


def downgrade() -> None:
    op.drop_table('ticket_comments')
    op.drop_table('tickets')