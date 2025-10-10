"""initial

Revision ID: 3c1a2b4d5e8f
Revises:
Create Date: 2025-10-10 14:01:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = '3c1a2b4d5e8f'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table('wishlists',
    sa.Column('wishlist_id', sa.Integer(), nullable=False),
    sa.Column('owner_user_id', sa.Integer(), nullable=False),
    sa.Column('name', sa.String(), nullable=False),
    sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
    sa.PrimaryKeyConstraint('wishlist_id')
    )
    op.create_table('wishlist_items',
    sa.Column('item_id', sa.Integer(), nullable=False),
    sa.Column('wishlist_id', sa.Integer(), nullable=False),
    sa.Column('name', sa.String(), nullable=False),
    sa.Column('item_url', sa.String(), nullable=True),
    sa.Column('added_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['wishlist_id'], ['wishlists.wishlist_id'], ),
    sa.PrimaryKeyConstraint('item_id')
    )
    op.create_table('item_bookings',
    sa.Column('booking_id', sa.Integer(), nullable=False),
    sa.Column('item_id', sa.Integer(), nullable=False),
    sa.Column('booked_by_user_id', sa.Integer(), nullable=False),
    sa.Column('booked_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['item_id'], ['wishlist_items.item_id'], ),
    sa.PrimaryKeyConstraint('booking_id'),
    sa.UniqueConstraint('item_id')
    )


def downgrade() -> None:
    op.drop_table('item_bookings')
    op.drop_table('wishlist_items')
    op.drop_table('wishlists')