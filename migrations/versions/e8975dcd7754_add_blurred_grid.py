"""add blurred grid

Revision ID: e8975dcd7754
Revises: bbf35a371076
Create Date: 2023-12-06 03:17:01.879530

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e8975dcd7754'
down_revision: Union[str, None] = 'bbf35a371076'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('records', sa.Column('grid_1_blurred', sa.String(length=50), nullable=True))
    op.add_column('records', sa.Column('grid_5_blurred', sa.String(length=50), nullable=True))
    op.add_column('records', sa.Column('grid_10_blurred', sa.String(length=50), nullable=True))
    op.add_column('records', sa.Column('grid_100_blurred', sa.String(length=50), nullable=True))
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('records', 'grid_100_blurred')
    op.drop_column('records', 'grid_10_blurred')
    op.drop_column('records', 'grid_5_blurred')
    op.drop_column('records', 'grid_1_blurred')
    # ### end Alembic commands ###
