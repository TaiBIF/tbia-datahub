"""add raw town / county

Revision ID: 6aa164420256
Revises: 6720016de3dc
Create Date: 2025-01-03 02:51:27.434134

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6aa164420256'
down_revision: Union[str, None] = '6720016de3dc'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('records', sa.Column('raw_county', sa.String(length=50), nullable=True))
    op.add_column('records', sa.Column('raw_town', sa.String(length=50), nullable=True))
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('records', 'raw_town')
    op.drop_column('records', 'raw_county')
    # ### end Alembic commands ###
