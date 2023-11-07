"""add is_ fields to taxon table

Revision ID: afb671754c4e
Revises: 03267355fec4
Create Date: 2023-11-07 09:11:33.119277

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'afb671754c4e'
down_revision: Union[str, None] = '03267355fec4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('taxon', sa.Column('is_fossil', sa.Boolean(), nullable=True))
    op.add_column('taxon', sa.Column('is_terrestrial', sa.Boolean(), nullable=True))
    op.add_column('taxon', sa.Column('is_freshwater', sa.Boolean(), nullable=True))
    op.add_column('taxon', sa.Column('is_brackish', sa.Boolean(), nullable=True))
    op.add_column('taxon', sa.Column('is_marine', sa.Boolean(), nullable=True))
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('taxon', 'is_marine')
    op.drop_column('taxon', 'is_brackish')
    op.drop_column('taxon', 'is_freshwater')
    op.drop_column('taxon', 'is_terrestrial')
    op.drop_column('taxon', 'is_fossil')
    # ### end Alembic commands ###
