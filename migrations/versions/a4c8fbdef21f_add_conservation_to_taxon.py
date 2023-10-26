"""add conservation to taxon

Revision ID: a4c8fbdef21f
Revises: 7da0c2681609
Create Date: 2023-10-18 05:20:01.879816

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a4c8fbdef21f'
down_revision: Union[str, None] = '7da0c2681609'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('taxon', sa.Column('cites', sa.String(length=10000), nullable=True))
    op.add_column('taxon', sa.Column('iucn', sa.String(length=10000), nullable=True))
    op.add_column('taxon', sa.Column('redlist', sa.String(length=10000), nullable=True))
    op.add_column('taxon', sa.Column('protected', sa.String(length=10000), nullable=True))
    op.add_column('taxon', sa.Column('sensitive', sa.String(length=10000), nullable=True))
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('taxon', 'sensitive')
    op.drop_column('taxon', 'protected')
    op.drop_column('taxon', 'redlist')
    op.drop_column('taxon', 'iucn')
    op.drop_column('taxon', 'cites')
    # ### end Alembic commands ###