"""add species_images table

Revision ID: 40defb4c1f03
Revises: 7ced9e61a431
Create Date: 2023-11-07 06:43:05.456965

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '40defb4c1f03'
down_revision: Union[str, None] = '7ced9e61a431'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('species_images',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('taxon_name_id', sa.String(length=1000), nullable=True),
    sa.Column('namecode', sa.String(length=1000), nullable=True),
    sa.Column('taieol_id', sa.String(length=1000), nullable=True),
    sa.Column('images', sa.JSON(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('namecode', 'taxon_name_id', name='namecode')
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('species_images')
    # ### end Alembic commands ###
