"""add modified to species_images table

Revision ID: 03267355fec4
Revises: 40defb4c1f03
Create Date: 2023-11-07 06:46:25.012658

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '03267355fec4'
down_revision: Union[str, None] = '40defb4c1f03'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('species_images', sa.Column('modified', sa.DateTime(timezone=True), nullable=False))
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('species_images', 'modified')
    # ### end Alembic commands ###
