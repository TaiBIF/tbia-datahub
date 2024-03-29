"""add is_endemic to taxon

Revision ID: a2e768a434f1
Revises: 4dfcb92fa899
Create Date: 2023-10-19 08:01:11.117096

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a2e768a434f1'
down_revision: Union[str, None] = '4dfcb92fa899'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('taxon', sa.Column('is_endemic', sa.Boolean(), nullable=True))
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('taxon', 'is_endemic')
    # ### end Alembic commands ###
