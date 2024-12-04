"""add dataset download count

Revision ID: 9e10c074064e
Revises: ddd9fd02e5e5
Create Date: 2024-12-03 08:31:18.496824

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '9e10c074064e'
down_revision: Union[str, None] = 'ddd9fd02e5e5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('dataset', sa.Column('donwloadCount', sa.Integer(), server_default='0', nullable=True))
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('dataset', 'donwloadCount')
    # ### end Alembic commands ###
