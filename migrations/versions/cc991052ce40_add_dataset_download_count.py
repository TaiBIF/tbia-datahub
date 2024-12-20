"""add dataset download count

Revision ID: cc991052ce40
Revises: 9e10c074064e
Create Date: 2024-12-03 08:37:50.504263

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'cc991052ce40'
down_revision: Union[str, None] = '9e10c074064e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('dataset', sa.Column('downloadCount', sa.Integer(), server_default='0', nullable=True))
    op.drop_column('dataset', 'donwloadCount')
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('dataset', sa.Column('donwloadCount', sa.INTEGER(), server_default=sa.text('0'), autoincrement=False, nullable=True))
    op.drop_column('dataset', 'downloadCount')
    # ### end Alembic commands ###
