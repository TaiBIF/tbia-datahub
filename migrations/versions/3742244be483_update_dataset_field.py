"""update dataset field

Revision ID: 3742244be483
Revises: 577587ceaa54
Create Date: 2024-03-22 06:47:27.013040

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '3742244be483'
down_revision: Union[str, None] = '577587ceaa54'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('dataset', sa.Column('datasetPublisher', sa.String(length=10000), nullable=True))
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('dataset', 'datasetPublisher')
    # ### end Alembic commands ###
