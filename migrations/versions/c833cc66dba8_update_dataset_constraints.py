"""update dataset constraints

Revision ID: c833cc66dba8
Revises: 3742244be483
Create Date: 2024-03-22 09:06:46.097187

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c833cc66dba8'
down_revision: Union[str, None] = '3742244be483'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint('dataset_unique', 'dataset', type_='unique')
    op.create_unique_constraint('dataset_unique', 'dataset', ['name', 'record_type', 'rights_holder', 'sourceDatasetID'])
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint('dataset_unique', 'dataset', type_='unique')
    op.create_unique_constraint('dataset_unique', 'dataset', ['name', 'record_type', 'rights_holder'])
    # ### end Alembic commands ###
