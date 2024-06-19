"""update dataset unique key

Revision ID: b9cd9bd3f66b
Revises: cf21c2ad4b71
Create Date: 2024-04-24 08:11:20.359997

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b9cd9bd3f66b'
down_revision: Union[str, None] = 'cf21c2ad4b71'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint('dataset_unique', 'dataset', type_='unique')
    op.create_unique_constraint('dataset_unique', 'dataset', ['name', 'rights_holder', 'sourceDatasetID'])
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint('dataset_unique', 'dataset', type_='unique')
    op.create_unique_constraint('dataset_unique', 'dataset', ['name', 'record_type', 'rights_holder', 'sourceDatasetID'])
    # ### end Alembic commands ###
