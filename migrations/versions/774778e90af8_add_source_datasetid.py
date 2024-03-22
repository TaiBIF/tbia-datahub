"""add source_datasetID

Revision ID: 774778e90af8
Revises: 0543fd1ce474
Create Date: 2024-02-15 08:50:07.094071

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '774778e90af8'
down_revision: Union[str, None] = '0543fd1ce474'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('dataset', sa.Column('source_datasetID', sa.String(length=10000), nullable=True))
    op.create_index(op.f('ix_dataset_source_datasetID'), 'dataset', ['source_datasetID'], unique=False)
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_dataset_source_datasetID'), table_name='dataset')
    op.drop_column('dataset', 'source_datasetID')
    # ### end Alembic commands ###
