"""add tbiaDatasetID to records

Revision ID: b99409471b4e
Revises: 1f29ffc22a8d
Create Date: 2024-04-23 06:10:48.915984

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b99409471b4e'
down_revision: Union[str, None] = '1f29ffc22a8d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('records', sa.Column('tbiaDatasetID', sa.String(length=10000), server_default='', nullable=True))
    op.create_index(op.f('ix_records_tbiaDatasetID'), 'records', ['tbiaDatasetID'], unique=False)
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_records_tbiaDatasetID'), table_name='records')
    op.drop_column('records', 'tbiaDatasetID')
    # ### end Alembic commands ###