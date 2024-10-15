"""empty message

Revision ID: 6b81d334069a
Revises: 77da7ef589e1, faaeb962ee36
Create Date: 2024-10-15 03:36:30.173698

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6b81d334069a'
down_revision: Union[str, None] = 'faaeb962ee36'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('match_log', sa.Column('catalogNumber', sa.String(length=10000), nullable=True))
    op.add_column('deleted_records', sa.Column('catalogNumber', sa.String(length=10000), nullable=True))
    op.create_index(op.f('ix_match_log_catalogNumber'), 'match_log', ['catalogNumber'], unique=False)
    op.create_index(op.f('ix_deleted_records_catalogNumber'), 'deleted_records', ['catalogNumber'], unique=False)
    pass



def downgrade() -> None:
    op.drop_index(op.f('ix_match_log_catalogNumber'), table_name='match_log')
    op.drop_index(op.f('ix_deleted_records_catalogNumber'), table_name='deleted_records')
    op.drop_column('match_log', 'catalogNumber')
    op.drop_column('deleted_records', 'catalogNumber')
    pass
