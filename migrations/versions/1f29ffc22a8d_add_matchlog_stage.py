"""add matchlog stage

Revision ID: 1f29ffc22a8d
Revises: ddc6d647bf01
Create Date: 2024-03-29 07:54:49.228095

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '1f29ffc22a8d'
down_revision: Union[str, None] = 'ddc6d647bf01'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('match_log', sa.Column('stage_6', sa.String(length=20), nullable=True))
    op.add_column('match_log', sa.Column('stage_7', sa.String(length=20), nullable=True))
    op.add_column('match_log', sa.Column('stage_8', sa.String(length=20), nullable=True))
    op.create_index(op.f('ix_match_log_stage_6'), 'match_log', ['stage_6'], unique=False)
    op.create_index(op.f('ix_match_log_stage_7'), 'match_log', ['stage_7'], unique=False)
    op.create_index(op.f('ix_match_log_stage_8'), 'match_log', ['stage_8'], unique=False)
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_match_log_stage_8'), table_name='match_log')
    op.drop_index(op.f('ix_match_log_stage_7'), table_name='match_log')
    op.drop_index(op.f('ix_match_log_stage_6'), table_name='match_log')
    op.drop_column('match_log', 'stage_8')
    op.drop_column('match_log', 'stage_7')
    op.drop_column('match_log', 'stage_6')
    # ### end Alembic commands ###
