"""add rightsHolder to match_log

Revision ID: c722d1629043
Revises: 4fe2d13eb227
Create Date: 2023-10-17 07:55:52.740176

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c722d1629043'
down_revision: Union[str, None] = '4fe2d13eb227'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('match_log', sa.Column('rights_holder', sa.String(length=10000), nullable=True))
    op.create_index(op.f('ix_match_log_rights_holder'), 'match_log', ['rights_holder'], unique=False)
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_match_log_rights_holder'), table_name='match_log')
    op.drop_column('match_log', 'rights_holder')
    # ### end Alembic commands ###