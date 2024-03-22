"""update match_log

Revision ID: a5e4e91d9fd2
Revises: 86d239df1fd5
Create Date: 2024-03-22 11:02:49.952273

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a5e4e91d9fd2'
down_revision: Union[str, None] = '86d239df1fd5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    # op.drop_constraint('tbiaID_unique', 'match_log', type_='unique')
    op.create_unique_constraint('tbiaid_unique', 'match_log', ['tbiaID'])
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    # op.drop_constraint('tbiaid_unique', 'match_log', type_='unique')
    # op.create_unique_constraint('tbiaID_unique', 'match_log', ['tbiaID'])
    # ### end Alembic commands ###
