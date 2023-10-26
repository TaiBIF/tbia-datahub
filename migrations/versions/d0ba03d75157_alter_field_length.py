"""alter field length

Revision ID: d0ba03d75157
Revises: 12ee5a0f4c80
Create Date: 2023-10-26 13:40:39.914518

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd0ba03d75157'
down_revision: Union[str, None] = '12ee5a0f4c80'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
