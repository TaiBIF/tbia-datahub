"""alter field length

Revision ID: 12ee5a0f4c80
Revises: b067a0e0b6bd
Create Date: 2023-10-26 13:23:13.681430

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '12ee5a0f4c80'
down_revision: Union[str, None] = 'b067a0e0b6bd'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
