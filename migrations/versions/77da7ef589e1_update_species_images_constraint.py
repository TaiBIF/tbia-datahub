"""update species_images constraint

Revision ID: 77da7ef589e1
Revises: 6278fdefb294
Create Date: 2024-06-19 05:23:07.715705

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '77da7ef589e1'
down_revision: Union[str, None] = '6278fdefb294'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
