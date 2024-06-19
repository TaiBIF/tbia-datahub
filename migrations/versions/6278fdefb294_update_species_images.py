"""update species_images

Revision ID: 6278fdefb294
Revises: b9cd9bd3f66b
Create Date: 2024-06-19 05:22:06.146341

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6278fdefb294'
down_revision: Union[str, None] = 'b9cd9bd3f66b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
