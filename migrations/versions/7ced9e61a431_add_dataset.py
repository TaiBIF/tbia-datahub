"""add dataset

Revision ID: 7ced9e61a431
Revises: 8a7865db5fca
Create Date: 2023-10-31 04:04:45.281904

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '7ced9e61a431'
down_revision: Union[str, None] = '8a7865db5fca'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('dataset',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('name', sa.String(length=10000), nullable=False),
    sa.Column('record_type', sa.String(length=20), nullable=False),
    sa.Column('rights_holder', sa.String(length=10000), nullable=True),
    sa.Column('deprecated', sa.Boolean(), server_default='f', nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('name', 'record_type', 'rights_holder', name='dataset_unique')
    )
    op.create_index(op.f('ix_dataset_deprecated'), 'dataset', ['deprecated'], unique=False)
    op.create_index(op.f('ix_dataset_name'), 'dataset', ['name'], unique=False)
    op.create_index(op.f('ix_dataset_record_type'), 'dataset', ['record_type'], unique=False)
    op.create_index(op.f('ix_dataset_rights_holder'), 'dataset', ['rights_holder'], unique=False)
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_dataset_rights_holder'), table_name='dataset')
    op.drop_index(op.f('ix_dataset_record_type'), table_name='dataset')
    op.drop_index(op.f('ix_dataset_name'), table_name='dataset')
    op.drop_index(op.f('ix_dataset_deprecated'), table_name='dataset')
    op.drop_table('dataset')
    # ### end Alembic commands ###
