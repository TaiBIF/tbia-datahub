"""update media_rule

Revision ID: 86d239df1fd5
Revises: 2ffce611ff5f
Create Date: 2024-03-22 09:53:35.354086

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '86d239df1fd5'
down_revision: Union[str, None] = '2ffce611ff5f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('media_rule',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('rights_holder', sa.String(length=10000), nullable=True),
    sa.Column('media_rule', sa.String(length=10000), nullable=True),
    sa.Column('modified', sa.DateTime(timezone=True), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('rights_holder', 'media_rule', name='media_rule_unique')
    )
    op.create_index(op.f('ix_media_rule_media_rule'), 'media_rule', ['media_rule'], unique=False)
    op.create_index(op.f('ix_media_rule_rights_holder'), 'media_rule', ['rights_holder'], unique=False)
    op.drop_index('ix_image_rule_image_rule', table_name='image_rule')
    op.drop_index('ix_image_rule_rights_holder', table_name='image_rule')
    op.drop_table('image_rule')
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('image_rule',
    sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('rights_holder', sa.VARCHAR(length=10000), autoincrement=False, nullable=True),
    sa.Column('image_rule', sa.VARCHAR(length=10000), autoincrement=False, nullable=True),
    sa.Column('modified', postgresql.TIMESTAMP(timezone=True), autoincrement=False, nullable=False),
    sa.PrimaryKeyConstraint('id', name='image_rule_pkey'),
    sa.UniqueConstraint('rights_holder', 'image_rule', name='rule_unique')
    )
    op.create_index('ix_image_rule_rights_holder', 'image_rule', ['rights_holder'], unique=False)
    op.create_index('ix_image_rule_image_rule', 'image_rule', ['image_rule'], unique=False)
    op.drop_index(op.f('ix_media_rule_rights_holder'), table_name='media_rule')
    op.drop_index(op.f('ix_media_rule_media_rule'), table_name='media_rule')
    op.drop_table('media_rule')
    # ### end Alembic commands ###
