"""lightwood_file

Revision ID: ccab3318c75c
Revises: cada7d2be947
Create Date: 2022-10-03 10:55:37.176223

"""
from alembic import op
import sqlalchemy as sa
import mindsdb.interfaces.storage.db as db

from pathlib import Path

from mindsdb.utilities.config import Config


# revision identifiers, used by Alembic.
revision = 'ccab3318c75c'
down_revision = 'cada7d2be947'
branch_labels = None
depends_on = None


def upgrade():
    config = Config()
    is_cloud = config.get('cloud', False)
    if is_cloud is True:
        return

    conn = op.get_bind()
    # session = sa.orm.Session(bind=conn)

    storage_path = Path(config.paths['storage'])

    predictors = conn.execute('''
        select id, code
        from predictor
        where code is not null
    ''').fetchall()

    for predictor in predictors:
        predictor_folder = storage_path / f'predictor_None_{predictor["id"]}'
        predictor_folder.mkdir(parents=True, exists_ok=True)
        code_path = predictor_folder / 'code.py'
        if code_path.exists():
            continue
        with code_path.open('wt', encoding='utf-8') as f:
            f.write(predictor['code'])

    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.drop_column('code')


def downgrade():
    config = Config()
    is_cloud = config.get('cloud', False)
    if is_cloud is True:
        return

    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.add_column(sa.Column('code', sa.String, nullable=True))

    conn = op.get_bind()
    session = sa.orm.Session(bind=conn)

    storage_path = Path(config.paths['storage'])
    for item in storage_path.iterdir():
        if item.is_dir() and item.name.startswith('predictor_'):
            predictor_id = int(item.name.split('_')[2])
            predicrtor_record = (
                session.query(db.Predictor)
                .filter_by(company_id=None, id=predictor_id)
                .first()
            )
            if predicrtor_record is None:
                continue

            predictor_folder = storage_path / f'predictor_None_{predicrtor_record.id}'
            code_path = predictor_folder / 'code.py'

            if code_path.exists() is False:
                continue

            with code_path.open('wt', encoding='utf-8') as f:
                code = f.read()

            predicrtor_record.code = code

    session.commit()
