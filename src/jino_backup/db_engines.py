"""Database types and stable, disjoint private identities."""
from .errors import BackupError
from .importers import identity

ENGINE_LABELS = {'mysql':'MySQL', 'postgresql':'PostgreSQL'}


def database_engine(value):
    if value not in ENGINE_LABELS:
        raise BackupError('DATABASE_ENGINE_UNSUPPORTED')
    return value


def database_alias(account, name, engine='mysql'):
    database_engine(engine)
    return identity('db',account,name) if engine=='mysql' else identity('db',account,engine,name)
