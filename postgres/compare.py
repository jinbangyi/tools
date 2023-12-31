import sys

import click
import humanfriendly
import psycopg2
# from psycopg2 import connection
from deepdiff import DeepDiff
from loguru import logger

exit_code = 0


def log(msg: str, obj, pretty: bool = False):
    global exit_code
    if obj:
        if pretty:
            for k, v in obj['values_changed'].items():
                v['new_value'] = round(v['new_value'] / pow(1024, 3), 2)
                v['old_value'] = round(v['old_value'] / pow(1024, 3), 2)
                v['delta'] = round(abs(v['new_value'] - v['old_value']), 2)
        # if pretty == 'bytes':
        #     obj = obj / pow(1024, 3)
        logger.info(f'{msg}: {obj}G')
        exit_code += 1


def size_compare(source: dict, target: dict, gap: int):
    new_source = {}
    new_target = {}
    for k in source.keys():
        if k in target:
            if abs(source.get(k) - target.get(k)) > gap:
                new_source[k] = source.get(k)
                new_target[k] = target.get(k)
        else:
            new_source[k] = source.get(k)

    [new_target.setdefault(i, target.get(i)) for i in set(target.keys()) - set(source.keys())]

    return new_source, new_target


@click.command()
@click.argument('source', type=str, required=True)
@click.argument('target', type=str, required=True)
@click.option('--level', type=str, default='DEBUG')
def start(source: str, target: str, level: str):
    # source_client = psycopg2.connect(source)
    # target_client = psycopg2.connect(target)
    logger.remove(0)  # remove default handler
    logger.add(sys.stderr, level=level.upper(), backtrace=False, diagnose=False)
    ignored_db = ['postgres', 'template0', 'template1', 'rdsadmin']

    s_databases, s_schemas, s_tables, s_tables_size, s_indices = get_postgresql_info(source, ignored_db=ignored_db)
    t_databases, t_schemas, t_tables, t_tables_size, t_indices = get_postgresql_info(target, ignored_db=ignored_db)
    # logger.debug(f'{s_databases}, {s_schemas}, {s_tables}, {s_tables_size}, {s_indices}')
    # logger.debug(f'{t_databases}, {t_schemas}, {t_tables}, {t_tables_size}, {t_indices}')

    log('db-size-diff', DeepDiff(*size_compare(
        s_databases,
        t_databases,
        1024 * 1024 * 1024 * 10
    )), pretty=True)

    log('db-schema-diff', DeepDiff(
        s_schemas,
        t_schemas,
    ))

    log('db-table-diff', DeepDiff(
        s_tables,
        t_tables,
    ))

    log('db-table-size-diff', DeepDiff(*size_compare(
        s_tables_size,
        t_tables_size,
        1024 * 1024 * 1024
    )), pretty=True)

    log('db-indices-diff', DeepDiff(
        s_indices,
        t_indices,
    ))


def get_postgresql_info(conn_str: str, ignored_db: list[str] = None):
    # Create a cursor
    conn = psycopg2.connect(conn_str)
    _cursor = conn.cursor()

    databases = {}
    schemas = {}
    tables = {}
    tables_size = {}
    indices = {}
    try:
        # Get all databases
        _cursor.execute("SELECT datname FROM pg_database")
        _databases = sorted(filter(lambda item: item not in ignored_db, [row[0] for row in _cursor.fetchall()]))
        _cursor.close()
        conn.close()
        logger.debug(f'dbs: {_databases}')

        for db in _databases:
            conn = psycopg2.connect(conn_str, database=db)
            # Connect to a specific database
            # conn.set_session(database=db)
            _cursor2 = conn.cursor()

            _cursor2.execute(f"SELECT pg_size_pretty(pg_database_size('{db}'))")
            _size = _cursor2.fetchone()[0]
            databases[db] = humanfriendly.parse_size(_size)

            # Get all schemas
            # _cursor2.execute("SELECT schema_name FROM information_schema.schemata")
            # _schemas = [row[0] for row in _cursor2.fetchall()]
            # logger.debug(f'schemas: {_schemas}')
            _schemas = ['public']
            schemas[db] = sorted(_schemas)

            for schema in _schemas:
                # Get all tables
                _cursor2.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}'")
                _tables = list(filter(lambda item: not item.startswith('awsdms'), [row[0] for row in _cursor2.fetchall()]))
                logger.debug(f'tables: {_tables}')
                tables[f'{db}-{schema}'] = sorted(_tables)

                for table in _tables:
                    _cursor2.execute(f"SELECT pg_size_pretty(pg_total_relation_size('{table}'))")
                    table_size = _cursor2.fetchone()[0]
                    tables_size[f'{db}-{schema}-{table}'] = humanfriendly.parse_size(table_size)

                    # Get all indices for each table
                    _cursor2.execute(
                        f"SELECT indexname FROM pg_indexes WHERE schemaname = '{schema}' AND tablename = '{table}'")
                    _indices = [row[0] for row in _cursor2.fetchall()]
                    # print(f"\nIndices for table '{table}':", _indices)
                    indices[f'{db}-{schema}-{table}'] = sorted(_indices)
                    # if str(table) == 'contract_payer_royalty_metrics':
                    #     print(_indices)

            _cursor2.close()
            conn.close()

    finally:
        # Close the cursor and connection
        # conn.close()
        pass

    return databases, schemas, tables, tables_size, indices


if __name__ == "__main__":
    start()
