import sys

import click
import humanfriendly
import psycopg2
# from psycopg2 import connection
from deepdiff import DeepDiff
from loguru import logger

from common import size_compare, log

exit_code = 0


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

    s_databases, s_schemas, s_tables, s_tables_size, s_indices, s_tables_count = get_postgresql_info(source, ignored_db=ignored_db)
    t_databases, t_schemas, t_tables, t_tables_size, t_indices, t_tables_count = get_postgresql_info(target, ignored_db=ignored_db)
    # logger.debug(f'{s_databases}, {s_schemas}, {s_tables}, {s_tables_size}, {s_indices}')
    # logger.debug(f'{t_databases}, {t_schemas}, {t_tables}, {t_tables_size}, {t_indices}')

    log('db-size-diff', DeepDiff(*size_compare(
        s_databases,
        t_databases,
        1024 * 1024 * 100
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
        1024 * 1024 * 10
    )), pretty=True)

    log('db-table-count-diff', DeepDiff(*size_compare(
        s_tables_count,
        t_tables_count,
        gap=0,
        op='lt'
    )), pretty=True)

    log('db-indices-diff', DeepDiff(
        s_indices,
        t_indices,
    ))


def get_postgresql_info(conn_str: str, ignored_db: list[str] = None):
    # Create a cursor
    conn = psycopg2.connect(conn_str)
    _cursor = conn.cursor()

    # db name and size
    databases = {}
    # db name and schemas
    schemas = {}
    # db-name_db-schema and tables
    tables = {}
    # db-name_schema_table and size
    tables_size = {}
    # db-name_schema_table and count
    tables_count = {}
    # db-name_schema_table and index
    tables_indices = {}
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
                    tables_indices[f'{db}-{schema}-{table}'] = sorted(_indices)
                    # if str(table) == 'contract_payer_royalty_metrics':
                    #     print(_indices)

                    # get table count
                    query = f"""
                        SELECT n_live_tup
                        FROM pg_stat_all_tables
                        WHERE schemaname='public' AND relname = '{table}'
                    """
                    _cursor2.execute(query)
                    table_count = _cursor2.fetchone()[0]
                    tables_count[f'{db}-{schema}-{table}'] = int(table_count)

            _cursor2.close()
            conn.close()

    finally:
        # Close the cursor and connection
        # conn.close()
        pass

    return databases, schemas, tables, tables_size, tables_indices, tables_count


if __name__ == "__main__":
    start()
