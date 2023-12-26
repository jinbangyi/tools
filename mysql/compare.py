import sys

import click
from sqlalchemy import create_engine, inspect, text
from deepdiff import DeepDiff
from loguru import logger

from common import log, size_compare

exit_code = 0


@click.command()
@click.argument('source', type=str, required=True)
@click.argument('target', type=str, required=True)
@click.option('--level', type=str, default='DEBUG')
def start(source: str, target: str, level: str):
    logger.remove(0)  # remove default handler
    logger.add(sys.stderr, level=level.upper(), backtrace=False, diagnose=False)
    # ignored_db = ['postgres', 'template0', 'template1', 'rdsadmin']
    ignored_db = ['information_schema', 'mysql', 'performance_schema', 'sys', 'awsdms_control']

    s_databases, s_tables, s_indices = get_mysql_info(source, ignored_db=ignored_db)
    t_databases, t_tables, t_indices = get_mysql_info(target, ignored_db=ignored_db)

    logger.debug(f'{s_databases}, {s_tables}, {s_indices}')

    log('db-size-diff', DeepDiff(*size_compare(
        s_databases,
        t_databases,
        1024 * 1024 * 1024 * 10
    )), pretty=True)

    log('db-table-diff', DeepDiff(
        s_tables,
        t_tables,
    ))

    log('db-indices-diff', DeepDiff(
        s_indices,
        t_indices,
    ))


def get_mysql_info(connection_string: str, ignored_db: list[str]):
    # Create a SQLAlchemy engine
    engine = create_engine(connection_string)

    # Create a connection
    connection = engine.connect()

    # Create an Inspector
    inspector = inspect(engine)

    r_databases = {}
    r_tables = {}
    r_indices = {}
    r_table_count = {}
    try:
        # Get all databases
        databases = list(filter(lambda item: item not in ignored_db, inspector.get_schema_names()))

        for database in databases:
            tables = inspector.get_table_names(schema=database)
            r_tables[database] = sorted(tables)
            for table in tables:
                print(inspector.get_table_options(table, schema=database))
                r_indices[f'{database}-{table}'] = sorted(inspector.get_indexes(table, schema=database),
                                                          key=lambda item: item['name'])
                with connection.begin():
                    # Get the size of each database
                    size_query = text(f"SELECT count(*) AS 'size'"
                                      f"FROM {database}.{table}")
                    result = connection.execute(size_query).fetchall()
                    print(result)
                    count = int(result[0][0])
                    r_table_count[f'{database}-{table}'] = count

            # Use the connection to execute queries
            with connection.begin():
                # Get the size of each database
                size_query = text(f"SELECT table_schema AS 'Database', "
                                  f"SUM(data_length + index_length)"
                                  f"FROM information_schema.tables WHERE table_schema = '{database}' "
                                  f"GROUP BY table_schema")
                result = connection.execute(size_query).fetchall()
                r_databases[database] = int(result[0][1])

    finally:
        # Close the connection
        connection.close()

    return r_databases, r_tables, r_indices


if __name__ == "__main__":
    start()
