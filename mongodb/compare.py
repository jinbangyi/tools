import click
from pymongo import MongoClient
from pymongo.database import Database
from deepdiff import DeepDiff
from loguru import logger


def size_compare(source: dict, target: dict, gap: int):
    new_source = {}
    new_target = {}
    for k in source.keys():
        if k in target:
            if abs(source.get(k) - target.get(k)) > gap:
                new_source[k] = source.get(k)
                new_target = target.get(k)

    return new_target, new_target


def compare_db(source: MongoClient, target: MongoClient, ignored_db: list[str] = None):
    source_dbs = list(filter(lambda item: item['name'] not in ignored_db, source.list_databases()))
    target_dbs = list(filter(lambda item: item['name'] not in ignored_db, target.list_databases()))
    logger.debug(f'source dbs {source_dbs}')

    db_diff = DeepDiff(
        sorted([i['name'] for i in source_dbs]),
        sorted([i['name'] for i in target_dbs]),
    )
    if db_diff:
        logger.info(f'db-diff: {db_diff}')

    size_diff = DeepDiff(*size_compare(
        dict([(i['name'], i['sizeOnDisk']) for i in source_dbs]),
        dict([(i['name'], i['sizeOnDisk']) for i in target_dbs]),
        1024*1024
    ))
    if size_diff:
        logger.info(f'db-size-diff: {size_diff}')


def compare_collection(source: Database, target: Database):
    source_collections = sorted(source.list_collection_names())
    target_collections = sorted(target.list_collection_names())
    collection_diff = DeepDiff(source_collections, target_collections)
    logger.debug(f'source collections {source_collections}')

    if collection_diff:
        logger.info(f'collection-diff: {collection_diff}')

    source_status = dict([(i, source.command('collStats', i)) for i in source_collections])
    target_status = dict([(i, source.command('collStats', i)) for i in target_collections])

    collection_size_diff = DeepDiff(*size_compare(
        dict([(k, v['size']) for k, v in source_status.items()]),
        dict([(k, v['size']) for k, v in target_status.items()]),
        1024 * 1024
    ))
    if collection_size_diff:
        logger.info(f'collection-size-diff: {collection_size_diff}')

    collection_count_diff = DeepDiff(*size_compare(
        dict([(k, v['count']) for k, v in source_status.items()]),
        dict([(k, v['count']) for k, v in target_status.items()]),
        100
    ))
    if collection_count_diff:
        logger.info(f'collection-count-diff: {collection_count_diff}')

    # compare indices
    collection_index_size_diff = DeepDiff(*size_compare(
        dict([(k, v['totalIndexSize']) for k, v in source_status.items()]),
        dict([(k, v['totalIndexSize']) for k, v in target_status.items()]),
        1024*1024
    ))
    if collection_index_size_diff:
        logger.info(f'collection-totalIndexSize-diff: {collection_index_size_diff}')

    collection_index_diff = DeepDiff(
        dict([(k, v['indexSizes'].keys()) for k, v in source_status.items()]),
        dict([(k, v['indexSizes'].keys()) for k, v in target_status.items()]),
    )
    if collection_index_diff:
        logger.info(f'collection-index-diff: {collection_index_diff}')

    # TODO index diff of each indices


@click.command()
@click.argument('source', type=str, required=True)
@click.argument('target', type=str, required=True)
@click.option('--db', type=str)
def start(source: str, target: str, db: str):
    mongo_source = MongoClient(source)
    mongo_target = MongoClient(target)
    ignored_db = ['admin', 'config', 'local']

    compare_db(mongo_source, mongo_target, ignored_db=ignored_db)
    if not db:
        for _db in filter(lambda item: item not in ignored_db, mongo_source.list_database_names()):
            compare_collection(mongo_source.get_database(_db), mongo_target.get_database(_db))
    else:
        compare_collection(mongo_source.get_database(db), mongo_target.get_database(db))


if __name__ == '__main__':
    start()
