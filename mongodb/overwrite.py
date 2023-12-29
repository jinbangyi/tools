from datetime import datetime, UTC

import click
from pymongo import MongoClient
from pymongo.database import Database
from loguru import logger


def migrate(source: Database, target: Database, collection: str, start_date: datetime, end_date: datetime, time_field: str = 't'):
    # Find documents in cluster A based on the time condition
    # start_date = datetime(2023, 12, 21, 10, tzinfo=UTC)  # Modify this as needed
    # end_date = datetime(2023, 12, 25, 00, tzinfo=UTC)  # Modify this as needed

    query = {
        time_field: {"$gte": start_date, "$lte": end_date}
    }
    logger.debug(query)
    # source get data
    documents_to_transfer = source[collection].find(query)
    # print(len(list(documents_to_transfer)))

    # target delete data
    res = target[collection].delete_many(query)
    logger.info(f'delete={res.deleted_count}')

    buffer = []
    i = 0
    # Transfer documents to cluster B
    for document in documents_to_transfer:
        if len(buffer) > 1000:
            logger.debug(f'insert batch {i}')
            target[collection].insert_many(buffer)
            buffer.clear()
            i += 1
        buffer.append(document)
    if len(buffer) > 0:
        target[collection].insert_many(buffer)


@click.command()
@click.argument('source', type=str, required=True)
@click.argument('target', type=str, required=True)
@click.option('--db', type=str, default='nftgo-prod-master-3')
@click.option('--collection', type=str, required=True)
def start(source: str, target: str, db: str, collection: str):
    mongo_source = MongoClient(source)
    mongo_target = MongoClient(target)

    start_date = datetime(2023, 12, 21, 10, tzinfo=UTC)  # Modify this as needed
    end_date = datetime(2023, 12, 25, 00, tzinfo=UTC)  # Modify this as needed

    migrate(mongo_source.get_database(db), mongo_target.get_database(db), collection, start_date, end_date)

    mongo_source.close()
    mongo_target.close()


if __name__ == '__main__':
    start()
