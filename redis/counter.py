import redis
import click
from loguru import logger


def count_keys_with_prefix(redis_client, prefix):
    cursor = 0
    count = 0

    while True:
        # SCAN returns a cursor and a list of keys matching the pattern
        cursor, keys = redis_client.scan(cursor, match=prefix + '*')

        # Count the keys
        count += len(keys)

        # Check if we have iterated through all keys
        if cursor == 0:
            break

    return count


def sync_keys_by_prefix(source_client, target_client, prefix):
    cursor = 0

    while True:
        # SCAN returns a cursor and a list of keys matching the pattern
        cursor, keys = source_client.scan(cursor, match=prefix + '*', count=400)

        # Iterate through keys and sync them to the target Redis server
        for key in keys:
            value = source_client.get(key)
            target_client.set(key, value)

        logger.debug(cursor)
        # Check if we have iterated through all keys
        if cursor == 0:
            break


@click.command()
@click.argument('source', type=str, required=True)
@click.argument('target', type=str, required=True)
@click.option('--prefix', type=str, default='guardian:fixedbucket:')
@click.option('--port', type=int, default=6379)
@click.option('--source_db', type=int, default=0)
@click.option('--target_db', type=int, default=0)
def start(source: str, target: str, prefix: str, port: int, source_db: int, target_db: int):
    source_redis = redis.StrictRedis(host=source, port=port, db=source_db)
    target_redis = redis.StrictRedis(host=target, port=port, db=target_db)
    source_count = count_keys_with_prefix(source_redis, prefix)
    target_count = count_keys_with_prefix(target_redis, prefix)
    logger.info(f"source count: {source_count}")
    logger.info(f"target count: {target_count}")

    sync_keys_by_prefix(source_redis, target_redis, prefix)


if __name__ == "__main__":
    start()
