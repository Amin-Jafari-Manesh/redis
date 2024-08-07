import logging
import redis
from os import environ
import time

logging.basicConfig(level=logging.INFO)

db_config = {
    'PASS': environ.get('PASS', ''),
    'DOMAIN': environ.get('DOMAIN', ''),
    'DATA_TYPE': environ.get('DATA_TYPE', ''),
    'DATA_SIZE': int(environ.get('HASH_SIZE', '')),
    'RECORDS': int(environ.get('RECORDS', '')),
    'INSERT_DELAY': int(environ.get('INSERT_DELAY', '')),
}

redis_client = redis.StrictRedis(host=db_config['DOMAIN'], port=6379,
                                 password=db_config['PASS'])


def generate_random_hash(numb: int) -> str:
    import random
    import string
    import hashlib
    return ''.join(
        [hashlib.sha256(''.join(random.choices(string.ascii_letters + string.digits, k=64)).encode()).hexdigest()
         for _ in range(numb)])


def generate_text(numb: int) -> str:
    text = ' The quick brown fox jumps over the lazy dog today. '
    return ''.join([text for _ in range(numb)])


def redis_write(size: int = 100) -> bool:
    if not redis_client.ping():
        logging.error("Failed to connect to the database.")
        return False

    logging.info("Successfully connected to the database.")

    if db_config['DATA_TYPE'] == 'h':
        func = generate_random_hash
    elif db_config['DATA_TYPE'] == 't':
        func = generate_text
    else:
        logging.error("Invalid data type.")
        return False

    try:
        for _ in range(size):
            time.sleep(db_config['INSERT_DELAY'] * 0.001)
            redis_client.set(func(db_config['DATA_SIZE']),
                             func(db_config['DATA_SIZE']))
    except Exception as e:
        logging.error(f"Error writing to the database: {e}")
        return False


if __name__ == '__main__':
    if redis_write(db_config['RECORDS']):
        logging.info("Data has been written successfully.")
    else:
        logging.error("Failed to write data.")
