import logging
import redis
from os import environ

db_config = {
    'PASS': environ.get('PASS', ''),
    'DOMAIN': environ.get('DOMAIN', ''),
    'HASH_SIZE': int(environ.get('HASH_SIZE', '')),
    'RECORDS': int(environ.get('RECORDS', '')),
}


def generate_random_hash(numb: int = 1) -> str:
    import random
    import string
    import hashlib
    if numb == 1:
        return hashlib.sha256(''.join(random.choices(string.ascii_letters + string.digits, k=64)).encode()).hexdigest()
    else:
        return ''.join(
            [hashlib.sha256(''.join(random.choices(string.ascii_letters + string.digits, k=64)).encode()).hexdigest()
             for _ in range(numb)])


def check_redis_connection():
    try:
        redis_client = redis.StrictRedis(host=db_config['DOMAIN'], port=6379,
                                         password=db_config['PASS'])
        redis_client.ping()
        logging.info("Successfully connected to Redis")
        return True
    except redis.ConnectionError as e:
        logging.error(f"Failed to connect to Redis: {str(e)}")
        return False


def redis_write_hash(size: int = 100) -> bool:
    if check_redis_connection():
        r = redis.StrictRedis(host=db_config['DOMAIN'], port=6379,
                              password=db_config['PASS'])
        for _ in range(size):
            r.set(generate_random_hash(db_config['HASH_SIZE']), generate_random_hash(db_config['HASH_SIZE']))
        return True
    return False

if __name__ == '__main__':
    redis_write_hash(db_config['RECORDS'])