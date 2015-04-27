import logging
import logging.config
import time
from threading import Thread

from boto import kinesis

logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s: %(message)s'
        },
    },
    'handlers': {
        'default': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
        },
    },
    'loggers': {
        '': {
            'handlers': ['default'],
            'level': 'DEBUG',
            'propagate': True
        },
        '__main__': {
            'handlers': ['default'],
            'level': 'DEBUG',
            'propagate': False
        },
        'boto': {
            'handlers': ['default'],
            'level': 'INFO',
            'propagate': False
        },
    }
})


LOG = logging.getLogger(__name__)


def log_stream_iterator(stream, connection, shard_id):
    LOG.debug("Thread starting for stream=%s, shard=%s", stream, shard_id)
    shard_iterator = connection.get_shard_iterator(stream, shard_id, "LATEST")["ShardIterator"]
    while True:
        out = connection.get_records(shard_iterator, limit=2)
        for record in out["Records"]:
            LOG.info("%s: %s", shard_id, record)
        shard_iterator = out["NextShardIterator"]
        time.sleep(2)


def process_stream(stream, region):
    LOG.error("process_stream(%s, %s)", stream, region)
    connection = kinesis.connect_to_region(region)
    description = connection.describe_stream(stream)
    shard_ids = [shard['ShardId'] for shard in description['StreamDescription']['Shards']]

    for shard_id in shard_ids:
        t = Thread(target=log_stream_iterator, args=(stream, connection, shard_id,))
        t.start()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Process a Kinesis stream.')
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("stream_name")
    args = parser.parse_args()

    process_stream(args.stream_name, args.region)
