import logging
import logging.config
import time
import uuid

from boto import kinesis


logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
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


def put_data_into_stream(stream, region, sleep_duration, records_to_create):
    LOG.error("put_data_into_stream(%s, %s)", stream, region)
    connection = kinesis.connect_to_region(region)
    for i in range(records_to_create):
        data = uuid.uuid4().hex
        LOG.debug("Putting %s onto stream", data)
        res = connection.put_record(stream, data, data)
        LOG.debug(res)
        time.sleep(sleep_duration)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Put test data onto a Kinesis stream.')
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--sleep", default=.1)
    parser.add_argument("--records", default=10)
    parser.add_argument("stream_name")
    args = parser.parse_args()

    put_data_into_stream(args.stream_name, args.region, args.sleep, args.records)
