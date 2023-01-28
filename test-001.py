import sys
import time
import logging

from mnrdmq.example import Controller


logging.basicConfig(format='%(asctime)-15s\t%(levelname)s:%(name)s: %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def test_controller():
    c = Controller(host='127.0.0.1', logger=logger)
    try:
        c.serve()
    finally:
        c.close()


def main():
    while True:
        try:
            test_controller()

        except Exception as e:
            logger.exception('test-001 exception')
            logger.info('sleeping for {}'.format(5))
            time.sleep(5)


if __name__ == '__main__':
    sys.exit(main())
