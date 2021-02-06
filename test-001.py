import sys
import logging

from import mnrdmq.simple import Controller


logging.basicConfig(format='%(asctime)-15s\t%(levelname)s:%(name)s: %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def test_controller():
    c = Controller(host='172.16.1.10', logger=logger)
    c.serve()


def main():
    test_controller()


if __name__ == '__main__':
    sys.exit(main())
