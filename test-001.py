import sys
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
    test_controller()


if __name__ == '__main__':
    sys.exit(main())
