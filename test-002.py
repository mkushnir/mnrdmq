import sys
import time
import logging

from mnrdmq.example import Agent


logging.basicConfig(format='%(asctime)-15s\t%(levelname)s:%(name)s: %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def test_agent(name):
    a = Agent(name=name, host='127.0.0.1', logger=logger)
    time.sleep(1)
    a.join()
    try:
        a.work()
    finally:
        a.leave()
        a.close()


def main():
    test_agent(sys.argv[1])


if __name__ == '__main__':
    sys.exit(main())

