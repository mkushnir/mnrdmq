import sys
import time

import mnrdmq

def test_agent(name):
    a = mnrdmq.Agent(name=name, host='172.16.1.10')
    time.sleep(1)
    a.join()
    try:
        a.work()
    finally:
        a.leave()


def main():
    test_agent(sys.argv[1])


if __name__ == '__main__':
    sys.exit(main())

