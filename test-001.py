import sys
from import mnrdmq.simple import Controller

def test_controller():
    c = Controller(host='172.16.1.10')
    c.serve()


def main():
    test_controller()


if __name__ == '__main__':
    sys.exit(main())
