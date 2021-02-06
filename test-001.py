import sys
import mnrdmq

def test_controller():
    c = mnrdmq.Controller(host='172.16.1.10')
    c.serve()


def main():
    test_controller()


if __name__ == '__main__':
    sys.exit(main())
