"""Simple controller-agent signalling system."""

import logging
import time
import re
import json

import redis

logging.basicConfig(format='%(asctime)-15s\t%(levelname)s:%(name)s: %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


# base
class _base(object):
    _CTRL = 'mnrdmqctrl'
    _BC = 'mnrdmqbc'
    _AGNT = 'mnrdmqagnt'
    _re_command = re.compile(rb'^([^:]+):(.*)$')

    def __init__(self, host='localhost', port=6379, db=0):
        self.conn = redis.Redis(host=host, port=port, db=db)
        self.p = self.conn.pubsub()
        self._handlers = {}

    def _cmd(self, msg):
        logger.debug('cmd: {}'.format(msg))
        m = self._re_command.match(msg['data'])
        if not m:
            logger.error('unknown message: {}'.format(msg['data']))
        else:
            cmd = m.group(1)
            args = m.group(2)
            if not cmd in self._handlers:
                logger.error('unknown command: {}:{}'.format(cmd, args))
            else:
                h = self._handlers[cmd]
                try:
                    a = json.loads(args)
                    h(a)
                except Exception:
                    logger.exception()

    def _add_handler(self, cmd, h):
        self._handlers[cmd.encode('utf-8')] = h

    def _setup(self, methods):
        self.p.subscribe(**methods)
        self.pthr = self.p.run_in_thread(sleep_time=1.0)

    def close(self):
        self.pthr.stop()
        self.conn.close()


# controller
class _controller(_base):
    def __init__(self, host='localhost', port=6379, db=0):
        super(_controller, self).__init__(host, port, db)
        self._agents = dict()

    def _setup(self):
        super(_controller, self)._setup({self._CTRL: self._cmd})

    def broadcast(self, cmd, args):
        self.conn.publish(self._BC, '{}:{}'.format(cmd, json.dumps(args)))

    def unicast(self, agent, cmd, args):
        ch = '{}.{}'.format(self._AGNT, agent)
        self.conn.publish(ch, '{}:{}'.format(cmd, json.dumps(args)))

    def serve(self):
        self.broadcast('discover', {'version': 1, 'bc': True})
        while True:
            time.sleep(10)
            logger.info('status:')
            for agent, caps in self._agents.items():
                version = caps.get('version')
                if version == 1:
                    self.unicast(agent, 'status', {'version': version})
                    logger.debug('asked status of {}'.format(agent))

                if 'state' in caps:
                    state = caps['state']
                    if state == 'suspended':
                        self.unicast(agent, 'resume', {'reason': 'asdasd'})
                    elif state == 'resumed':
                        self.unicast(agent, 'suspend', {'reason': 'zxczxc'})
                else:
                    self.unicast(agent, 'suspend', {'reason': 'qweqwe'})

            self.broadcast('status', {'version': 1, 'bc': True})
            logger.debug('asked status of via bc')


# agent
class _agent(_base):
    def __init__(self, name, host='localhost', port=6379, db=0):
        super(_agent, self).__init__(host, port, db)
        self._name = name
        self._kind = None
        self._state = None
        self._release = None

    def _setup(self):
        super(_agent, self)._setup({
            '{}.{}'.format(self._AGNT, self._name): self._cmd,
            self._BC: self._cmd,
        })

    def notify(self, cmd, args):
        self.conn.publish(self._CTRL, '{}:{}'.format(cmd, json.dumps(args)))

    def join(self):
        self.notify('join', { 'agent': self._name, 'version': 1, })

    def leave(self):
        self.notify('leave', {'agent': self._name})

    def work(self):
        while True:
            time.sleep(10)
            logger.info('working: {}'.format(self._name))
            self.notify(
                'status',
                {'status': 'OK', 'agent': self._name, 'state': self._state})
