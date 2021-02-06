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


class Controller(_controller):
    def __init__(self, host='localhost', port=6379, db=0):
        super(Controller, self).__init__(host, port, db)

        self._add_handler('join', self._handle_join)
        self._add_handler('leave', self._handle_leave)
        self._add_handler('status', self._handle_status)
        self._add_handler('suspend', self._handle_suspend)
        self._add_handler('resume', self._handle_resume)

        self._setup()

    def _handle_join(self, caps):
        agent = caps.get('agent')
        if agent is None:
            logger.warning('invalid caps: {}, ignoring join'.format(caps))
            return

        logger.info('joined {}'.format(agent))
        if agent in self._agents:
            logger.warning('already joined: {}, updating caps'.format(agent))
            self._agents[agent] = caps
        else:
            self._agents[agent] = caps

    def _handle_leave(self, args):
        agent = args['agent']
        logger.info('left {}'.format(agent))
        if agent not in self._agents:
            logger.warning('already left: {}'.format(agent))
        else:
            del self._agents[agent]

    def _handle_status(self, status):
        logger.info('got status {} from {} state {}'.format(
            status['status'], status['agent'], status['state']))

    def _handle_suspend(self, args):
        agent = args['agent']
        if agent in self._agents:
            self._agents[agent]['state'] = 'suspended'
            logger.info('agent {} set to suspended'.format(agent))

    def _handle_resume(self, args):
        agent = args['agent']
        if agent in self._agents:
            self._agents[agent]['state'] = 'resumed'
            logger.info('agent {} set to resumed'.format(agent))


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


class Agent(_agent):
    def __init__(self, name, host='localhost', port=6379, db=0):
        super(Agent, self).__init__(name, host, port, db)

        self._add_handler('status', self._handle_status)
        self._add_handler('discover', self._handle_discover)
        self._add_handler('suspend', self._handle_suspend)
        self._add_handler('resume', self._handle_resume)

        self._setup()

    def _handle_status(self, args):
        if args['version'] == 1:
            self.notify(
                'status',
                {'status': 'OK', 'agent': self._name, 'state': self._state})

    def _handle_discover(self, args):
        self.join()

    def _handle_suspend(self, args):
        self._state = 'S'
        self.notify('suspend', {'status': 'OK', 'agent': self._name})

    def _handle_resume(self, args):
        self._state = 'R'
        self.notify('resume', {'status': 'OK', 'agent': self._name})
