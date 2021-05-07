"""Simple controller-agent signalling system."""

import time
import re
import json

import redis


# base
class _base(object):
    _CTRL = 'mnrdmqctrl'
    _BC = 'mnrdmqbc'
    _AGNT = 'mnrdmqagnt'
    _re_command = re.compile(rb'^([^:]+):(.*)$')

    def __init__(self,
                 host='localhost',
                 port=6379,
                 db=0,
                 password=None,
                 ssl=False,
                 ssl_keyfile=None,
                 ssl_certfile=None,
                 ssl_cert_reqs=None,
                 ssl_ca_certs=None,
                 logger=None):
        """Initialize instance."""
        self.conn = redis.Redis(
            host=host,
            port=port,
            db=db,
            password=password,
            ssl=ssl,
            ssl_keyfile=ssl_keyfile,
            ssl_certfile=ssl_certfile,
            ssl_cert_reqs=ssl_cert_reqs,
            ssl_ca_certs=ssl_ca_certs)

        self.p = self.conn.pubsub()

        self._handlers = {}

        if logger is None:
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logger

    def _add_handler(self, cmd, h):
        # called in the constructor
        self._handlers[cmd.encode('utf-8')] = h

    def _setup(self, methods):
        # the last call in the constructor
        self.p.subscribe(**methods)
        self.pthr = self.p.run_in_thread(sleep_time=1.0)

    def _cmd(self, msg):
        self.logger.debug('cmd: {}'.format(msg))
        m = self._re_command.match(msg['data'])
        if not m:
            self.logger.error('unknown message: {}'.format(msg['data']))
        else:
            cmd = m.group(1)
            args = m.group(2)
            if not cmd in self._handlers:
                self.logger.error('unknown command: {}:{}'.format(cmd, args))
            else:
                h = self._handlers[cmd]
                try:
                    a = json.loads(args)
                    h(a)
                except Exception:
                    self.logger.exception('Error handling cmd {}, args {}'.format(cmd, args))

    def close(self):
        self.pthr.stop()
        self.conn.close()


# controller
class _controller(_base):
    def __init__(self,
                 host='localhost',
                 port=6379,
                 db=0,
                 password=None,
                 ssl=False,
                 ssl_keyfile=None,
                 ssl_certfile=None,
                 ssl_cert_reqs=None,
                 ssl_ca_certs=None,
                 logger=None):
        """Initialize instance."""

        super(_controller, self).__init__(
            host,
            port,
            db,
            password,
            ssl,
            ssl_keyfile,
            ssl_certfile,
            ssl_cert_reqs,
            ssl_ca_certs,
            logger)

        self._agents = dict()

        self._add_handler('join', self._handle_join)
        self._add_handler('leave', self._handle_leave)
        self._add_handler('status', self._handle_status)

    def _handle_join(self, caps):
        agent = caps.get('agent')
        if agent is None:
            self.logger.warning('invalid caps: {}, ignoring join'.format(caps))
            return

        self.logger.info('joined {}'.format(agent))
        if agent in self._agents:
            self.logger.warning('already joined: {}, updating caps'.format(agent))
            self._agents[agent] = caps
        else:
            self._agents[agent] = caps

    def _handle_leave(self, args):
        agent = args['agent']
        self.logger.info('left {}'.format(agent))
        if agent not in self._agents:
            self.logger.warning('already left: {}'.format(agent))
        else:
            del self._agents[agent]

    def _handle_status(self, status):
        self.logger.info('got status {} from {}: {}'.format(
            status['data']['status'], status['agent'], status))

    def _setup(self):
        # the last call in the constructor
        super(_controller, self)._setup({self._CTRL: self._cmd})

    def broadcast(self, cmd, args):
        self.conn.publish(self._BC, '{}:{}'.format(cmd, json.dumps(args)))

    def unicast(self, agent, cmd, args):
        ch = '{}.{}'.format(self._AGNT, agent)
        self.conn.publish(ch, '{}:{}'.format(cmd, json.dumps(args)))

    def status(self, agent=None):
        if agent is not None:
            self.unicast(agent, 'status', {'version': 1})
        else:
            self.broadcast('status', {'version': 1, 'bc': True})

    def _serve(self):
        raise NotImplementedError()

    def serve(self):
        self.broadcast('discover', {'version': 1, 'bc': True})
        while True:
            time.sleep(10)
            self._serve()


# agent
class _agent(_base):
    def __init__(self,
                 name,
                 host='localhost',
                 port=6379,
                 db=0,
                 password=None,
                 ssl=False,
                 ssl_keyfile=None,
                 ssl_certfile=None,
                 ssl_cert_reqs=None,
                 ssl_ca_certs=None,
                 logger=None):
        """Initialize instance."""

        super(_agent, self).__init__(
            host,
            port,
            db,
            password,
            ssl,
            ssl_keyfile,
            ssl_certfile,
            ssl_cert_reqs,
            ssl_ca_certs,
            logger)

        self._name = name
        self._add_handler('discover', self._handle_discover)
        self._add_handler('status', self._handle_status)

    def _handle_discover(self, args):
        self.join()

    def _handle_status(self, args):
        if args['version'] == 1:
            self.notify('status')

    def _setup(self):
        # the last call in the constructor
        super(_agent, self)._setup({
            '{}.{}'.format(self._AGNT, self._name): self._cmd,
            self._BC: self._cmd,
        })

    def notify(self, cmd, data=None):
        args = {
            'agent': self._name,
            'version': 1,
        }
        if isinstance(data, Exception):
            args['result'] = 'FAIL'
            args['data'] = str(data)
        else:
            args['result'] = 'OK'
            args['data'] = data

        self.conn.publish(self._CTRL, '{}:{}'.format(cmd, json.dumps(args)))

    def join(self):
        self.notify('join')

    def leave(self):
        self.notify('leave')

    def _work(self):
        raise NotImplementedError()

    def work(self):
        while True:
            time.sleep(10)
            self._work()
