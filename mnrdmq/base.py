"""Simple controller-agent signalling system."""

import logging
import time
import re
import json

from collections import namedtuple

import redis


# base
class _base(object):
    _CTRL = 'mnrdmqctrl'
    _BCST = 'mnrdmqbcst'
    _AGNT = 'mnrdmqagnt'
    _re_message = re.compile(rb'^([^:]+):(.*)$')

    def __init__(self,
                 realm='',
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

        self._realm = realm
        self._ctrl = '{}:{}'.format(self._CTRL, realm)
        self._bcst = '{}:{}'.format(self._BCST, realm)
        self._agnt = '{}:{}'.format(self._AGNT, realm)

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

    def _add_handler(self, event_or_command, h):
        """Add event or command handler.

        Can only be called in the implementation's constructor before the
        final self._setup(...).
        """
        self._handlers[event_or_command.encode('utf-8')] = h

    def _setup(self, methods):
        """Register methods and complete initialization sequence.

        Must be the last call in the implementation's constructor with
        the dictionary of Redis method handlers.
        """
        self.p.subscribe(**methods)
        self.pthr = self.p.run_in_thread(sleep_time=1.0)

    def _dispatch(self, msg):
        """Dispatch Redis messages.

        Default implementation.  Supports the following message format:

            name ":" json-arguments

        where name matches the a-zA-Z0-9._- pattern, and json-arguments is
        the json serialized object specific the the given message.

        Mandatory keys are:
            - agent: agent's name;
            - version: protocol version;
            - result: result verb: OK, EX, optional;
            - data: message data, optional.
        """
        self.logger.debug('msg: {}'.format(msg))

        m = self._re_message.match(msg['data'])

        if not m:
            self.logger.error('unknown message: {}'.format(msg['data']))

        else:
            command_or_event = m.group(1)
            args = m.group(2)

            if not command_or_event in self._handlers:
                self.logger.error('unknown command/event: {}:{}'.format(
                    command_or_event, args))
            else:
                h = self._handlers[command_or_event]
                try:
                    a = json.loads(args)
                    h(a)

                except Exception:
                    self.logger.exception(
                        'Error handling msg {}, args {}'.format(msg, args))

    def close(self):
        """Close the node."""
        self.pthr.stop()
        self.conn.close()


# controller
_node_fields = ('caps', 'joined', 'left', 'seen', 'status', 'meta')
try:
    _node = namedtuple(
        '_node', _node_fields, defaults=(None, 0.0, 0.0, 0.0, None, {}), module=__name__)
except TypeError:
    _node = namedtuple('_node', _node_fields, module=__name__)
    _node.__new__.__defaults__ = (None, 0.0, 0.0, 0.0, None, {})

class _controller(_base):
    def __init__(self,
                 realm='',
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
            realm,
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

    def _handle_join(self, args):
        agent = args.get('agent')
        if agent is None:
            self.logger.warning('invalid args: {}, ignoring join'.format(args))
            return

        self.logger.info('joined {}'.format(agent))

        if agent in self._agents:
            self.logger.warning('already joined: {}, updating caps'.format(
                agent))

            self._agents[agent] = self._agents[agent]._replace(
                caps=args, joined=time.time())

        else:
            self._agents[agent] = _node(caps=args, joined=time.time())

    def _handle_leave(self, args):
        agent = args['agent']
        self.logger.info('left {}'.format(agent))

        if agent not in self._agents:
            self.logger.warning('Not known: {}'.format(agent))
            self._agents[agent] = _node(caps=args, left=time.time())

        else:
            self._agents[agent] = self._agents[agent]._replace(
                left=time.time())

    def _handle_status(self, args):
        agent = args['agent']
        self.logger.info('status {}'.format(agent))

        if agent not in self._agents:
            self.logger.warning('Not known: {}'.format(agent))
            self._agents[agent] = _node(
                seen=time.time(), status=args)

        else:
            self._agents[agent] = self._agents[agent]._replace(
                seen=time.time(), status=args)

    def _setup(self):
        """Finalize initialization sequence.

        Must be the last call in the implementation's constructor.
        """
        super(_controller, self)._setup({self._ctrl: self._dispatch})

    def broadcast(self, command_or_event, args):
        """Send command/event to all nodes."""
        self.conn.publish(self._bcst, '{}:{}'.format(
            command_or_event, json.dumps(args)))

    def unicast(self, agent, command_or_event, args):
        """Send command/event to the given node."""
        ch = '{}.{}'.format(self._agnt, agent)
        self.conn.publish(ch, '{}:{}'.format(
            command_or_event, json.dumps(args)))

    def status(self, agent=None):
        """Request status from either one agent, or all agents."""
        if agent is not None:
            self.unicast(agent, 'status', {'version': 1})
        else:
            self.broadcast('status', {'version': 1})

    def _serve(self):
        """Implementation defined callback in the controller's serve loop."""  # noqa
        raise NotImplementedError()

    def serve(self):
        """Poor man's serve loop."""  # noqa
        self.broadcast('discover', {'version': 1, 'realm': self._realm})
        while True:
            time.sleep(10)
            self._serve()


# agent
class _agent(_base):
    def __init__(self,
                 name,
                 realm='',
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
            realm,
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
        """Finalize initialization sequence.

        Must be the last call in the implementation's constructor.
        """
        super(_agent, self)._setup({
            '{}.{}'.format(self._agnt, self._name): self._dispatch,
            self._BCST: self._dispatch,
        })

    def notify(self, command_or_event, data=None):
        """Send a command/event to the controller."""
        args = {
            'agent': self._name,
            'version': 1,
        }
        if isinstance(data, Exception):
            args['result'] = 'EX'
            args['data'] = {
                'cls': str(data.__class__.__name__),
                'args': data.args,
            }
        else:
            args['result'] = 'OK'
            args['data'] = data

        self.conn.publish(self._ctrl, '{}:{}'.format(
            command_or_event, json.dumps(args)))

    def join(self):
        """Join the realm."""
        self.notify('join')

    def leave(self):
        """Leave the realm."""
        self.notify('leave')

    def _work(self):
        """Implementation defined callback in the agent's work loop."""  # noqa
        raise NotImplementedError()

    def work(self):
        """Poor man's work loop."""  # noqa
        while True:
            time.sleep(10)
            self._work()
