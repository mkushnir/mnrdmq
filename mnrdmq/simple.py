from base import _controller, _agent


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
