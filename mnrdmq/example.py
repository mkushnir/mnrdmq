from mnrdmq.base import _controller, _agent


class Controller(_controller):
    def __init__(self, realm='', host='localhost', port=6379, db=0, logger=None):
        super(Controller, self).__init__(realm, host, port, db, logger=logger)

        self._add_handler('suspend', self._handle_suspend)
        self._add_handler('resume', self._handle_resume)

        self._setup()

    def _handle_suspend(self, args):
        agent = args['agent']
        if agent in self._agents:
            self._agents[agent].meta['state'] = 'suspended'
            self.logger.info('agent {} set to suspended'.format(agent))

    def _handle_resume(self, args):
        agent = args['agent']
        if agent in self._agents:
            self._agents[agent].meta['state'] = 'resumed'
            self.logger.info('agent {} set to resumed'.format(agent))

    def suspend(self, agent, reason=None):
        self.unicast(agent, 'suspend', {'reason': reason})

    def resume(self, agent, reason=None):
        self.unicast(agent, 'resume', {'reason': reason})

    def _serve(self):
        self.logger.info('serving:')
        for agent, node in self._agents.items():
            version = node.meta.get('version')

            if version == 1:
                self.status(agent)
                self.logger.debug('asked status of {}'.format(agent))

            if 'state' in node.meta:
                state = node.meta['state']

                if state == 'suspended':
                    self.resume(agent, 'asdasd')

                elif state == 'resumed':
                    self.suspend(agent, 'zxczxc')

            else:
                self.suspend(agent, 'qweqwe')

        self.status()
        self.logger.debug('asked status of via bc')


class Agent(_agent):
    def __init__(self, name, realm='', host='localhost', port=6379, db=0, logger=None):
        super(Agent, self).__init__(name, realm, host, port, db, logger=logger)
        self._kind = None
        self._state = None
        self._release = None
        self._add_handler('suspend', self._handle_suspend)
        self._add_handler('resume', self._handle_resume)

        self._setup()

    def _handle_suspend(self, args):
        self._state = 'S'
        self.notify('suspend')

    def _handle_resume(self, args):
        self._state = 'R'
        self.notify('resume')

    def _work(self):
        self.logger.info('working: {}'.format(self._name))
        self.notify('status')
