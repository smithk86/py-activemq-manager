import logging

import requests
from bs4 import BeautifulSoup
from collections import namedtuple

from .errors import ActiveMQError, ActiveMQValueError
from .queue import Queue
from .message import ScheduledMessage


logger = logging.getLogger(__name__)

Connection = namedtuple('Connection', ['id', 'id_href', 'remote_address', 'active', 'slow'])


class Client:

    def __init__(self, host, port=8161, username=None, password=None, protocol='http'):
        self.host = host
        self.port = port
        self.protocol = protocol
        self.session = requests.Session()
        self.session.headers.update({
            'User-agent': 'python-activemq.Client'
        })

        if username and password:
            self.session.auth = (username, password)

    def close(self):
        self.session.close()

    def get(self, path):
        return self.session.get(f'{self.protocol}://{self.host}:{self.port}{path}', allow_redirects=False)

    def bsoup(self, path):
        response = self.get(path)

        if response.status_code is not requests.codes.ok:
            response.raise_for_status()

        return BeautifulSoup(response.text, 'lxml')

    def queue_table(self):
        bsoup = self.bsoup('/admin/queues.jsp')
        table = bsoup.find_all('table', {'id': 'queues'})

        if len(table) == 1:
            return table[0]
        else:
            ActiveMQValueError('no queue table was found')

    def queues_count(self):
        return len(self.queue_table().find('tbody').find_all('tr'))

    def queues(self):
        for row in self.queue_table().find('tbody').find_all('tr'):
            yield Queue.parse(self, row)

    def queue(self, name):
        for queue in self.queues():
            if queue.name == name:
                return queue
        raise ActiveMQError('queue not found: {}'.format(name))

    def scheduled_messages_table(self):
        try:
            bsoup = self.bsoup('/admin/scheduled.jsp')
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                raise ActiveMQError('scheduled messages not supported')
            else:
                raise e

        table = bsoup.find_all('table', {'id': 'Jobs'})

        if len(table) == 1:
            return table[0]
        else:
            raise ActiveMQValueError('no queue table was found')

    def scheduled_messages_count(self):
        return len(self.scheduled_messages_table().find('tbody').find_all('tr'))

    def scheduled_messages(self):
        for row in self.scheduled_messages_table().find('tbody').find_all('tr'):
            yield ScheduledMessage.parse(self, row)

    def connections(self):
        try:
            bsoup = self.bsoup('/admin/connections.jsp')
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                raise ActiveMQError('path not supported: /admin/connections.jsp')
            else:
                raise e

        for table in bsoup.find_all(id='connections'):
            # ensure the head only has four columns (name, remote address, active, slow)
            if len(table.find('thead').find_all('th')) == 4:
                for row in table.find('tbody').find_all('tr'):
                    cells = row.find_all('td')
                    yield Connection(
                        id=cells[0].text,
                        id_href=cells[0].find('a').get('href'),
                        remote_address=cells[1].text,
                        active=cells[2].text == 'true',
                        slow=cells[3].text == 'true'
                    )
