import logging

import requests
from bs4 import BeautifulSoup
from collections import namedtuple

from .errors import ActiveMQError, ActiveMQValueError
from .queue import Queue
from .message import ScheduledMessage


logger = logging.getLogger(__name__)

Connection = namedtuple('Connection', ['id', 'id_href', 'remote_address', 'active', 'slow'])


def parse_amq_api_object(dict_):
    # get objectName and strip off "org.apache.activemq:"
    object_name = dict_['objectName'][20:]
    parts = dict()
    for part in object_name.split(','):
        key, val = tuple(part.split('='))
        parts[key] = val
    return parts


class Client:
    def __init__(self, endpoint, broker_name='localhost', username=None, password=None):
        self.endpoint = endpoint
        self.broker_name = broker_name
        self.session = requests.Session()
        self.session.headers.update({
            'User-agent': 'python-activemq.Client'
        })

        if username and password:
            self.session.auth = (username, password)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def close(self):
        self.session.close()

    def api(self, path):
        r = self.session.get(f'{self.endpoint}{path}')
        if r.status_code is not requests.codes.ok:
            r.raise_for_status()
        return r.json().get('value')

    def web(self, path):
        r = self.session.get(f'{self.endpoint}{path}', allow_redirects=False)
        if r.status_code is not requests.codes.ok:
            r.raise_for_status()
        return r.text

    def bsoup(self, path):
        text = self.web(path)
        return BeautifulSoup(text, 'lxml')

    def queue_names(self):
        data = self.api(f'/api/jolokia/read/org.apache.activemq:brokerName={self.broker_name},type=Broker/Queues')
        for queue in data:
            parsed = parse_amq_api_object(queue)
            yield parsed['destinationName']

    def queue(self, name):
        attrs = [
            'QueueSize',
            'EnqueueCount',
            'DequeueCount',
            'ConsumerCount'
        ]
        data = self.api(f'/api/jolokia/read/org.apache.activemq:brokerName={self.broker_name},type=Broker,destinationType=Queue,destinationName={name}/{",".join(attrs)}')
        return Queue(
            client=self,
            name=name,
            messages_pending=data['QueueSize'],
            messages_enqueued=data['EnqueueCount'],
            messages_dequeued=data['DequeueCount'],
            consumers=data['ConsumerCount']
        )

    def queues(self):
        for name in self.queue_names():
            yield self.queue(name)

    def queues_count(self):
        return sum(1 for _ in self.queue_names())

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
