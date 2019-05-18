import logging
from datetime import timedelta
from uuid import UUID

import requests

from .errors import ActiveMQValueError
from .message import Message


logger = logging.getLogger(__name__)


class Queue:
    def __init__(self, client, name, messages_pending, messages_enqueued, messages_dequeued, consumers):
        self.client = client
        self.name = name
        self.messages_pending = messages_pending
        self.messages_enqueued = messages_enqueued
        self.messages_dequeued = messages_dequeued
        self.consumers = consumers

    def __repr__(self):
        return f'<activemq_console_parser.queue.Queue object name={self.name}>'

    def asdict(self):
        return {
            'name': self.name,
            'messages_pending': self.messages_pending,
            'messages_enqueued': self.messages_enqueued,
            'messages_dequeued': self.messages_dequeued,
            'consumers': self.consumers
        }

    def purge(self):
        self.client.api('exec', f'org.apache.activemq:brokerName={self.client.broker_name},type=Broker,destinationType=Queue,destinationName={self.name}', operation='purge')

    def delete(self):
        self.client.api('exec', f'org.apache.activemq:type=Broker,brokerName={self.client.broker_name}', operation='removeQueue(java.lang.String)', arguments=[self.name])

    def message_table(self):
        bsoup = self.client.bsoup(f'/admin/browse.jsp?JMSDestination={self.name}')
        table = bsoup.find_all('table', {'id': 'messages'})

        if len(table) == 1:
            return table[0]
        else:
            ActiveMQValueError('no message table was found')

    def messages(self):
        for row in self.message_table().find('tbody').find_all('tr'):
            yield Message.parse(self, row)
