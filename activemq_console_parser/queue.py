import logging
from datetime import timedelta
from uuid import UUID

import requests

from .errors import ActiveMQValueError
from .message import Message


logger = logging.getLogger(__name__)


class Queue:
    def __init__(self, client, name, messages_pending, messages_enqueued, messages_dequeued, consumers, href_secret, href_purge, href_delete):
        self.client = client
        self.name = name
        self.messages_pending = messages_pending
        self.messages_enqueued = messages_enqueued
        self.messages_dequeued = messages_dequeued
        self.consumers = consumers
        self.href_secret = href_secret
        self.href_purge = href_purge
        self.href_delete = href_delete

    @staticmethod
    def parse(client, bsoup_tr):
        cells = bsoup_tr.find_all('td')

        # Number Of Pending Messages    Number Of Consumers     Messages Enqueued   Messages Dequeued
        _name_col_spans = cells[0].find_all('span')
        queue_name = _name_col_spans[-1].text.strip() if len(_name_col_spans) > 0 else cells[0].find('a').text.strip()
        messages_pending = int(cells[1].text.strip())
        messages_enqueued = int(cells[3].text.strip())
        messages_dequeued = int(cells[4].text.strip())
        consumers = int(cells[2].text.strip())
        anchors = cells[6].find_all('a')
        href_secret = None
        href_purge = anchors[1].get('href')
        href_delete = anchors[2].get('href')

        for param in href_delete.split('&'):
            if param.startswith('secret'):
                href_secret = UUID(param[7:])
                break
        else:
            logger.warning('could not determine the value of the href secret')

        if href_purge and not href_purge.startswith('purgeDestination.action'):
            raise ActiveMQValueError(f'purge href does not start with "purgeDestination.action": {href_purge}')
        elif href_delete and not href_delete.startswith('deleteDestination.action'):
            raise ActiveMQValueError(f'delete href does not start with "deleteDestination.action": {href_delete}')

        return Queue(
            client=client,
            name=queue_name,
            messages_pending=messages_pending,
            messages_enqueued=messages_enqueued,
            messages_dequeued=messages_dequeued,
            consumers=consumers,
            href_secret=href_secret,
            href_purge=href_purge,
            href_delete=href_delete
        )

    def to_dict(self):
        return {
            'name': self.name,
            'messages_pending': self.messages_pending,
            'messages_enqueued': self.messages_enqueued,
            'messages_dequeued': self.messages_dequeued,
            'consumers': self.consumers
        }

    def purge(self):
        self.client.get(f'/admin/{self.href_purge}')

    def delete(self):
        self.client.get(f'/admin/{self.href_delete}')

    def message_table(self):
        bsoup = self.client.bsoup('/admin/browse.jsp?JMSDestination={queue_name}'.format(queue_name=self.name))
        table = bsoup.find_all('table', {'id': 'messages'})

        if len(table) == 1:
            return table[0]
        else:
            ActiveMQValueError('no message table was found')

    def messages(self):
        for row in self.message_table().find('tbody').find_all('tr'):
            yield Message.parse(self, row)
