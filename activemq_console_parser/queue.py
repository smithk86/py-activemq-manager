import logging
from datetime import timedelta

import requests

from .errors import ActiveMQValueError
from .message import Message


logger = logging.getLogger(__name__)


class Queue:
    def __init__(self, server, name, messages_pending, messages_enqueued, messages_dequeued, consumers, href_purge):
        self.server = server
        self.name = name
        self.messages_pending = messages_pending
        self.messages_enqueued = messages_enqueued
        self.messages_dequeued = messages_dequeued
        self.consumers = consumers
        self.href_purge = href_purge

    def __iter__(self):
        yield ('name', self.name)
        yield ('messages_pending', self.messages_pending)
        yield ('messages_enqueued', self.messages_enqueued)
        yield ('messages_dequeued', self.messages_dequeued)
        yield ('consumers', self.consumers)
        yield ('href_purge', self.href_purge)

    @staticmethod
    def parse(server, bsoup_tr):
        cells = bsoup_tr.find_all('td')

        # Number Of Pending Messages    Number Of Consumers     Messages Enqueued   Messages Dequeued
        _name_col_spans = cells[0].find_all('span')
        queue_name = _name_col_spans[-1].text.strip() if len(_name_col_spans) > 0 else cells[0].find('a').text.strip()
        messages_pending = int(cells[1].text.strip())
        messages_enqueued = int(cells[3].text.strip())
        messages_dequeued = int(cells[4].text.strip())
        consumers = int(cells[2].text.strip())
        anchors = cells[6].find_all('a')
        href_purge = anchors[1].get('href')

        if href_purge and not href_purge.startswith('purgeDestination.action'):
           raise ActiveMQValueError('purge href does not start with "purgeDestination.action": {}'.format(href_purge))

        return Queue(
            server=server,
            name=queue_name,
            messages_pending=messages_pending,
            messages_enqueued=messages_enqueued,
            messages_dequeued=messages_dequeued,
            consumers=consumers,
            href_purge=href_purge,
        )

    def purge(self):
        if self.messages_pending > 0:
            purge_path = '/admin/{href}'.format(href=self.href_purge)
            logger.info('purging {name}: {messages_pending} messages'.format(name=self.name, messages_pending=self.messages_pending))

            response = self.server.get(purge_path)

            if response.status_code is not requests.codes.ok:
                response.raise_for_status()
        else:
            logger.info('purging {name}: queue is already empty'.format(name=self.name))

    def purge_by_age(self, minutes):
        def get_all_messages():
            # get/yield messages
            for message in self.get_messages():
                yield message
            # recursive call to get_all_messages since only 400 messages are shown at a time
            for message in get_all_messages():
                yield message

        for message in get_all_messages():
            parsed = message.parse()
            if parsed['timestamp'] < (datetime.now() - timedelta(minutes=minutes)):
                message.delete()
            else:
                break

    def get_message_table(self):
        bsoup = self.server.get_bsoup('/admin/browse.jsp?JMSDestination={queue_name}'.format(queue_name=self.name))
        table = bsoup.find_all('table', {'id': 'messages'})

        if len(table) == 1:
            return table[0]
        else:
            ActiveMQValueError('no message table was found')

    def get_messages(self):
        for row in self.get_message_table().find('tbody').find_all('tr'):
            yield Message(self, row)
