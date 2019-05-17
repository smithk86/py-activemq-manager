import logging
from collections import namedtuple, OrderedDict

import requests

from .errors import ActiveMQValueError
from .helpers import activemq_stamp_datetime


logger = logging.getLogger(__name__)
MessageData = namedtuple('MessageData', ['header', 'properties', 'message'])


class Message(object):
    def __init__(self, queue, message_id, persistence, timestamp, href_properties, href_delete):
        self.queue = queue
        self.message_id = message_id
        self.persistence = persistence
        self.timestamp = timestamp
        self.href_properties = href_properties
        self.href_delete = href_delete

    @staticmethod
    def parse(queue, bsoup_tr):
        cells = bsoup_tr.find_all('td')

        message_id = cells[0].get_text().strip()
        persistence = cells[2].get_text().strip() == 'Persistent'
        timestamp = cells[6].get_text().strip()
        href_properties = cells[0].find('a').get('href')
        href_delete = cells[8].find('a').get('href')

        if not href_delete.startswith('deleteMessage.action'):
            raise ActiveMQValueError(f'purge href does not start with "deleteMessage.action": {href_delete}')

        return Message(
            queue=queue,
            message_id=message_id,
            persistence=persistence,
            timestamp=activemq_stamp_datetime(timestamp),
            href_properties=href_properties,
            href_delete=href_delete
        )

    def delete(self):
        delete_path = f'/admin/{self.href_delete}'
        logger.info(f'delete message from {self.queue.name}: {self.message_id}')
        self.queue.client.web(delete_path)

    def data(self):
        def _bsoup_table_to_json(bsoup_table):
            d = OrderedDict()
            for row in bsoup_table.find('tbody').find_all('tr'):
                cells = row.find_all('td')
                d[cells[0].text.strip()] = cells[1].text.strip()
            return d

        bsoup = self.queue.client.bsoup(f'/admin/{self.href_properties}')

        bsoup_table_header = bsoup.find('table', {'id': 'header'})
        bsoup_table_properties = bsoup.find('table', {'id': 'properties'})
        bsoup_div_message = bsoup.find('div', {'class': 'message'})

        return MessageData(
            header=_bsoup_table_to_json(bsoup_table_header) if bsoup_table_header else None,
            properties=_bsoup_table_to_json(bsoup_table_properties) if bsoup_table_properties else None,
            message=bsoup_div_message.text.strip()
        )


class ScheduledMessage(object):

    def __init__(self, client, message_id, next_scheduled_time, start, delay, href_delete):
        self.client = client
        self.message_id = message_id
        self.next_scheduled_time = next_scheduled_time
        self.start = start
        self.delay = delay
        self.href_delete = href_delete

    @staticmethod
    def parse(client, bsoup_tr):
        cells = bsoup_tr.find_all('td')

        message_id = cells[0].get_text().strip()
        next_scheduled_time = cells[2].get_text().strip()
        start = cells[3].get_text().strip()
        delay = int(cells[4].get_text().strip())
        href_delete = cells[7].find('a').get('href')

        if not href_delete.startswith('deleteJob.action'):
            raise ActiveMQValueError(f'purge href does not start with "deleteJob.action": {href_delete}')

        return ScheduledMessage(
            client=client,
            message_id=message_id,
            next_scheduled_time=activemq_stamp_datetime(next_scheduled_time),
            start=activemq_stamp_datetime(start),
            delay=delay,
            href_delete=href_delete
        )

    def delete(self):
        delete_path = f'/admin/{self.href_delete}'
        logger.info(f'delete scheduled message: {self.message_id} [start={self.start}]')
        self.client.web(delete_path)
