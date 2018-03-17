import logging
from collections import OrderedDict
from abc import abstractmethod

from .errors import ActiveMQValueError
from .helpers import activemq_stamp_datetime


logger = logging.getLogger(__name__)


class BaseMessage(object):

    def __init__(self, bsoup_tr):

        self.bsoup_tr = bsoup_tr
        self._parsed = None

    def __iter__(self):
        for key, val in self.parse().items():
            yield (key, val)

    def parse(self):
        if not self._parsed:
            self._parsed = self._do_parse()
        return self._parsed

    @abstractmethod
    def _do_parse(self):
        pass


class Message(BaseMessage):
    def __init__(self, queue, bsoup_tr):
        self.queue = queue
        super(Message, self).__init__(bsoup_tr)

    def _do_parse(self):
        cells = self.bsoup_tr.find_all('td')

        messageid = cells[0].get_text().strip()
        persistence = cells[2].get_text().strip() == 'Persistent'
        timestamp = cells[6].get_text().strip()
        href_properties = cells[0].find('a').get('href')
        href_delete = cells[8].find('a').get('href')

        if not href_delete.startswith('deleteMessage.action'):
            raise ActiveMQValueError('purge href does not start with "deleteMessage.action": {}'.format(href_delete))

        return {
            'messageid': messageid,
            'persistence': persistence,
            'timestamp': activemq_stamp_datetime(timestamp),
            'href_properties': href_properties,
            'href_delete': href_delete
        }

    def delete(self):
        parsed = self.parse()

        delete_path = '/admin/{href}'.format(href=parsed['href_delete'])
        logger.info('delete message from {name}: {messageid}'.format(name=self.queue.name, messageid=parsed['messageid']))

        response = self.queue.server.get(delete_path)

        if response.status_code is not requests.codes.ok:
            response.raise_for_status()

    def payload(self):
        def _bsoup_table_to_json(bsoup_table):
            d = OrderedDict()
            for row in bsoup_table.find('tbody').find_all('tr'):
                cells = row.find_all('td')
                d[cells[0].text.strip()] = cells[1].text.strip()
            return d

        _parsed = self.parse()

        bsoup = self.queue.server.get_bsoup('/admin/{}'.format(_parsed['href_properties']))

        bsoup_table_header = bsoup.find('table', {'id': 'header'})
        bsoup_table_properties = bsoup.find('table', {'id': 'properties'})

        return {
            'header': _bsoup_table_to_json(bsoup_table_header) if bsoup_table_header else None,
            'properties': _bsoup_table_to_json(bsoup_table_properties) if bsoup_table_properties else None
        }


class ScheduledMessage(BaseMessage):
    def _do_parse(self):
        cells = self.bsoup_tr.find_all('td')

        messageid = cells[0].get_text().strip()
        timestamp = cells[3].get_text().strip()
        delay = cells[4].get_text().strip()
        href_delete = cells[7].find('a').get('href')

        if not href_delete.startswith('deleteJob.action'):
            raise ActiveMQValueError('purge href does not start with "deleteJob.action": {}'.format(href_delete))

        return {
            'messageid': messageid,
            'timestamp': activemq_stamp_datetime(timestamp),
            'delay': int(delay),
            'href_delete': href_delete
        }
