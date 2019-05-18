import asyncio
import logging

import aiohttp
from bs4 import BeautifulSoup
from collections import namedtuple

from .errors import BrokerError, ApiError
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

        if username and password:
            auth = aiohttp.BasicAuth(username, password)
        else:
            auth = None
        self.session = aiohttp.ClientSession(auth=auth, headers={
            'User-agent': 'activemq-console-parser.Client'
        })

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.close()

    async def close(self):
        await self.session.close()

    async def api(self, type, mbean, **kwargs):
        payload = {
            'type': type,
            'mbean': mbean
        }
        payload.update(kwargs)

        async with self.session.post(f'{self.endpoint}/api/jolokia', json=payload) as r:
            # jolokia does not set the correct content-type; content_type=None will bypass this check
            rdata = await r.json(content_type=None)
            if rdata.get('status') == 200:
                return rdata.get('value')
            else:
                raise ApiError(rdata)

    async def web(self, path, **kwargs):
        async with self.session.get(f'{self.endpoint}{path}', allow_redirects=False, **kwargs) as r:
            return await r.text()

    async def bsoup(self, path):
        text = await self.web(path)
        return BeautifulSoup(text, 'lxml')

    async def queue_names(self):
        data = await self.api('read', f'org.apache.activemq:brokerName={self.broker_name},type=Broker', attribute='Queues')
        for queue in data:
            parsed = parse_amq_api_object(queue)
            yield parsed['destinationName']

    async def queue(self, name):
        try:
            data = await self.api('read', f'org.apache.activemq:brokerName={self.broker_name},type=Broker,destinationType=Queue,destinationName={name}', attributes=[
                'QueueSize',
                'EnqueueCount',
                'DequeueCount',
                'ConsumerCount'
            ])
        except ApiError as e:
            if e.error_type == 'javax.management.InstanceNotFoundException':
                raise BrokerError(f'queue not found: {name}')
            raise e

        return Queue(
            client=self,
            name=name,
            messages_pending=data['QueueSize'],
            messages_enqueued=data['EnqueueCount'],
            messages_dequeued=data['DequeueCount'],
            consumers=data['ConsumerCount']
        )

    async def queues(self):
        async for name in self.queue_names():
            yield await self.queue(name)

    async def scheduled_messages_table(self):
        try:
            bsoup = await self.bsoup('/admin/scheduled.jsp')
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                raise BrokerError('scheduled messages not supported')
            else:
                raise e

        table = bsoup.find_all('table', {'id': 'Jobs'})

        if len(table) == 1:
            return table[0]
        else:
            raise BrokerError('no queue table was found')

    async def scheduled_messages_count(self):
        return len((await self.scheduled_messages_table()).find('tbody').find_all('tr'))

    async def scheduled_messages(self):
        for row in (await self.scheduled_messages_table()).find('tbody').find_all('tr'):
            yield ScheduledMessage.parse(self, row)

    async def connections(self):
        try:
            bsoup = await self.bsoup('/admin/connections.jsp')
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                raise BrokerError('path not supported: /admin/connections.jsp')
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
