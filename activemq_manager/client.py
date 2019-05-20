import asyncio
import logging

import aiohttp
from bs4 import BeautifulSoup
from collections import namedtuple

from .errors import BrokerError, ApiError
from .helpers import yield_from_pool
from .queue import Queue
from .message import ScheduledMessage


logger = logging.getLogger(__name__)

Connection = namedtuple('Connection', ['client', 'id', 'remote_address', 'active', 'slow'])


def parse_jolokia_path(path):
    parts = dict()
    for part in path.split(','):
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

    def __repr__(self):
        return f'<activemq_manager.Client object endpoint={self.endpoint}>'

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
        queues = await self.api('search', f'org.apache.activemq:type=Broker,brokerName={self.broker_name},destinationType=Queue,destinationName=*')
        for queue in queues:
            yield parse_jolokia_path(queue).get('destinationName')

    async def queue(self, name):
        try:
            data = await self.api('read', f'org.apache.activemq:type=Broker,brokerName={self.broker_name},destinationType=Queue,destinationName={name}', attribute=[
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
        async for q in yield_from_pool(self.queue_names(), self.queue):
            yield q

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
        remote_address_search = await self.api('search', f'org.apache.activemq:type=Broker,brokerName={self.broker_name},connector=clientConnectors,connectorName=openwire,connectionViewType=remoteAddress,connectionName=*')
        for remote_address_obj in remote_address_search:
            connection_name = parse_jolokia_path(remote_address_obj).get('connectionName')
            remote_address = await self.api('read', f'org.apache.activemq:type=Broker,brokerName={self.broker_name},connector=clientConnectors,connectorName=openwire,connectionViewType=remoteAddress,connectionName={connection_name}', attribute=[
                'ClientId',
                'RemoteAddress',
                'Active',
                'Slow'
            ])
            yield Connection(
                client=self,
                id=remote_address['ClientId'],
                remote_address=remote_address['RemoteAddress'],
                active=remote_address['Active'],
                slow=remote_address['Slow'],
            )
