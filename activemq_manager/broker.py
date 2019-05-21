import asyncio
import logging
from datetime import datetime, timedelta

import aiohttp
from collections import namedtuple

from .errors import BrokerError, ApiError
from .job import ScheduledJob
from .queue import Queue


logger = logging.getLogger(__name__)

Connection = namedtuple('Connection', ['client', 'id', 'remote_address', 'active', 'slow'])


def parse_jolokia_path(path):
    parts = dict()
    for part in path.split(','):
        key, val = tuple(part.split('='))
        parts[key] = val
    return parts


class Broker:
    dtformat = '%Y-%m-%d %H:%M:%S'

    def __init__(self, endpoint, name='localhost', username=None, password=None):
        self.endpoint = endpoint
        self.name = name

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

        logger.debug(f'api payload: {payload}')

        async with self.session.post(f'{self.endpoint}/api/jolokia', json=payload) as r:
            # jolokia does not set the correct content-type; content_type=None will bypass this check
            rdata = await r.json(content_type=None)
            if rdata.get('status') == 200:
                return rdata.get('value')
            else:
                raise ApiError(rdata)

    async def attribute(self, attribute_):
            return await self.api('read', f'org.apache.activemq:type=Broker,brokerName={self.name}', attribute=attribute_)

    async def queues(self):
        queues = await self.api('search', f'org.apache.activemq:type=Broker,brokerName={self.name},destinationType=Queue,destinationName=*')
        for queue in queues:
            name = parse_jolokia_path(queue).get('destinationName')
            yield Queue(self, name)

    async def queue(self, name):
        queues = await self.api('search', f'org.apache.activemq:type=Broker,brokerName={self.name},destinationType=Queue,destinationName={name}')
        if len(queues) == 1:
            queue_name = parse_jolokia_path(queues[0]).get('destinationName')
            return Queue(self, queue_name)
        else:
            raise BrokerError(f'queue not found: {name}')

    async def _jobs(self, start=None, end=None):
        if not start:
            start = datetime.now()
        if not end:
            end = start + timedelta(weeks=52)
        return await self.api('exec', f'org.apache.activemq:type=Broker,brokerName={self.name},service=JobScheduler,name=JMS', operation='getAllJobs(java.lang.String,java.lang.String)', arguments=[
            start.strftime(Broker.dtformat),
            end.strftime(Broker.dtformat)
        ])

    async def job_count(self, start=None, end=None):
        count = 0
        for _ in (await self._jobs()).keys():
            count += 1
        return count

    async def jobs(self, start=None, end=None):
        for data in (await self._jobs()).values():
            yield ScheduledJob.parse(self, data)

    async def connections(self):
        remote_address_search = await self.api('search', f'org.apache.activemq:type=Broker,brokerName={self.name},connector=clientConnectors,connectorName=openwire,connectionViewType=remoteAddress,connectionName=*')
        for remote_address_obj in remote_address_search:
            connection_name = parse_jolokia_path(remote_address_obj).get('connectionName')
            remote_address = await self.api('read', f'org.apache.activemq:type=Broker,brokerName={self.name},connector=clientConnectors,connectorName=openwire,connectionViewType=remoteAddress,connectionName={connection_name}', attribute=[
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
