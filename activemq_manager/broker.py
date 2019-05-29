import logging
from collections import namedtuple
from datetime import datetime, timedelta
from functools import partial

import aiohttp

from .connection import Connection
from .errors import ApiError, BrokerError, HttpError
from .helpers import concurrent_functions
from .job import ScheduledJob
from .queue import Queue


logger = logging.getLogger(__name__)


def parse_object_name(path):
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
            if r.status == 200:
                # jolokia does not set the correct content-type; content_type=None will bypass this check
                rdata = await r.json(content_type=None)
                if rdata.get('status') == 200:
                    return rdata.get('value')
                else:
                    raise ApiError(rdata)
            else:
                text = await r.text()
                raise HttpError(f'http request failed\nstatus_code={r.status}\ntext={text}')

    async def attribute(self, attribute_):
            return await self.api('read', f'org.apache.activemq:type=Broker,brokerName={self.name}', attribute=attribute_)

    async def queues(self, workers=10):
        funcs = list()
        for object_name in await self.api('search', f'org.apache.activemq:type=Broker,brokerName={self.name},destinationType=Queue,destinationName=*'):
            queue_name = parse_object_name(object_name).get('destinationName')
            funcs.append(
                partial(Queue, self, queue_name)
            )
        async for q in concurrent_functions(funcs):
            yield q

    async def queue(self, name):
        queue_objects = await self.api('search', f'org.apache.activemq:type=Broker,brokerName={self.name},destinationType=Queue,destinationName={name}')
        if len(queue_objects) == 1:
            queue_name = parse_object_name(queue_objects[0]).get('destinationName')
            return await Queue(self, queue_name)
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
        funcs = list()
        for object_name in await self.api('search', f'org.apache.activemq:type=Broker,brokerName={self.name},connector=clientConnectors,connectorName=openwire,connectionViewType=remoteAddress,connectionName=*'):
            connection_name = parse_object_name(object_name).get('connectionName')
            funcs.append(
                partial(Connection, self, connection_name)
            )
        async for conn in concurrent_functions(funcs):
            yield conn
