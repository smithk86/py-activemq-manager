import logging
from collections import namedtuple
from datetime import datetime, timedelta
from functools import partial

import aiohttp

from .connection import Connection
from .errors import ApiError, BrokerError, HttpError
from .helpers import concurrent_functions, parse_object_name
from .job import ScheduledJob
from .queue import Queue


logger = logging.getLogger(__name__)


class Broker:
    dtformat = '%Y-%m-%d %H:%M:%S'

    def __init__(self, endpoint, name='localhost', username=None, password=None, timeout=30):
        self.endpoint = endpoint
        self.name = name
        self.http_timeout = aiohttp.ClientTimeout(total=timeout)
        self.http_auth = aiohttp.BasicAuth(username, password=password) if (username and password) else None

    def __repr__(self):
        return f'<activemq_manager.Client object endpoint={self.endpoint}>'

    def session(self):
        return aiohttp.ClientSession(
            timeout=self.http_timeout,
            auth=self.http_auth,
            headers={
                'User-agent': 'py-activemq-manager.Broker'
            },
            raise_for_status=True
        )

    async def api(self, type, mbean, **kwargs):
        payload = {
            'type': type,
            'mbean': mbean
        }
        payload.update(kwargs)

        logger.debug(f'api payload: {payload}')
        async with self.session() as session:
            async with session.post(f'{self.endpoint}/api/jolokia', json=payload) as r:
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

    async def _new_queue(self, name):
        return await Queue(self, name)

    async def queues(self, workers=10):
        funcs = list()
        for object_name in await self.api('search', f'org.apache.activemq:type=Broker,brokerName={self.name},destinationType=Queue,destinationName=*'):
            queue_name = parse_object_name(object_name).get('destinationName')
            funcs.append(
                partial(self._new_queue, queue_name)
            )
        async for q in concurrent_functions(funcs):
            yield q

    async def queue(self, name):
        queue_objects = await self.api('search', f'org.apache.activemq:type=Broker,brokerName={self.name},destinationType=Queue,destinationName={name}')
        if len(queue_objects) == 1:
            queue_name = parse_object_name(queue_objects[0]).get('destinationName')
            return await self._new_queue(queue_name)
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

    async def _connections(self):
        for connection_type in (await self.attribute('TransportConnectors')).keys():
            for object_name in await self.api('search', f'org.apache.activemq:type=Broker,brokerName={self.name},connector=clientConnectors,connectorName={connection_type},connectionViewType=remoteAddress,connectionName=*'):
                yield connection_type, object_name

    async def connection_count(self):
        count = 0
        async for _ in self._connections():
            count += 1
        return count

    async def connections(self):
        funcs = list()
        async for connection_type, object_name in self._connections():
            connection_name = parse_object_name(object_name).get('connectionName')
            funcs.append(
                partial(Connection, self, connection_name, connection_type)
            )
        async for conn in concurrent_functions(funcs):
            yield conn
