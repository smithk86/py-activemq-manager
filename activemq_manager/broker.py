import logging
from collections import namedtuple
from datetime import datetime, timedelta
from functools import partial

import httpx
from asyncio_concurrent_functions import AsyncioConcurrentFunctions

from .connection import Connection
from .errors import ApiError, BrokerError, HttpError
from .helpers import parse_object_name
from .job import ScheduledJob
from .queue import Queue


logger = logging.getLogger(__name__)


class Broker:
    dtformat = '%Y-%m-%d %H:%M:%S'
    queue_object = Queue
    connection_object = Connection

    def __init__(self, endpoint, origin='http://localhost:80', name='localhost', username=None, password=None, timeout=30):
        self.endpoint = endpoint
        self.origin = origin
        self.name = name
        self.timeout = timeout
        self.http_auth = httpx.BasicAuth(username, password=password) if (username and password) else None

    def __repr__(self):
        return f'<activemq_manager.Client object endpoint={self.endpoint}>'

    async def api(self, type, mbean, **kwargs):
        payload = {
            'type': type,
            'mbean': mbean
        }
        payload.update(kwargs)

        logger.debug(f'api payload: {payload}')
        headers = {
            'Origin': self.origin
        }
        async with httpx.AsyncClient(auth=self.http_auth, headers=headers, timeout=self.timeout) as client:
            try:
                r = await client.post(f'{self.endpoint}/api/jolokia', json=payload)
            except httpx.NetworkError as e:
                logger.exception(e)
                raise HttpError('api call failed')

            if r.status_code == 200:
                rdata = r.json()
                if rdata.get('status') == 200:
                    return rdata.get('value')
                else:
                    raise ApiError(rdata)
            else:
                text = r.text()
                raise HttpError(f'http request failed\nstatus_code={r.status}\ntext={text}')

    async def attribute(self, attribute_):
        return await self.api('read', f'org.apache.activemq:type=Broker,brokerName={self.name}', attribute=attribute_)

    async def queues(self, workers=10):
        funcs = list()
        for object_name in await self.api('search', f'org.apache.activemq:type=Broker,brokerName={self.name},destinationType=Queue,destinationName=*'):
            queue_name = parse_object_name(object_name).get('destinationName')
            funcs.append(
                partial(Broker.queue_object.new, self, queue_name)
            )
        async for q in AsyncioConcurrentFunctions(funcs):
            yield q

    async def queue(self, name):
        queue_objects = await self.api('search', f'org.apache.activemq:type=Broker,brokerName={self.name},destinationType=Queue,destinationName={name}')
        if len(queue_objects) == 1:
            queue_name = parse_object_name(queue_objects[0]).get('destinationName')
            return await Broker.queue_object.new(self, queue_name)
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
            yield ScheduledJob(self, data['jobId'], data)

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
                partial(Broker.connection_object.new, self, connection_name, connection_type)
            )
        async for conn in AsyncioConcurrentFunctions(funcs):
            yield conn
